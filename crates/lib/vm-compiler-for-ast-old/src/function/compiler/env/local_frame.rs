//! Local-frame and register-allocation management.

use std::cell::RefCell;
use std::rc::Rc;

use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::super::suspend::PromiseMarker;
use super::{
    FlowState, InitializedLocalMarker, LocalSlot,
    locals::{LocalId, Locals},
};

/// Shared storage for reusable temporary registers.
#[derive(Debug, Clone, Default)]
struct TemporaryRegisterPool {
    /// Registers released back into the temporary pool.
    available_registers: Rc<RefCell<Vec<RegisterId>>>,
}

impl TemporaryRegisterPool {
    /// Removes one reusable register from the pool if available.
    fn pop(&self) -> Option<RegisterId> {
        self.available_registers.borrow_mut().pop()
    }

    /// Creates an owned temporary-register handle for `register`.
    fn lease(&self, register: RegisterId) -> TemporaryRegister {
        TemporaryRegister {
            register,
            pool: self.clone(),
        }
    }

    /// Returns a released temporary register back to the pool.
    fn release(&self, register: RegisterId) {
        self.available_registers.borrow_mut().push(register);
    }
}

/// An owned temporary register that returns itself to the pool on drop.
#[derive(Debug)]
pub struct TemporaryRegister {
    /// The leased register id.
    register: RegisterId,

    /// Shared temporary-register pool that owns reusable ids.
    pool: TemporaryRegisterPool,
}

impl TemporaryRegister {
    /// Returns the underlying register id.
    pub fn register(&self) -> RegisterId {
        self.register
    }
}

impl Drop for TemporaryRegister {
    fn drop(&mut self) {
        self.pool.release(self.register);
    }
}

/// A register handle that may own a temporary register lease.
#[derive(Debug)]
pub enum RegisterHandle {
    /// A stable pre-existing register that must not be released.
    Existing(RegisterId),

    /// A temporary register lease that is released on drop.
    Temporary(TemporaryRegister),
}

impl RegisterHandle {
    /// Returns the underlying register id.
    pub fn register(&self) -> RegisterId {
        match self {
            Self::Existing(register) => *register,
            Self::Temporary(register) => register.register(),
        }
    }
}

/// Helpers for promise-register handles that keep temporary leases alive until await.
impl Marked<RegisterHandle, PromiseMarker> {
    /// Returns the promise register tagged for VM core instructions.
    pub fn marked(&self) -> Marked<RegisterId, PromiseMarker> {
        Marked::mark(self.register())
    }

    /// Converts the promise register back into a plain register handle.
    pub fn into_register(self) -> RegisterHandle {
        Self::unmark(self)
    }
}

/// Per-function local-variable state and register allocation.
///
/// A frame can have more registers allocated than the amount of
/// local variables the function uses. This is due to the nature of
/// the lowering process - for instance, sometimes you need to lower
/// an expression into a sequence of instructions that
/// have to pass the data to each other.
pub struct LocalFrame {
    /// All locals declared for the current function.
    locals: Locals,

    /// The next register index to allocate.
    next_register_index: usize,

    /// Released temporary registers that can be reused by later lowering.
    available_temporary_registers: TemporaryRegisterPool,

    /// Reusable register for discarded statement results.
    discard_register: Option<RegisterId>,
}

/// A local lookup result that may already exist or may need declaration.
enum LocalEntry<'a> {
    /// The local already exists.
    Occupied(LocalSlot),

    /// The local name is not declared yet.
    Vacant(VacantLocal<'a>),
}

/// Deferred declaration information for a local that does not exist yet.
struct VacantLocal<'a> {
    /// The local frame that will own the new declaration.
    local_frame: &'a mut LocalFrame,

    /// The name to declare.
    name: String,
}

impl<'a> VacantLocal<'a> {
    /// Declares the vacant local and allocates a register for it.
    fn declare(self) -> LocalSlot {
        let register = self.local_frame.allocate_register();
        let local = self
            .local_frame
            .locals
            .declare_known_vacant(self.name, register);
        LocalSlot::new(local, register)
    }
}

impl LocalFrame {
    /// Creates an empty local frame.
    pub fn new() -> Self {
        Self {
            locals: Locals::new(),
            next_register_index: 0,
            available_temporary_registers: TemporaryRegisterPool::default(),
            discard_register: None,
        }
    }

    /// Declares an input local and marks it initialized.
    pub fn declare_input(
        &mut self,
        flow_state: &mut FlowState,
        name: String,
    ) -> Option<Marked<LocalSlot, InitializedLocalMarker>> {
        match self.entry(name) {
            LocalEntry::Occupied(_) => None,
            LocalEntry::Vacant(local) => {
                let local = local.declare();
                flow_state.mark_initialized(local);
                Some(Marked::mark(local))
            }
        }
    }

    /// Allocates the next register in the function frame.
    pub fn allocate_register(&mut self) -> RegisterId {
        let register = RegisterId(self.next_register_index);
        self.next_register_index += 1;
        register
    }

    /// Allocates a temporary register that is released back to the pool on drop.
    pub fn allocate_temporary_register(&mut self) -> TemporaryRegister {
        let register = self
            .available_temporary_registers
            .pop()
            .unwrap_or_else(|| self.allocate_register());
        self.available_temporary_registers.lease(register)
    }

    /// Returns a reusable register for statement results that will be discarded.
    pub fn discard_register(&mut self) -> RegisterId {
        if let Some(register) = self.discard_register {
            return register;
        }

        let register = self.allocate_register();
        self.discard_register = Some(register);
        register
    }

    /// Resolves a local by name, declaring it if needed.
    pub fn get_or_declare_local(&mut self, name: &str, flow_state: &mut FlowState) -> LocalSlot {
        match self.entry(name.to_owned()) {
            LocalEntry::Occupied(local) => {
                flow_state.declare_local(local);
                local
            }
            LocalEntry::Vacant(local) => {
                let local = local.declare();
                flow_state.declare_local(local);
                local
            }
        }
    }

    /// Resolves a local only if it exists and is definitely initialized.
    pub fn resolve_initialized_local(
        &self,
        name: &str,
        flow_state: &FlowState,
    ) -> Option<Marked<LocalSlot, InitializedLocalMarker>> {
        let local = self.locals.lookup(name)?;
        if !flow_state.is_initialized(local) {
            return None;
        }

        let slot = self.local_slot(local).unwrap();
        Some(Marked::mark(slot))
    }

    /// Returns the number of registers allocated so far.
    pub fn num_registers(&self) -> usize {
        self.next_register_index
    }

    /// Reconstructs a local slot for a known local id.
    fn local_slot(&self, local: LocalId) -> Option<LocalSlot> {
        let register = self.locals.register(local)?;
        Some(LocalSlot::new(local, register))
    }

    /// Returns an occupied or vacant entry view for `name`.
    fn entry<'a>(&'a mut self, name: String) -> LocalEntry<'a> {
        if let Some(local) = self.locals.lookup(&name) {
            return LocalEntry::Occupied(self.local_slot(local).unwrap());
        }

        LocalEntry::Vacant(VacantLocal {
            local_frame: self,
            name,
        })
    }
}
