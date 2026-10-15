//! Compatibility between the state-vm-runtimes core traits and the state
//! manager.

#![warn(missing_docs)]

use std::hash::Hash;
use std::sync::Arc;

/// A state manager type as an implementation of the state-vm-runtimes core
/// traits.
pub struct Compat<T>(pub T);

impl<Provider> waymark_state_vm_runtimes_core::ExecutableProvider for Compat<Provider>
where
    Provider: waymark_state_manager::provider::Provider,
{
    type ExecutableId = Provider::Key;

    /// Holding the handle keeps the state manager from evicting the
    /// executable.
    type Handle = Compat<waymark_state_manager::Handle<Provider::Key, Provider::Value>>;

    type Error = Provider::Error;

    fn provide_executable(
        &self,
        executable_id: Self::ExecutableId,
    ) -> impl Future<Output = Result<Self::Handle, Self::Error>> + Send + '_ {
        let handle = self.0.get(executable_id);

        async move { handle.await.map(Compat) }
    }
}

impl<Key, Executable> waymark_state_vm_runtimes_core::ExecutableAccess
    for Compat<
        waymark_state_manager::Handle<
            Key,
            Arc<waymark_state_vm_executables_core::Entry<Key, Executable>>,
        >,
    >
where
    Key: Eq + Hash,
{
    type Executable = Arc<Executable>;

    fn executable(&self) -> &Self::Executable {
        &waymark_state_manager::Handle::value(&self.0).executable
    }
}
