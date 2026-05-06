//! Helpers for iterating over values resolved from register ids.

use waymark_vm_runtime_core::RegisterId;

/// Calls `f` with an iterator over values resolved from register ids.
///
/// `resolve_value` is called for each register id as the iterator is consumed, and
/// any remaining register ids are still validated if `f` stops consuming early.
pub fn with_register_values<RegisterIds, ResolveValue, Value, Error, Output>(
    register_ids: RegisterIds,
    resolve_value: ResolveValue,
    f: impl FnOnce(&mut RegisterValues<RegisterIds::IntoIter, ResolveValue, Value, Error>) -> Output,
) -> Result<Output, Error>
where
    RegisterIds: IntoIterator<Item = RegisterId>,
    ResolveValue: FnMut(usize, RegisterId) -> Result<Value, Error>,
{
    let mut register_values = RegisterValues::new(register_ids.into_iter(), resolve_value);
    let output = f(&mut register_values);
    register_values.finish()?;
    Ok(output)
}

/// Iterator over values resolved from register ids.
pub struct RegisterValues<RegisterIds, ResolveValue, Value, Error> {
    register_ids: RegisterIds,
    resolve_value: ResolveValue,
    next_item_pos: usize,
    pending_error: Option<Error>,
    phantom_data: core::marker::PhantomData<Value>,
}

impl<RegisterIds, ResolveValue, Value, Error>
    RegisterValues<RegisterIds, ResolveValue, Value, Error>
where
    RegisterIds: Iterator<Item = RegisterId>,
    ResolveValue: FnMut(usize, RegisterId) -> Result<Value, Error>,
{
    fn new(register_ids: RegisterIds, resolve_value: ResolveValue) -> Self {
        Self {
            register_ids,
            resolve_value,
            next_item_pos: 0,
            pending_error: None,
            phantom_data: core::marker::PhantomData,
        }
    }

    fn next_register(&mut self) -> Option<(usize, RegisterId)> {
        let register = self.register_ids.next()?;
        let item_pos = self.next_item_pos;
        self.next_item_pos += 1;
        Some((item_pos, register))
    }

    fn resolve_value(&mut self, item_pos: usize, register: RegisterId) -> Result<Value, Error> {
        (self.resolve_value)(item_pos, register)
    }

    fn finish(&mut self) -> Result<(), Error> {
        if let Some(error) = self.pending_error.take() {
            return Err(error);
        }

        while let Some((item_pos, register)) = self.next_register() {
            self.resolve_value(item_pos, register)?;
        }

        Ok(())
    }
}

impl<RegisterIds, ResolveValue, Value, Error> Iterator
    for RegisterValues<RegisterIds, ResolveValue, Value, Error>
where
    RegisterIds: Iterator<Item = RegisterId>,
    ResolveValue: FnMut(usize, RegisterId) -> Result<Value, Error>,
{
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pending_error.is_some() {
            return None;
        }

        let (item_pos, register) = self.next_register()?;

        match self.resolve_value(item_pos, register) {
            Ok(value) => Some(value),
            Err(error) => {
                self.pending_error = Some(error);
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::with_register_values;
    use waymark_vm_runtime_core::{Promise, PromiseStateId, RegisterId, Registers};

    #[derive(Debug)]
    enum TestError {
        MissingListItem {
            item_pos: usize,
            register: RegisterId,
        },
        UnresolvedListItem {
            item_pos: usize,
            promise_state_id: PromiseStateId,
        },
    }

    #[test]
    fn validates_remaining_items_after_early_stop() {
        let mut registers = Registers::new(3);
        registers.set(RegisterId(0), Promise::Resolved(10));
        registers.set(RegisterId(1), Promise::Resolved(20));
        registers.set(RegisterId(2), Promise::Resolved(30));

        let values = with_register_values(
            [RegisterId(0), RegisterId(1), RegisterId(2)],
            |item_pos, register| {
                let value = registers
                    .get(register)
                    .ok_or(TestError::MissingListItem { item_pos, register })?;
                let value = value.require_resolved_ref().map_err(|source| {
                    TestError::UnresolvedListItem {
                        item_pos,
                        promise_state_id: source.promise_state_id,
                    }
                })?;

                Ok::<_, TestError>(*value)
            },
            |items| items.take(2).collect::<Vec<_>>(),
        )
        .expect("all items should resolve");

        assert_eq!(values, vec![10, 20]);
    }

    #[test]
    fn reports_unresolved_items_after_early_stop() {
        let mut registers = Registers::new(3);
        registers.set(RegisterId(0), Promise::Resolved(10));
        registers.set(RegisterId(1), Promise::Resolved(20));
        registers.set(RegisterId(2), Promise::Pending(PromiseStateId(7)));

        let err = with_register_values(
            [RegisterId(0), RegisterId(1), RegisterId(2)],
            |item_pos, register| {
                let value = registers
                    .get(register)
                    .ok_or(TestError::MissingListItem { item_pos, register })?;
                let value = value.require_resolved_ref().map_err(|source| {
                    TestError::UnresolvedListItem {
                        item_pos,
                        promise_state_id: source.promise_state_id,
                    }
                })?;

                Ok::<_, TestError>(*value)
            },
            |items| items.take(1).collect::<Vec<_>>(),
        )
        .expect_err("remaining pending item should still be validated");

        match err {
            TestError::UnresolvedListItem {
                item_pos,
                promise_state_id,
            } => {
                assert_eq!(item_pos, 2);
                assert_eq!(promise_state_id, PromiseStateId(7));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn reports_missing_items() {
        let mut registers = Registers::new(2);
        registers.set(RegisterId(0), Promise::Resolved(10));

        let err = with_register_values(
            [RegisterId(0), RegisterId(1)],
            |item_pos, register| {
                let value = registers
                    .get(register)
                    .ok_or(TestError::MissingListItem { item_pos, register })?;
                let value = value.require_resolved_ref().map_err(|source| {
                    TestError::UnresolvedListItem {
                        item_pos,
                        promise_state_id: source.promise_state_id,
                    }
                })?;

                Ok::<_, TestError>(*value)
            },
            |items| items.collect::<Vec<_>>(),
        )
        .expect_err("missing registers should be reported");

        match err {
            TestError::MissingListItem { item_pos, register } => {
                assert_eq!(item_pos, 1);
                assert_eq!(register, RegisterId(1));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
