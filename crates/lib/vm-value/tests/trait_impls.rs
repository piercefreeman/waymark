//! Compile-time assertion that [`Value`] implements the necessary traits.

use waymark_vm_value::Value;

#[allow(dead_code)]
fn assert_fullset_interpreter_impls() {
    fn assert_impl<T: waymark_vm_interpreter_fullset::value::Value>() {}
    assert_impl::<Value>();
}
