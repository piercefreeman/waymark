pub trait Factory {
    type Key;
    type Value;
    type Error;

    fn produce<'a>(
        &'a self,
        key: &'a Self::Key,
    ) -> impl Future<Output = Result<Self::Value, Self::Error>> + Send + 'a;
}
