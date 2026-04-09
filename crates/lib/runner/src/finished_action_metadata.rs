use chrono::{DateTime, Utc};

pub struct FinishedActionMetadata<Result> {
    pub attempt: i32,
    pub started_at: Option<DateTime<Utc>>,
    pub result: Result,
}

impl<Result> FinishedActionMetadata<Result> {
    pub fn map_result<NewResult>(
        self,
        f: impl FnOnce(Result) -> NewResult,
    ) -> FinishedActionMetadata<NewResult> {
        let Self {
            attempt,
            started_at,
            result,
        } = self;
        FinishedActionMetadata {
            attempt,
            started_at,
            result: f(result),
        }
    }
}

impl<T, E> FinishedActionMetadata<Result<T, E>> {
    pub fn transpose_result(self) -> Result<FinishedActionMetadata<T>, FinishedActionMetadata<E>> {
        let Self {
            attempt,
            started_at,
            result,
        } = self;
        match result {
            Ok(result) => Ok(FinishedActionMetadata {
                attempt,
                started_at,
                result,
            }),
            Err(result) => Err(FinishedActionMetadata {
                attempt,
                started_at,
                result,
            }),
        }
    }
}

impl<R> FinishedActionMetadata<R> {
    pub fn map_transpose_result<T, E>(
        self,
        f: impl FnOnce(R) -> Result<T, E>,
    ) -> Result<FinishedActionMetadata<T>, FinishedActionMetadata<E>> {
        self.map_result(f).transpose_result()
    }
}
