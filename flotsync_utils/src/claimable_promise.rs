use kompact::prelude::{Completable, Fulfillable, KFuture, KPromise, PromiseErr, promise};
use std::sync::{Arc, Mutex};

/// Cloneable one-shot promise handle for values that must be fanned out across
/// Kompact port channels yet still complete exactly once.
///
/// This is primarily useful for indication payloads that Kompact may broadcast
/// to several channel clones while still needing a single completion edge back
/// into one component.
pub struct KClaimablePromise<T>
where
    T: Send + Sized,
{
    inner: Arc<Mutex<Option<KPromise<T>>>>,
}

impl<T> KClaimablePromise<T>
where
    T: Send + Sized,
{
    /// Create one cloneable promise handle plus its paired future.
    pub fn create_pair() -> (Self, KFuture<T>) {
        let (promise, future) = promise();
        (
            Self {
                inner: Arc::new(Mutex::new(Some(promise))),
            },
            future,
        )
    }

    pub fn take_promise(self) -> Result<KPromise<T>, PromiseErr> {
        let mut guard = self.inner.lock().map_err(|_| PromiseErr::ChannelBroken)?;
        guard.take().ok_or(PromiseErr::AlreadyFulfilled)
    }
}

impl<T> Fulfillable<T> for KClaimablePromise<T>
where
    T: Send + Sized,
{
    fn fulfil(self, value: T) -> Result<(), PromiseErr> {
        let promise = self.take_promise()?;
        promise.fulfil(value)
    }
}

impl<T> Completable for KClaimablePromise<T>
where
    T: Send + Sized + Default,
{
    fn complete(self) -> Result<(), PromiseErr> {
        let promise = self.take_promise()?;
        promise.complete()
    }
}

impl<T> Clone for KClaimablePromise<T>
where
    T: Send + Sized,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> std::fmt::Debug for KClaimablePromise<T>
where
    T: Send + Sized,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("KClaimablePromise(..)")
    }
}

impl<T> PartialEq for KClaimablePromise<T>
where
    T: Send + Sized,
{
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<T> Eq for KClaimablePromise<T> where T: Send + Sized {}

#[cfg(test)]
mod tests {
    use super::*;
    use kompact::prelude::block_on;

    #[test]
    fn claimable_promise_completes_only_once() {
        let (handle, future) = KClaimablePromise::<()>::create_pair();

        handle
            .clone()
            .complete()
            .expect("first completion should succeed");
        assert!(matches!(
            handle.complete(),
            Err(PromiseErr::AlreadyFulfilled)
        ));
        assert!(matches!(block_on(future), Ok(())));
    }

    #[test]
    fn claimable_promise_pointer_equality_matches_shared_handle() {
        let (handle, _future) = KClaimablePromise::<u8>::create_pair();
        let clone = handle.clone();

        assert_eq!(handle, clone);
    }
}
