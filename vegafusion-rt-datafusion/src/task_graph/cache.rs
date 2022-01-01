use async_lock::{Mutex, RwLock};
use futures::FutureExt;
use lru::LruCache;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::panic::{resume_unwind, AssertUnwindSafe};
use std::sync::Arc;
use vegafusion_core::error::{DuplicateResult, Result, ToExternalError};
use vegafusion_core::task_graph::task_value::TaskValue;

#[derive(Debug, Clone)]
struct CachedValue {
    value: NodeValue, // Maybe add metrics like compute time, or a cache weight
}

type NodeValue = (TaskValue, Vec<TaskValue>);
type Initializer = Arc<RwLock<Option<Result<NodeValue>>>>;

#[derive(Debug, Clone)]
pub struct VegaFusionCache {
    values: Arc<Mutex<LruCache<u64, CachedValue>>>,
    initializers: Arc<RwLock<HashMap<u64, Initializer>>>,
}

impl VegaFusionCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            values: Arc::new(Mutex::new(LruCache::new(capacity))),
            initializers: Default::default(),
        }
    }

    pub async fn clear(&self) {
        // Clear the values cache. There may still be initializers representing in progress
        // futures which will not be cleared.
        self.values.lock().await.clear();
    }

    async fn get_from_values(&self, state_fingerprint: u64) -> Option<CachedValue> {
        // This is a write lock because the LruCache.get function mutates the Cache to update
        // the LRU status
        self.values.lock().await.get(&state_fingerprint).cloned()
    }

    async fn set_value(&self, state_fingerprint: u64, value: NodeValue) -> Option<CachedValue> {
        self.values
            .lock()
            .await
            .put(state_fingerprint, CachedValue { value })
    }

    async fn remove_initializer(&self, state_fingerprint: u64) -> Option<Initializer> {
        self.initializers.write().await.remove(&state_fingerprint)
    }

    pub async fn get_or_try_insert_with<F>(
        &self,
        state_fingerprint: u64,
        init: F,
    ) -> Result<NodeValue>
    where
        F: Future<Output = Result<NodeValue>> + Send + 'static,
    {
        // Check if present in the values cache
        if let Some(value) = self.get_from_values(state_fingerprint).await {
            return Ok(value.value);
        }

        // Check if present in initializers
        let mut initializers_lock = self.initializers.write().await;

        match initializers_lock.entry(state_fingerprint) {
            Entry::Occupied(entry) => {
                // Calculation is in progress, await on Arc clone of it's initializer
                let initializer = entry.get().clone();
                // Drop lock on initializers collection
                drop(initializers_lock);
                let result = initializer.read().await;
                let result = result.as_ref().unwrap();
                result.duplicate()
            }
            Entry::Vacant(entry) => {
                // Create new initializer
                let initializer: Initializer = Arc::new(RwLock::new(None));

                // Get and hold write lock for initializer
                let mut initializer_lock = initializer.write().await;

                // Store Arc clone of initializer in initializers map
                entry.insert(initializer.clone());

                // Drop write lock
                drop(initializers_lock);

                // Invoke future to initialize
                match AssertUnwindSafe(tokio::spawn(init)).catch_unwind().await {
                    // Resolved.
                    Ok(Ok(value)) => {
                        // If result Ok, clone to values
                        match value {
                            Ok(value) => {
                                *initializer_lock = Some(Ok(value.clone()));
                                self.set_value(state_fingerprint, value.clone()).await;

                                // Stored initializer no longer required. Initializers are Arc
                                // pointers, so it's fine to drop initializer from here even if
                                // other tasks are still awaiting on it.
                                self.remove_initializer(state_fingerprint).await;
                                Ok(value)
                            }
                            Err(e) => {
                                // Remove initializer so that another future can try again
                                *initializer_lock = Some(Err(e.duplicate()));
                                self.remove_initializer(state_fingerprint).await;
                                Err(e)
                            }
                        }
                    }
                    Ok(Err(err)) => Err(err).external("tokio error"),
                    // Panicked.
                    Err(payload) => {
                        // Remove the waiter so that others can retry.
                        self.remove_initializer(state_fingerprint).await;
                        // triggers panic, so no return value in this branch
                        resume_unwind(payload);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test_cache {
    use crate::task_graph::cache::{NodeValue, VegaFusionCache};
    use tokio::time::Duration;
    use vegafusion_core::data::scalar::ScalarValue;
    use vegafusion_core::error::Result;
    use vegafusion_core::task_graph::task_value::TaskValue;

    async fn make_value(value: ScalarValue) -> Result<NodeValue> {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        Ok((TaskValue::Scalar(value), Vec::new()))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_cache() {
        let cache = VegaFusionCache::new(4);

        let value_future1 = cache.get_or_try_insert_with(1, make_value(ScalarValue::from(23.5)));
        let value_future2 = cache.get_or_try_insert_with(1, make_value(ScalarValue::from(23.5)));
        let value_future3 = cache.get_or_try_insert_with(1, make_value(ScalarValue::from(23.5)));

        tokio::time::sleep(Duration::from_millis(100)).await;
        println!("{:?}", cache.initializers);

        // assert_eq!(cache.num_values().await, 0);
        // assert_eq!(cache.num_initializers().await, 1);

        let futures = vec![value_future1, value_future2];
        let values = futures::future::join_all(futures).await;

        let next_value = value_future3.await;

        // tokio::time::sleep(Duration::from_millis(300));
        println!("{:?}", cache.initializers);
        // assert_eq!(cache.num_values().await, 1);
        // assert_eq!(cache.num_initializers().await, 0);

        println!("values: {:?}", values);
        println!("next_value: {:?}", next_value);
    }
}
