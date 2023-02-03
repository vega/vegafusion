use tokio::runtime::Runtime;

/// Double default stack size from 2MB to 4MB
pub const TOKIO_THREAD_STACK_SIZE: usize = 4 * 1024 * 1024;

lazy_static! {
    pub static ref TOKIO_RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
        .build()
        .unwrap();
}
