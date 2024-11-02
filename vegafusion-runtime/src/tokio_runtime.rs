use cfg_if::cfg_if;
use tokio::runtime::Runtime;

/// Double default stack size from 2MB to 4MB
pub const TOKIO_THREAD_STACK_SIZE: usize = 4 * 1024 * 1024;

cfg_if! {
    if #[cfg(feature="multi-thread")] {
        lazy_static! {
            pub static ref TOKIO_RUNTIME: Runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
                .build()
                .unwrap();
        }
    } else {
        lazy_static! {
            pub static ref TOKIO_RUNTIME: Runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_stack_size(TOKIO_THREAD_STACK_SIZE)
                .build()
                .unwrap();
        }
    }
}
