use crate::proto::gen::tasks::{ScanUrlTask, Variable};

impl ScanUrlTask {
    pub fn url(&self) -> &Variable {
        self.url.as_ref().unwrap()
    }
}
