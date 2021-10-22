use crate::spec::chart::{ChartSpec, MutChartVisitor};
use crate::error::Result;
use crate::spec::mark::MarkSpec;
use crate::spec::data::DataSpec;
use crate::proto::gen::tasks::Variable;

pub fn extract_server_data(client_spec: &mut ChartSpec) -> Result<ChartSpec> {
    let mut extract_server_visitor = ExtractServerDataVisitor::new();
    client_spec.walk_mut(&mut extract_server_visitor)?;

    Ok(extract_server_visitor.server_spec)
}


#[derive(Debug, Default)]
pub struct ExtractServerDataVisitor {
    pub server_spec: ChartSpec,
}

impl ExtractServerDataVisitor {
    pub fn new() -> Self {
        Self {
            server_spec: Default::default(),
        }
    }
}

impl MutChartVisitor for ExtractServerDataVisitor {
    fn visit_data(&mut self, data: &mut DataSpec, scope: &[u32]) -> Result<()> {
        // Add clone to server data
        let server_data = data.clone();
        if scope.is_empty() {
            self.server_spec.data.push(server_data)
        } else {
            let server_group = self.server_spec.get_nested_group_mut(scope)?;
            server_group.data.push(server_data);
        }

        // Clear everything except name from client spec
        data.format = None;
        data.source = None;
        data.values = None;
        data.transform = Vec::new();
        data.on = None;
        data.url = None;

        Ok(())
    }

    fn visit_group_mark(&mut self, _mark: &mut MarkSpec, scope: &[u32]) -> Result<()> {
        // Initialize group mark in server spec
        let parent_scope = &scope[..scope.len() - 1];
        let new_group = MarkSpec {
            type_: "group".to_string(),
            name: None,
            from: None,
            encode: None,
            data: vec![],
            signals: vec![],
            marks: vec![],
            scales: vec![],
            extra: Default::default(),
        };
        if parent_scope.is_empty() {
            self.server_spec.marks.push(new_group);
        } else {
            let parent_group = self.server_spec.get_nested_group_mut(parent_scope)?;
            parent_group.marks.push(new_group);
        }

        Ok(())
    }
}