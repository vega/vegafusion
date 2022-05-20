use crate::expression::column_usage::{
    ColumnUsage, DatasetsColumnUsage, GetDatasetsColumnUsage, VlSelectionFields,
};
use crate::expression::parser::parse;
use crate::proto::gen::tasks::Variable;
use crate::spec::chart::ChartSpec;
use crate::spec::data::DataSpec;
use crate::spec::mark::{MarkEncodeSpec, MarkEncodingField, MarkEncodingSpec, MarkSpec};
use crate::spec::scale::{ScaleDataReferenceSpec, ScaleDomainSpec, ScaleRangeSpec, ScaleSpec};
use crate::spec::signal::{SignalOnEventSpec, SignalSpec};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;

impl GetDatasetsColumnUsage for MarkEncodingField {
    fn datasets_column_usage(
        &self,
        datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        if let Some(datum_var) = datum_var {
            let column_usage = match self {
                MarkEncodingField::Field(field) => {
                    if field.contains(".") || field.contains("[") {
                        // Specification of a nested column like "target['x']" or "source.x"
                        // (https://vega.github.io/vega/docs/types/#Field)
                        // Eventually we could add a separate parser to identify the column portion,
                        // but for now just declare as unknown column usage
                        ColumnUsage::Unknown
                    } else {
                        ColumnUsage::empty().with_column(field)
                    }
                }
                MarkEncodingField::Object(_) => {
                    // Field is an object that should have a "field" property.
                    // Eventually we can add support for this form, for now declare as unknown
                    // column usage
                    ColumnUsage::Unknown
                }
            };
            DatasetsColumnUsage::empty().with_column_usage(datum_var, column_usage)
        } else {
            DatasetsColumnUsage::empty()
        }
    }
}

impl GetDatasetsColumnUsage for MarkEncodingSpec {
    fn datasets_column_usage(
        &self,
        datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        let mut usage = DatasetsColumnUsage::empty();

        if let Some(datum_var) = datum_var {
            // Handle direct field references
            if let Some(field) = &self.field {
                usage = usage.union(&field.datasets_column_usage(
                    &Some(datum_var.clone()),
                    usage_scope,
                    task_scope,
                    &vl_selection_fields,
                ))
            }

            // Handle signal
            if let Some(signal) = &self.signal {
                match parse(signal) {
                    Ok(parsed) => {
                        usage = usage.union(&parsed.datasets_column_usage(
                            &Some(datum_var.clone()),
                            usage_scope,
                            task_scope,
                            &vl_selection_fields,
                        ))
                    }
                    Err(_) => {
                        // Failed to parse expression, unknown column usage
                        usage = usage.with_unknown_usage(&datum_var);
                    }
                }
            }

            // Handle test expression
            if let Some(signal) = &self.test {
                match parse(signal) {
                    Ok(parsed) => {
                        usage = usage.union(&parsed.datasets_column_usage(
                            &Some(datum_var.clone()),
                            usage_scope,
                            task_scope,
                            &vl_selection_fields,
                        ))
                    }
                    Err(_) => {
                        // Failed to parse expression, unknown column usage
                        usage = usage.with_unknown_usage(&datum_var);
                    }
                }
            }
        }
        usage
    }
}

impl GetDatasetsColumnUsage for MarkEncodeSpec {
    fn datasets_column_usage(
        &self,
        datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        // Initialize empty usage
        let mut usage = DatasetsColumnUsage::empty();

        // Iterate over all encoding channels
        for encoding_spec in self.encodings.values() {
            for encoding_or_list in encoding_spec.channels.values() {
                for encoding in encoding_or_list.to_vec() {
                    usage = usage.union(&encoding.datasets_column_usage(
                        datum_var,
                        usage_scope,
                        task_scope,
                        vl_selection_fields,
                    ))
                }
            }
        }

        usage
    }
}

impl GetDatasetsColumnUsage for MarkSpec {
    fn datasets_column_usage(
        &self,
        _datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        // Initialize empty usage
        let mut usage = DatasetsColumnUsage::empty();
        if self.type_ == "group" {
            // group marks with data, signals, scales, marks
            for sig in &self.signals {
                usage = usage.union(&sig.datasets_column_usage(
                    &None,
                    usage_scope,
                    task_scope,
                    vl_selection_fields,
                ))
            }

            for scale in &self.scales {
                usage = usage.union(&scale.datasets_column_usage(
                    &None,
                    usage_scope,
                    task_scope,
                    vl_selection_fields,
                ))
            }

            for data in &self.data {
                usage = usage.union(&data.datasets_column_usage(
                    &None,
                    usage_scope,
                    task_scope,
                    vl_selection_fields,
                ))
            }

            let mut child_group_idx = 0;
            for mark in &self.marks {
                if mark.type_ == "group" {
                    let mut child_usage_scope = Vec::from(usage_scope);
                    child_usage_scope.push(child_group_idx as u32);
                    usage = usage.union(&mark.datasets_column_usage(
                        &None,
                        child_usage_scope.as_slice(),
                        task_scope,
                        vl_selection_fields,
                    ));
                    child_group_idx += 1;
                } else {
                    usage = usage.union(&mark.datasets_column_usage(
                        &None,
                        usage_scope,
                        task_scope,
                        vl_selection_fields,
                    ))
                }
            }
        } else {
            // non-group mark with "from" data source
            if let Some(from) = &self.from {
                let data_name = if let Some(data) = &from.data {
                    Some(data.clone())
                } else if let Some(facet) = &from.facet {
                    Some(facet.data.clone())
                } else {
                    None
                };

                if let Some(data_name) = data_name {
                    let data_var = Variable::new_data(&data_name);
                    if let Ok(resolved) = task_scope.resolve_scope(&data_var, usage_scope) {
                        let scoped_datum_var: ScopedVariable = (resolved.var, resolved.scope);
                        if let Some(encode) = &self.encode {
                            usage = usage.union(&encode.datasets_column_usage(
                                &Some(scoped_datum_var),
                                usage_scope,
                                task_scope,
                                vl_selection_fields,
                            ))
                        }
                    }
                }
            }
        }

        usage
    }
}

impl GetDatasetsColumnUsage for ScaleDataReferenceSpec {
    fn datasets_column_usage(
        &self,
        _datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        let mut usage = DatasetsColumnUsage::empty();
        let data_var = Variable::new_data(&self.data);
        if let Ok(resolved) = task_scope.resolve_scope(&data_var, usage_scope) {
            let scoped_datum_var: ScopedVariable = (resolved.var, resolved.scope);
            usage =
                usage.with_column_usage(&scoped_datum_var, ColumnUsage::from(self.field.as_str()))
        }

        usage
    }
}

impl GetDatasetsColumnUsage for ScaleDomainSpec {
    fn datasets_column_usage(
        &self,
        _datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        let mut usage = DatasetsColumnUsage::empty();
        let scale_data_refs = match &self {
            ScaleDomainSpec::FieldReference(field) => {
                vec![field.clone()]
            }
            ScaleDomainSpec::FieldsReference(fields) => fields.fields.clone(),
            _ => Vec::new(),
        };
        for scale_data_ref in scale_data_refs {
            usage = usage.union(&scale_data_ref.datasets_column_usage(
                &None,
                usage_scope,
                task_scope,
                vl_selection_fields,
            ))
        }
        usage
    }
}

impl GetDatasetsColumnUsage for ScaleRangeSpec {
    fn datasets_column_usage(
        &self,
        _datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        let mut usage = DatasetsColumnUsage::empty();
        match &self {
            ScaleRangeSpec::Reference(data_ref) => {
                usage = usage.union(&data_ref.datasets_column_usage(
                    &None,
                    usage_scope,
                    task_scope,
                    vl_selection_fields,
                ))
            }
            _ => {}
        }
        usage
    }
}

impl GetDatasetsColumnUsage for ScaleSpec {
    fn datasets_column_usage(
        &self,
        _datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        let mut usage = DatasetsColumnUsage::empty();
        if let Some(domain) = &self.domain {
            usage = usage.union(&domain.datasets_column_usage(
                &None,
                usage_scope,
                task_scope,
                vl_selection_fields,
            ))
        }

        if let Some(range) = &self.range {
            usage = usage.union(&range.datasets_column_usage(
                &None,
                usage_scope,
                task_scope,
                vl_selection_fields,
            ))
        }
        usage
    }
}

impl GetDatasetsColumnUsage for SignalSpec {
    fn datasets_column_usage(
        &self,
        _datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        let mut usage = DatasetsColumnUsage::empty();
        let mut expr_strs = Vec::new();

        // Collect all expression strings used in the signal definition
        // init
        if let Some(init) = &self.init {
            expr_strs.push(init.clone())
        }

        // update
        if let Some(update) = &self.update {
            expr_strs.push(update.clone())
        }

        // on
        for sig_on in &self.on {
            expr_strs.push(sig_on.update.clone());
            for sig_event in sig_on.events.to_vec() {
                if let SignalOnEventSpec::Signal(signal) = sig_event {
                    expr_strs.push(signal.signal.clone());
                }
            }
        }

        for expr_str in expr_strs {
            if let Ok(parsed) = parse(&expr_str) {
                usage = usage.union(&parsed.datasets_column_usage(
                    &None,
                    usage_scope,
                    task_scope,
                    vl_selection_fields,
                ))
            }
        }

        usage
    }
}

impl GetDatasetsColumnUsage for DataSpec {
    fn datasets_column_usage(
        &self,
        _datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        let mut usage = DatasetsColumnUsage::empty();
        if let Some(source) = &self.source {
            // For right now, assume that all columns in source dataset are required by this
            // dataset. Eventually we'll want to examine the individual transforms in this dataset
            // to determine the precise subset of columns that are required.
            let source_var = Variable::new_data(source);
            if let Ok(resolved) = _task_scope.resolve_scope(&source_var, usage_scope) {
                let data_var = (resolved.var, resolved.scope);
                usage = usage.with_unknown_usage(&data_var);
            }
        }
        usage
    }
}

impl GetDatasetsColumnUsage for ChartSpec {
    fn datasets_column_usage(
        &self,
        _datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> DatasetsColumnUsage {
        // Initialize empty usage
        let mut usage = DatasetsColumnUsage::empty();

        // group marks with data, signals, scales, marks
        for sig in &self.signals {
            usage = usage.union(&sig.datasets_column_usage(
                &None,
                usage_scope,
                task_scope,
                vl_selection_fields,
            ))
        }

        for scale in &self.scales {
            usage = usage.union(&scale.datasets_column_usage(
                &None,
                usage_scope,
                task_scope,
                vl_selection_fields,
            ))
        }

        for data in &self.data {
            usage = usage.union(&data.datasets_column_usage(
                &None,
                usage_scope,
                task_scope,
                vl_selection_fields,
            ))
        }

        let mut child_group_idx = 0;
        for mark in &self.marks {
            if mark.type_ == "group" {
                let mut child_usage_scope = Vec::from(usage_scope);
                child_usage_scope.push(child_group_idx as u32);
                usage = usage.union(&mark.datasets_column_usage(
                    &None,
                    child_usage_scope.as_slice(),
                    task_scope,
                    vl_selection_fields,
                ));
                child_group_idx += 1;
            } else {
                usage = usage.union(&mark.datasets_column_usage(
                    &None,
                    usage_scope,
                    task_scope,
                    vl_selection_fields,
                ))
            }
        }

        usage
    }
}

#[cfg(test)]
mod tests {
    use crate::expression::column_usage::{
        ColumnUsage, DatasetsColumnUsage, GetDatasetsColumnUsage, VlSelectionFields,
    };
    use crate::proto::gen::tasks::Variable;
    use crate::spec::data::DataSpec;
    use crate::spec::mark::{MarkEncodeSpec, MarkSpec};
    use crate::spec::scale::ScaleSpec;
    use crate::spec::signal::SignalSpec;
    use crate::task_graph::graph::ScopedVariable;
    use crate::task_graph::scope::TaskScope;
    use serde_json::json;

    fn selection_fields() -> VlSelectionFields {
        vec![(
            (Variable::new_data("brush2_store"), Vec::new()),
            vec!["AA".to_string(), "BB".to_string(), "CC".to_string()],
        )]
        .into_iter()
        .collect()
    }

    fn task_scope() -> TaskScope {
        let mut task_scope = TaskScope::new();
        task_scope
            .add_variable(&Variable::new_data("brush2_store"), &[])
            .unwrap();
        task_scope
            .add_variable(&Variable::new_data("dataA"), &[])
            .unwrap();
        task_scope
    }

    #[test]
    fn test_mark_encoding_column_known_usage() {
        // Define selection dataset fields
        let selection_fields = selection_fields();

        let encodings: MarkEncodeSpec = serde_json::from_value(json!({
            "update": {
                "x": {"field": "one", "scale": "scale_a"},
                "y": [
                    {"field": "three", "scale": "scale_a", "test": "datum.two > 7"},
                    {"value": 23},
                ],
                "opacity": [
                    {"signal": "datum['four'] * 2", "test": "vlSelectionTest('brush2_store', datum)"},
                    {"value": 0.3},
                ]
            }
        })).unwrap();

        // Build dataset_column_usage args
        let datum_var: ScopedVariable = (Variable::new_data("dataA"), Vec::new());
        let usage_scope = Vec::new();
        let task_scope = task_scope();

        let usage = encodings.datasets_column_usage(
            &Some(datum_var.clone()),
            &usage_scope,
            &task_scope,
            &selection_fields,
        );

        let expected = DatasetsColumnUsage::empty()
            .with_column_usage(
                &datum_var,
                ColumnUsage::from(vec!["AA", "BB", "CC", "one", "two", "three", "four"].as_slice()),
            )
            .with_unknown_usage(&(Variable::new_data("brush2_store"), Vec::new()));

        assert_eq!(usage, expected);

        // // Without selection fields column usage should be unknown
        let usage = encodings.datasets_column_usage(
            &Some(datum_var.clone()),
            &usage_scope,
            &task_scope,
            &Default::default(),
        );
        let expected = DatasetsColumnUsage::empty()
            .with_unknown_usage(&datum_var)
            .with_unknown_usage(&(Variable::new_data("brush2_store"), Vec::new()));

        assert_eq!(usage, expected);
    }

    #[test]
    fn test_mark_with_known_usage() {
        // Define selection dataset fields
        let selection_fields = selection_fields();

        let mark: MarkSpec = serde_json::from_value(json!({
            "type": "rect",
            "from": {"data": "dataA"},
            "encode": {
                "init": {
                    "x": {"field": "one", "scale": "scale_a"},
                    "y": [
                        {"field": "three", "scale": "scale_a", "test": "datum.two > 7"},
                        {"value": 23},
                    ],
                },
                "update": {
                    "opacity": [
                        {"signal": "datum['four'] * 2", "test": "vlSelectionTest('brush2_store', datum)"},
                        {"value": 0.3},
                    ]
                }
            }
        })).unwrap();

        // Build dataset_column_usage args
        let usage_scope = Vec::new();
        let task_scope = task_scope();

        let usage = mark.datasets_column_usage(&None, &usage_scope, &task_scope, &selection_fields);

        let expected = DatasetsColumnUsage::empty()
            .with_column_usage(
                &(Variable::new_data("dataA"), Vec::new()),
                ColumnUsage::from(vec!["AA", "BB", "CC", "one", "two", "three", "four"].as_slice()),
            )
            .with_unknown_usage(&(Variable::new_data("brush2_store"), Vec::new()));

        assert_eq!(usage, expected);
    }

    #[test]
    fn test_scale_usage() {
        let scale: ScaleSpec = serde_json::from_value(json!({
            "name": "color",
            "scale": "quantize",
            "domain": {"data": "dataA", "field": "colZ"},
            "range": {"scheme": "blues", "count": 7}
        }))
        .unwrap();

        // Build dataset_column_usage args
        let usage_scope = Vec::new();
        let task_scope = task_scope();

        let usage =
            scale.datasets_column_usage(&None, &usage_scope, &task_scope, &Default::default());

        let expected = DatasetsColumnUsage::empty().with_column_usage(
            &(Variable::new_data("dataA"), Vec::new()),
            ColumnUsage::from(vec!["colZ"].as_slice()),
        );

        assert_eq!(usage, expected);
    }

    #[test]
    fn test_signal_usage() {
        let signal: SignalSpec = serde_json::from_value(json!({
            "name": "indexDate",
            "description": "A date value that updates in response to mousemove.",
            "update": "length(data('brush2_store'))",
            "on": [{"events": "mousemove", "update": "length(data('dataA'))"}]
        }))
        .unwrap();

        // Build dataset_column_usage args
        let usage_scope = Vec::new();
        let task_scope = task_scope();

        let usage =
            signal.datasets_column_usage(&None, &usage_scope, &task_scope, &Default::default());

        println!("{:#?}", usage);

        let expected = DatasetsColumnUsage::empty()
            .with_unknown_usage(&(Variable::new_data("brush2_store"), Vec::new()))
            .with_unknown_usage(&(Variable::new_data("dataA"), Vec::new()));

        assert_eq!(usage, expected);
    }

    #[test]
    fn test_data_usage() {
        let dataset: DataSpec = serde_json::from_value(json!({
            "name": "dataB",
            "source": "dataA",
            "transform": []
        }))
        .unwrap();

        // Build dataset_column_usage args
        let usage_scope = Vec::new();
        let task_scope = task_scope();

        let usage =
            dataset.datasets_column_usage(&None, &usage_scope, &task_scope, &Default::default());

        println!("{:#?}", usage);

        let expected = DatasetsColumnUsage::empty()
            .with_unknown_usage(&(Variable::new_data("dataA"), Vec::new()));

        assert_eq!(usage, expected);
    }
}
