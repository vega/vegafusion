use crate::expression::column_usage::{ColumnUsage, GetColumnUsage, VlSelectionFields};
use crate::expression::parser::parse;
use crate::spec::mark::{MarkEncodeSpec, MarkEncodingField, MarkEncodingSpec};

impl GetColumnUsage for MarkEncodingField {
    fn column_usage(&self, _vl_selection_fields: &VlSelectionFields) -> ColumnUsage {
        match self {
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
        }
    }
}

impl GetColumnUsage for MarkEncodingSpec {
    fn column_usage(&self, vl_selection_fields: &VlSelectionFields) -> ColumnUsage {
        let mut usage = ColumnUsage::empty();

        // Handle direct field references
        if let Some(field) = &self.field {
            usage = usage.union(&field.column_usage(&vl_selection_fields))
        }

        // Handle signal
        if let Some(signal) = &self.signal {
            match parse(signal) {
                Ok(parsed) => usage = usage.union(&parsed.column_usage(&vl_selection_fields)),
                Err(_) => {
                    // Failed to parse expression, unknown column usage
                    return ColumnUsage::Unknown;
                }
            }
        }

        // Handle test expression
        if let Some(signal) = &self.test {
            match parse(signal) {
                Ok(parsed) => usage = usage.union(&parsed.column_usage(&vl_selection_fields)),
                Err(_) => {
                    // Failed to parse expression, unknown column usage
                    return ColumnUsage::Unknown;
                }
            }
        }

        usage
    }
}

impl GetColumnUsage for MarkEncodeSpec {
    fn column_usage(&self, vl_selection_fields: &VlSelectionFields) -> ColumnUsage {
        // Initialize empty usage
        let mut usage = ColumnUsage::empty();

        // Iterate over all encoding channels
        for encoding_spec in self.encodings.values() {
            for encoding_or_list in encoding_spec.channels.values() {
                for encoding in encoding_or_list.to_vec() {
                    usage = usage.union(&encoding.column_usage(&vl_selection_fields))
                }
            }
        }

        usage
    }
}

#[cfg(test)]
mod tests {
    use crate::expression::column_usage::{ColumnUsage, GetColumnUsage, VlSelectionFields};
    use crate::spec::mark::MarkEncodeSpec;
    use serde_json::json;

    #[test]
    fn test_mark_encoding_column_known_usage() {
        // Define selection dataset fields
        let selection_fields: VlSelectionFields = vec![(
            "brush2_store".to_string(),
            vec!["AA".to_string(), "BB".to_string(), "CC".to_string()],
        )]
        .into_iter()
        .collect();

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

        let usage = encodings.column_usage(&selection_fields);
        let expected =
            ColumnUsage::from(vec!["AA", "BB", "CC", "one", "two", "three", "four"].as_slice());
        assert_eq!(usage, expected);

        // Without selection fields column usage should be unknown
        let usage = encodings.column_usage(&Default::default());
        assert_eq!(usage, ColumnUsage::Unknown);
    }
}
