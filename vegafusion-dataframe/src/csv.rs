use arrow::datatypes::Schema;

/// Options that control the reading of CSV files.
/// Simplification of CsvReadOptions from DataFusion
#[derive(Clone, Debug)]
pub struct CsvReadOptions {
    /// Does the CSV file have a header?
    ///
    /// If schema inference is run on a file with no headers, default column names
    /// are created.
    pub has_header: bool,
    /// An optional column delimiter. Defaults to `b','`.
    pub delimiter: u8,
    /// An optional schema representing the CSV files. If None, CSV reader will try to infer it
    /// based on data in file.
    pub schema: Option<Schema>,
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to `FileType::CSV.get_ext().as_str()`.
    pub file_extension: String,
}

impl Default for CsvReadOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            schema: None,
            file_extension: ".csv".to_string(),
        }
    }
}
