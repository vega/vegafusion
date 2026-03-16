use regex::Regex;
use std::sync::LazyLock;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_expr::LogicalPlan;
use vegafusion_common::error::Result;

pub enum ResolutionResult {
    /// Resolver fully materialized the plan
    Table(VegaFusionTable),
    /// Resolver produced a rewritten plan for the next resolver to handle,
    /// or for DataFusion to execute if this is the last resolver
    Plan(LogicalPlan),
}

/// Three-state base URL setting for public API boundaries.
#[derive(Clone, Debug, Default)]
pub enum DataBaseUrlSetting {
    /// Use the default CDN base URL (vega-datasets)
    #[default]
    Default,
    /// Disable base URL; relative paths produce an error
    Disabled,
    /// Use a custom base URL (scheme URL or absolute path)
    Custom(String),
}

/// Parsed URL representation passed to resolvers during the scan phase.
/// All fields are populated from the fully-resolved URL (after base URL
/// resolution and hash-stripping). Resolvers pattern-match on these fields
/// rather than doing their own URL string parsing.
pub struct ParsedUrl {
    /// Original URL string (after base URL resolution and hash-stripping)
    pub url: String,
    /// URL scheme (http, https, file, s3, spark, etc.) — always present
    pub scheme: String,
    /// Host/authority component (e.g. "example.com", S3 bucket name)
    pub host: Option<String>,
    /// Path component
    pub path: String,
    /// Query parameters in URL order, preserving duplicates
    pub query_params: Vec<(String, String)>,
    /// File extension extracted from path (e.g. "csv", "parquet")
    pub extension: Option<String>,
    /// Explicit format type from Vega spec (overrides extension)
    pub format_type: Option<String>,
    /// Parse spec from Vega format (e.g., {"date": "date"} for CSV column typing)
    pub parse: Option<crate::proto::gen::tasks::scan_url_format::Parse>,
}

/// Map a DataBaseUrlSetting (from public API) to the two-state Option<String>
/// used by PlannerConfig. Custom base URLs are normalized (bare absolute paths
/// become file:// URLs).
pub fn resolve_data_base_url(
    api_value: DataBaseUrlSetting,
    default: Option<String>,
) -> Result<Option<String>> {
    match api_value {
        DataBaseUrlSetting::Default => Ok(default),
        DataBaseUrlSetting::Disabled => Ok(None),
        DataBaseUrlSetting::Custom(s) => Ok(Some(normalize_base_url(s)?)),
    }
}

static URL_SCHEME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(//|[a-zA-Z][a-zA-Z0-9+.\-]*://)").unwrap());

/// Returns true if the string is already a URL (has a scheme per RFC 3986)
/// or is scheme-relative (starts with //).
pub fn has_url_scheme(s: &str) -> bool {
    URL_SCHEME_RE.is_match(s)
}

/// Returns true if `path` is an absolute filesystem path.
/// Unix: starts with `/`. Windows: starts with a drive letter `[A-Za-z]:\` or `[A-Za-z]:/`.
pub fn is_absolute_path(path: &str) -> bool {
    let bytes = path.as_bytes();
    if bytes.first() == Some(&b'/') {
        return true;
    }
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes[2] == b'\\' || bytes[2] == b'/')
}

/// Normalize a base URL so it always has a scheme.
/// Bare absolute paths become file:// URLs; scheme-relative URLs get
/// https: prepended; scheme URLs are preserved as-is; everything else is rejected.
pub fn normalize_base_url(base: String) -> Result<String> {
    if base.starts_with("//") {
        // Protocol-relative URL — prepend https: so url::Url::parse works
        Ok(format!("https:{base}"))
    } else if has_url_scheme(&base) {
        Ok(base)
    } else if is_absolute_path(&base) {
        path_to_file_url(&base)
    } else {
        Err(vegafusion_common::error::VegaFusionError::specification(
            format!("data_base_url must be absolute (scheme URL or absolute path), got: {base}"),
        ))
    }
}

/// Convert an absolute local path to a file:// URL.
/// Uses url::Url::from_file_path() for correct percent-encoding.
#[cfg(not(target_arch = "wasm32"))]
pub fn path_to_file_url(path: &str) -> Result<String> {
    let normalized = path.replace('\\', "/");
    let p = std::path::Path::new(&normalized);
    url::Url::from_file_path(p)
        .map(|u| u.to_string())
        .map_err(|_| {
            vegafusion_common::error::VegaFusionError::specification(format!(
                "Cannot convert path to file URL: {}",
                p.display()
            ))
        })
}

/// Browser-wasm fallback: `url::Url::from_file_path` is unavailable on
/// `wasm32-unknown-unknown` (not compiled for that target in the `url` crate),
/// and `std::path` absolute-path semantics on that target do not recognize
/// POSIX-like virtual paths such as `/foo`.
///
/// We therefore synthesize a `file:` URL for the restricted path forms we
/// expect here. Unlike `Url::from_file_path`, this does **not** percent-encode
/// reserved characters, so inputs must not contain `#`, `?`, etc.
#[cfg(target_arch = "wasm32")]
pub fn path_to_file_url(path: &str) -> Result<String> {
    let normalized = path.replace('\\', "/");
    Ok(format!("file://{normalized}"))
}

/// Resolve a spec URL against a base URL. This is the shared function used by
/// both plan-time resolution (MakeTasksVisitor for Url::String) and eval-time
/// resolution (DataUrlTask::eval for Url::Expr).
pub fn resolve_url(url: &str, data_base_url: &Option<String>) -> Result<String> {
    // Future: This is the natural place for a URL permissions layer
    if url.starts_with("//") {
        // Protocol-relative URL — prepend https: so downstream parsers work
        Ok(format!("https:{url}"))
    } else if has_url_scheme(url) {
        Ok(url.to_string())
    } else if is_absolute_path(url) {
        path_to_file_url(url)
    } else {
        // Relative path — resolve against base URL using RFC 3986 joining
        match data_base_url {
            Some(base) => {
                let base_url = url::Url::parse(base).map_err(|e| {
                    vegafusion_common::error::VegaFusionError::specification(format!(
                        "Invalid base URL '{base}': {e}"
                    ))
                })?;
                let resolved = base_url.join(url).map_err(|e| {
                    vegafusion_common::error::VegaFusionError::specification(format!(
                        "Cannot resolve '{url}' against base '{base}': {e}"
                    ))
                })?;
                Ok(resolved.to_string())
            }
            None => Err(vegafusion_common::error::VegaFusionError::specification(
                format!("Relative URL with no base URL configured: {url}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── has_url_scheme ──

    #[test]
    fn test_has_url_scheme_https() {
        assert!(has_url_scheme("https://example.com/data.csv"));
    }

    #[test]
    fn test_has_url_scheme_custom() {
        assert!(has_url_scheme("spark://org.users"));
    }

    #[test]
    fn test_has_url_scheme_scheme_relative() {
        assert!(has_url_scheme("//example.com/data.csv"));
    }

    #[test]
    fn test_has_url_scheme_absolute_path() {
        assert!(!has_url_scheme("/tmp/data.csv"));
    }

    #[test]
    fn test_has_url_scheme_relative() {
        assert!(!has_url_scheme("data/cars.json"));
    }

    #[test]
    fn test_has_url_scheme_embedded_scheme_in_query() {
        // Relative reference with "://" in a query parameter — must not be
        // misclassified as an absolute URL.
        assert!(!has_url_scheme("fetch?target=http://evil.com/data"));
    }

    #[test]
    fn test_has_url_scheme_embedded_scheme_in_path() {
        assert!(!has_url_scheme("foo/http://bar"));
    }

    // ── is_absolute_path ──

    #[test]
    fn test_is_absolute_path_unix() {
        assert!(is_absolute_path("/tmp/data.csv"));
    }

    #[test]
    fn test_is_absolute_path_windows_backslash() {
        assert!(is_absolute_path("C:\\tmp\\foo.csv"));
    }

    #[test]
    fn test_is_absolute_path_windows_forward() {
        assert!(is_absolute_path("C:/tmp/foo.csv"));
    }

    #[test]
    fn test_is_absolute_path_rejects_ambiguous_colon() {
        assert!(!is_absolute_path("a:b"));
    }

    #[test]
    fn test_is_absolute_path_rejects_digit_colon() {
        assert!(!is_absolute_path("1:/foo"));
    }

    #[test]
    fn test_is_absolute_path_rejects_relative() {
        assert!(!is_absolute_path("relative/path"));
    }

    // ── path_to_file_url ──

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_path_to_file_url_unix() {
        let result = path_to_file_url("/tmp/data.csv").unwrap();
        assert_eq!(result, "file:///tmp/data.csv");
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_path_to_file_url_spaces() {
        let result = path_to_file_url("/tmp/my data/file.csv").unwrap();
        assert_eq!(result, "file:///tmp/my%20data/file.csv");
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_path_to_file_url_hash() {
        let result = path_to_file_url("/tmp/file#1.csv").unwrap();
        assert!(
            result.contains("%23"),
            "Hash should be percent-encoded: {result}"
        );
    }

    // ── normalize_base_url ──

    #[test]
    fn test_normalize_base_url_scheme() {
        let result = normalize_base_url("https://example.com/data/".to_string()).unwrap();
        assert_eq!(result, "https://example.com/data/");
    }

    #[test]
    fn test_normalize_base_url_scheme_relative() {
        let result = normalize_base_url("//example.com/data/".to_string()).unwrap();
        assert_eq!(result, "https://example.com/data/");
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_normalize_base_url_absolute_path() {
        let result = normalize_base_url("/home/user/data".to_string()).unwrap();
        assert_eq!(result, "file:///home/user/data");
    }

    #[test]
    fn test_normalize_base_url_rejects_relative() {
        let result = normalize_base_url("relative/path".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_normalize_base_url_rejects_ambiguous_colon() {
        let result = normalize_base_url("a:b".to_string());
        assert!(result.is_err());
    }

    // ── resolve_url ──

    #[test]
    fn test_resolve_url_scheme_passthrough() {
        let base = Some("https://cdn.example.com/".to_string());
        let result = resolve_url("https://other.com/data.csv", &base).unwrap();
        assert_eq!(result, "https://other.com/data.csv");
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_resolve_url_absolute_path_to_file() {
        let base = Some("https://cdn.example.com/".to_string());
        let result = resolve_url("/tmp/data.csv", &base).unwrap();
        assert_eq!(result, "file:///tmp/data.csv");
    }

    #[test]
    fn test_resolve_url_relative_with_base() {
        let base = Some("https://raw.githubusercontent.com/vega/vega-datasets/v2.3.0/".to_string());
        let result = resolve_url("data/cars.json", &base).unwrap();
        assert_eq!(
            result,
            "https://raw.githubusercontent.com/vega/vega-datasets/v2.3.0/data/cars.json"
        );
    }

    #[test]
    fn test_resolve_url_relative_without_trailing_slash() {
        // Per RFC 3986, joining against a base without trailing slash replaces
        // the last path segment: "data" is replaced by "cars.json"
        let base = Some("https://example.com/data".to_string());
        let result = resolve_url("cars.json", &base).unwrap();
        assert_eq!(result, "https://example.com/cars.json");
    }

    #[test]
    fn test_resolve_url_relative_parent_traversal() {
        let base = Some("https://example.com/data/v2/".to_string());
        let result = resolve_url("../v1/cars.json", &base).unwrap();
        assert_eq!(result, "https://example.com/data/v1/cars.json");
    }

    #[test]
    fn test_resolve_url_relative_no_base_errors() {
        let result = resolve_url("data/cars.json", &None);
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_url_relative_with_embedded_scheme() {
        // A relative reference that contains "://" in a query parameter should
        // be joined against the base URL, not treated as absolute.
        let base = Some("https://proxy.com/".to_string());
        let result = resolve_url("fetch?target=http://evil.com/data", &base).unwrap();
        assert_eq!(
            result,
            "https://proxy.com/fetch?target=http://evil.com/data"
        );
    }

    // ── resolve_data_base_url ──

    #[test]
    fn test_resolve_data_base_url_default() {
        let default = Some("https://cdn.example.com/".to_string());
        let result = resolve_data_base_url(DataBaseUrlSetting::Default, default.clone()).unwrap();
        assert_eq!(result, default);
    }

    #[test]
    fn test_resolve_data_base_url_disabled() {
        let result = resolve_data_base_url(
            DataBaseUrlSetting::Disabled,
            Some("https://cdn.example.com/".to_string()),
        )
        .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_resolve_data_base_url_custom() {
        let result = resolve_data_base_url(
            DataBaseUrlSetting::Custom("https://my-server.com/data/".to_string()),
            Some("https://cdn.example.com/".to_string()),
        )
        .unwrap();
        assert_eq!(result, Some("https://my-server.com/data/".to_string()));
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_resolve_data_base_url_custom_path() {
        let result = resolve_data_base_url(
            DataBaseUrlSetting::Custom("/home/user/data".to_string()),
            None,
        )
        .unwrap();
        assert_eq!(result, Some("file:///home/user/data".to_string()));
    }
}
