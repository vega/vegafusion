use regex::Regex;
#[cfg(not(target_arch = "wasm32"))]
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use vegafusion_common::error::{Result, VegaFusionError};

/// Parsed URL representation passed to resolvers during the scan phase.
/// All fields are populated from the fully-resolved URL (after base URL
/// resolution and hash-stripping). Resolvers pattern-match on these fields
/// rather than doing their own URL string parsing.
#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AllowedBaseUrlPattern {
    Any,
    Scheme(String),
    Prefix(String),
    WildcardHost {
        scheme: String,
        host_suffix: String,
        path_prefix: String,
    },
    FilePathPrefix(PathBuf),
}

static URL_SCHEME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(//|[a-zA-Z][a-zA-Z0-9+.\-]*://)").unwrap());
static SCHEME_PATTERN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z][a-zA-Z0-9+.\-]*:$").unwrap());
static WILDCARD_HOST_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([a-zA-Z][a-zA-Z0-9+.\-]*)://\*\.([^/?#]+)(/[^?#]*)?$").unwrap()
});

#[cfg(not(target_arch = "wasm32"))]
fn normalize_file_base_url(base_url: String) -> Result<String> {
    let parsed = match url::Url::parse(&base_url) {
        Ok(parsed) => parsed,
        Err(_) => return Ok(base_url),
    };

    if parsed.scheme() != "file" {
        return Ok(base_url);
    }

    let Ok(path) = parsed.to_file_path() else {
        return Ok(base_url);
    };

    if path.is_dir() && !base_url.ends_with('/') {
        Ok(format!("{base_url}/"))
    } else {
        Ok(base_url)
    }
}

#[cfg(target_arch = "wasm32")]
fn normalize_file_base_url(base_url: String) -> Result<String> {
    Ok(base_url)
}

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
        normalize_file_base_url(base)
    } else if is_absolute_path(&base) {
        normalize_file_base_url(path_to_file_url(&base)?)
    } else {
        Err(VegaFusionError::specification(format!(
            "base_url must be absolute (scheme URL or absolute path), got: {base}"
        )))
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
            VegaFusionError::specification(format!(
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

#[cfg(not(target_arch = "wasm32"))]
pub fn file_url_to_path(url: &str) -> Result<PathBuf> {
    let parsed = url::Url::parse(url)
        .map_err(|e| VegaFusionError::specification(format!("Invalid file URL '{url}': {e}")))?;
    parsed.to_file_path().map_err(|_| {
        VegaFusionError::specification(format!("Cannot convert file URL to path: {url}"))
    })
}

#[cfg(target_arch = "wasm32")]
pub fn file_url_to_path(url: &str) -> Result<PathBuf> {
    Err(VegaFusionError::specification(format!(
        "Cannot convert file URL to path on wasm target: {url}"
    )))
}

#[cfg(not(target_arch = "wasm32"))]
fn portable_canonicalize(path: &Path) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path).map_err(|e| {
        VegaFusionError::specification(format!("Failed to resolve path {}: {e}", path.display()))
    })?;
    // On Windows, fs::canonicalize returns extended-length paths (\\?\C:\...)
    // which break prefix matching. Strip the prefix for consistent comparisons.
    #[cfg(target_os = "windows")]
    {
        let s = canonical.to_string_lossy();
        if let Some(stripped) = s.strip_prefix(r"\\?\") {
            return Ok(PathBuf::from(stripped));
        }
    }
    Ok(canonical)
}

#[cfg(target_arch = "wasm32")]
fn portable_canonicalize(path: &Path) -> Result<PathBuf> {
    Err(VegaFusionError::specification(format!(
        "Cannot canonicalize path on wasm target: {}",
        path.display()
    )))
}

pub fn canonicalize_path_for_policy_check(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return portable_canonicalize(path);
    }

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let canonical_parent = portable_canonicalize(parent)?;
    let Some(file_name) = path.file_name() else {
        return Err(VegaFusionError::specification(format!(
            "Failed to resolve local path {}: missing file name",
            path.display()
        )));
    };
    Ok(canonical_parent.join(file_name))
}

fn normalize_url_prefix(mut normalized: String) -> String {
    if !normalized.ends_with('/') {
        normalized.push('/');
    }
    normalized
}

pub fn normalize_allowed_base_urls(
    allowed_base_urls: Option<Vec<String>>,
) -> Result<Option<Vec<AllowedBaseUrlPattern>>> {
    allowed_base_urls
        .map(|urls| {
            urls.into_iter()
                .map(|url| normalize_allowed_base_url(&url))
                .collect::<Result<Vec<_>>>()
        })
        .transpose()
}

pub fn normalize_allowed_base_url(allowed_base_url: &str) -> Result<AllowedBaseUrlPattern> {
    if allowed_base_url == "*" {
        return Ok(AllowedBaseUrlPattern::Any);
    }

    if SCHEME_PATTERN_RE.is_match(allowed_base_url) {
        return Ok(AllowedBaseUrlPattern::Scheme(
            allowed_base_url[..allowed_base_url.len() - 1].to_ascii_lowercase(),
        ));
    }

    if is_absolute_path(allowed_base_url) || allowed_base_url.starts_with("file:///") {
        let path = if allowed_base_url.starts_with("file:///") {
            file_url_to_path(allowed_base_url)?
        } else {
            PathBuf::from(allowed_base_url)
        };
        let canonical = portable_canonicalize(&path)?;
        if !canonical.is_dir() {
            return Err(VegaFusionError::specification(format!(
                "Filesystem path in allowed_base_urls must be a directory: {}",
                canonical.display()
            )));
        }
        return Ok(AllowedBaseUrlPattern::FilePathPrefix(canonical));
    }

    if let Some(captures) = WILDCARD_HOST_RE.captures(allowed_base_url) {
        let scheme = captures.get(1).unwrap().as_str().to_ascii_lowercase();
        let host_suffix = captures.get(2).unwrap().as_str().to_ascii_lowercase();
        if host_suffix.is_empty() || host_suffix.contains('@') || host_suffix.contains(':') {
            return Err(VegaFusionError::specification(format!(
                "Invalid wildcard host pattern in allowed_base_urls: {allowed_base_url}"
            )));
        }
        let path_prefix = normalize_url_prefix(
            captures
                .get(3)
                .map(|m| m.as_str().to_string())
                .unwrap_or_else(|| "/".to_string()),
        );
        return Ok(AllowedBaseUrlPattern::WildcardHost {
            scheme,
            host_suffix,
            path_prefix,
        });
    }

    let parsed_url = url::Url::parse(allowed_base_url).map_err(|e| {
        VegaFusionError::specification(format!(
            "Invalid allowed_base_url '{allowed_base_url}': {e}"
        ))
    })?;

    if !parsed_url.username().is_empty() || parsed_url.password().is_some() {
        return Err(VegaFusionError::specification(format!(
            "allowed_base_url cannot include userinfo credentials: {allowed_base_url}"
        )));
    }

    if parsed_url.query().is_some() {
        return Err(VegaFusionError::specification(format!(
            "allowed_base_url cannot include a query component: {allowed_base_url}"
        )));
    }

    if parsed_url.fragment().is_some() {
        return Err(VegaFusionError::specification(format!(
            "allowed_base_url cannot include a fragment component: {allowed_base_url}"
        )));
    }

    Ok(AllowedBaseUrlPattern::Prefix(normalize_url_prefix(
        parsed_url.to_string(),
    )))
}

fn url_to_local_path(url: &str) -> Result<PathBuf> {
    if url.starts_with("file://") {
        file_url_to_path(url)
    } else if is_absolute_path(url) {
        Ok(PathBuf::from(url))
    } else {
        Err(VegaFusionError::specification(format!(
            "Expected local file path or file URL, got: {url}"
        )))
    }
}

pub fn is_url_allowed(url: &str, allowed_base_urls: &[AllowedBaseUrlPattern]) -> bool {
    let parsed_url = url::Url::parse(url).ok();

    allowed_base_urls.iter().any(|pattern| match pattern {
        AllowedBaseUrlPattern::Any => true,
        AllowedBaseUrlPattern::Scheme(scheme) => parsed_url
            .as_ref()
            .map(|parsed| parsed.scheme().eq_ignore_ascii_case(scheme))
            .unwrap_or(false),
        AllowedBaseUrlPattern::Prefix(prefix) => parsed_url
            .as_ref()
            .map(|parsed| parsed.as_str().starts_with(prefix))
            .unwrap_or(false),
        AllowedBaseUrlPattern::WildcardHost {
            scheme,
            host_suffix,
            path_prefix,
        } => parsed_url
            .as_ref()
            .and_then(|parsed| {
                parsed.host_str().map(|host| {
                    parsed.scheme().eq_ignore_ascii_case(scheme)
                        && (host.eq_ignore_ascii_case(host_suffix)
                            || host
                                .to_ascii_lowercase()
                                .ends_with(&format!(".{host_suffix}")))
                        && parsed.path().starts_with(path_prefix)
                })
            })
            .unwrap_or(false),
        AllowedBaseUrlPattern::FilePathPrefix(prefix) => url_to_local_path(url)
            .and_then(|path| canonicalize_path_for_policy_check(&path))
            .map(|path| path.starts_with(prefix))
            .unwrap_or(false),
    })
}

pub fn check_url_allowed(
    url: &str,
    allowed_base_urls: &Option<Vec<AllowedBaseUrlPattern>>,
) -> Result<()> {
    if allowed_base_urls
        .as_ref()
        .map(|patterns| is_url_allowed(url, patterns))
        .unwrap_or(true)
    {
        Ok(())
    } else {
        Err(VegaFusionError::specification(format!(
            "URL or path '{url}' blocked by allowed_base_urls. Add the URL prefix to allowed_base_urls or change base_url."
        )))
    }
}

/// Resolve a spec URL against a base URL. This is the shared function used by
/// both plan-time resolution (MakeTasksVisitor for Url::String) and eval-time
/// resolution (DataUrlTask::eval for Url::Expr).
pub fn resolve_url(url: &str, base_url: &Option<String>) -> Result<String> {
    if url.starts_with("//") {
        // Protocol-relative URL — prepend https: so downstream parsers work
        Ok(format!("https:{url}"))
    } else if has_url_scheme(url) {
        Ok(url.to_string())
    } else if is_absolute_path(url) {
        path_to_file_url(url)
    } else {
        // Relative path — resolve against base URL using RFC 3986 joining
        match base_url {
            Some(base) => {
                let base_url = url::Url::parse(base).map_err(|e| {
                    VegaFusionError::specification(format!("Invalid base URL '{base}': {e}"))
                })?;
                let resolved = base_url.join(url).map_err(|e| {
                    VegaFusionError::specification(format!(
                        "Cannot resolve '{url}' against base '{base}': {e}"
                    ))
                })?;
                Ok(resolved.to_string())
            }
            None => Err(VegaFusionError::specification(format!(
                "Relative URL with no base_url configured: {url}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_normalize_base_url_existing_directory_adds_trailing_slash() {
        let tempdir = tempfile::tempdir().unwrap();
        let result = normalize_base_url(tempdir.path().to_str().unwrap().to_string()).unwrap();
        assert!(
            result.ends_with('/'),
            "expected trailing slash, got {result}"
        );
    }

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

    #[test]
    fn test_normalize_allowed_base_url_star() {
        assert_eq!(
            normalize_allowed_base_url("*").unwrap(),
            AllowedBaseUrlPattern::Any
        );
    }

    #[test]
    fn test_normalize_allowed_base_url_generic_scheme() {
        assert_eq!(
            normalize_allowed_base_url("s3:").unwrap(),
            AllowedBaseUrlPattern::Scheme("s3".to_string())
        );
    }

    #[test]
    fn test_normalize_allowed_base_url_prefix() {
        assert_eq!(
            normalize_allowed_base_url("https://example.com/data").unwrap(),
            AllowedBaseUrlPattern::Prefix("https://example.com/data/".to_string())
        );
    }

    #[test]
    fn test_normalize_allowed_base_url_wildcard_host() {
        assert_eq!(
            normalize_allowed_base_url("https://*.example.com/data").unwrap(),
            AllowedBaseUrlPattern::WildcardHost {
                scheme: "https".to_string(),
                host_suffix: "example.com".to_string(),
                path_prefix: "/data/".to_string(),
            }
        );
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_normalize_allowed_base_url_filesystem_root() {
        let tempdir = tempfile::tempdir().unwrap();
        let normalized = normalize_allowed_base_url(tempdir.path().to_str().unwrap()).unwrap();
        assert_eq!(
            normalized,
            AllowedBaseUrlPattern::FilePathPrefix(fs::canonicalize(tempdir.path()).unwrap())
        );
    }

    #[test]
    fn test_normalize_allowed_base_url_rejects_query() {
        assert!(normalize_allowed_base_url("https://example.com/data?q=1").is_err());
    }

    #[test]
    fn test_is_url_allowed_generic_scheme() {
        let patterns = vec![normalize_allowed_base_url("myproto:").unwrap()];
        assert!(is_url_allowed("myproto://warehouse/sales", &patterns));
        assert!(!is_url_allowed("otherproto://warehouse/sales", &patterns));
    }

    #[test]
    fn test_is_url_allowed_prefix() {
        let patterns = vec![normalize_allowed_base_url("https://example.com/data/").unwrap()];
        assert!(is_url_allowed(
            "https://example.com/data/cars.json",
            &patterns
        ));
        assert!(!is_url_allowed(
            "https://example.com/other/cars.json",
            &patterns
        ));
    }

    #[test]
    fn test_is_url_allowed_wildcard_host() {
        let patterns = vec![normalize_allowed_base_url("https://*.example.com/data/").unwrap()];
        assert!(is_url_allowed(
            "https://example.com/data/cars.json",
            &patterns
        ));
        assert!(is_url_allowed(
            "https://cdn.example.com/data/cars.json",
            &patterns
        ));
        assert!(!is_url_allowed(
            "https://example.com.evil.com/data/cars.json",
            &patterns
        ));
        assert!(!is_url_allowed(
            "https://cdn.example.com/other/cars.json",
            &patterns
        ));
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_is_url_allowed_filesystem_canonicalization() {
        let root = tempfile::tempdir().unwrap();
        let nested = root.path().join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        let file_path = nested.join("data.json");
        std::fs::write(&file_path, "{}").unwrap();

        let patterns = vec![normalize_allowed_base_url(root.path().to_str().unwrap()).unwrap()];
        assert!(is_url_allowed(
            &format!("file://{}", file_path.display()),
            &patterns
        ));
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_is_url_allowed_rejects_parent_traversal() {
        let root = tempfile::tempdir().unwrap();
        let allowed = root.path().join("allowed");
        std::fs::create_dir_all(&allowed).unwrap();
        let outside = root.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        let file_path = allowed.join("../outside/data.json");

        let patterns = vec![normalize_allowed_base_url(allowed.to_str().unwrap()).unwrap()];
        assert!(!is_url_allowed(
            &format!("file://{}", file_path.display()),
            &patterns
        ));
    }

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_file_url_to_path_roundtrip() {
        let path = "/tmp/my data/file.csv";
        let url = path_to_file_url(path).unwrap();
        let roundtrip = file_url_to_path(&url).unwrap();
        assert_eq!(roundtrip, PathBuf::from(path));
    }
}
