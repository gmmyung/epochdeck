use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use epochdeck_protocol::DashboardConfigResponse;
use thiserror::Error;

use crate::AppState;

const DEFAULT_ACCENT_COLOR: &str = "#2766ad";
const DASHBOARD_LOGO_URL: &str = "/api/v1/dashboard/logo";
const MAX_DASHBOARD_LOGO_BYTES: u64 = 1024 * 1024;
const SVG_NAMESPACE: &str = "http://www.w3.org/2000/svg";
const SVG_CONTENT_SECURITY_POLICY: &str = "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; style-src 'unsafe-inline'; sandbox";

#[derive(Debug, Clone)]
pub struct DashboardConfig {
    response: DashboardConfigResponse,
    logo: Option<DashboardLogo>,
}

impl DashboardConfig {
    pub fn from_environment() -> Result<Self, DashboardConfigError> {
        let accent_color = match std::env::var("EPOCHDECK_DASHBOARD_ACCENT_COLOR") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => DEFAULT_ACCENT_COLOR.to_owned(),
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(DashboardConfigError::NonUnicodeEnvironment {
                    name: "EPOCHDECK_DASHBOARD_ACCENT_COLOR",
                });
            }
        };
        let logo_path = std::env::var_os("EPOCHDECK_DASHBOARD_LOGO_PATH");
        if logo_path.as_deref().is_some_and(OsStr::is_empty) {
            return Err(DashboardConfigError::EmptyLogoPath);
        }
        Self::new(&accent_color, logo_path.as_deref().map(Path::new))
    }

    pub fn new(accent_color: &str, logo_path: Option<&Path>) -> Result<Self, DashboardConfigError> {
        validate_accent_color(accent_color)?;
        let logo = logo_path.map(load_logo).transpose()?;
        Ok(Self {
            response: DashboardConfigResponse {
                accent_color: accent_color.to_owned(),
                logo_url: logo.as_ref().map(|_| DASHBOARD_LOGO_URL.to_owned()),
            },
            logo,
        })
    }
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            response: DashboardConfigResponse {
                accent_color: DEFAULT_ACCENT_COLOR.to_owned(),
                logo_url: None,
            },
            logo: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum DashboardConfigError {
    #[error("{name} is not valid Unicode")]
    NonUnicodeEnvironment { name: &'static str },
    #[error("EPOCHDECK_DASHBOARD_ACCENT_COLOR must be exactly #RRGGBB, got {value:?}")]
    InvalidAccentColor { value: String },
    #[error("EPOCHDECK_DASHBOARD_LOGO_PATH cannot be empty")]
    EmptyLogoPath,
    #[error("failed to open dashboard logo {path}")]
    OpenLogo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read dashboard logo {path}")]
    ReadLogo {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("dashboard logo {path} exceeds the 1 MiB limit")]
    LogoTooLarge { path: PathBuf },
    #[error("dashboard logo {path} is not a valid PNG, JPEG, WebP, or SVG image")]
    UnsupportedLogo { path: PathBuf },
    #[error("dashboard SVG logo {path} is invalid: {message}")]
    InvalidSvg { path: PathBuf, message: String },
}

#[derive(Debug, Clone)]
struct DashboardLogo {
    bytes: Bytes,
    content_type: &'static str,
}

pub(super) async fn dashboard_config(
    State(state): State<AppState>,
) -> Json<DashboardConfigResponse> {
    Json(state.dashboard.response.clone())
}

pub(super) async fn dashboard_logo(State(state): State<AppState>) -> Response {
    let Some(logo) = &state.dashboard.logo else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut response = Body::from(logo.bytes.clone()).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(logo.content_type),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    if logo.content_type == "image/svg+xml" {
        response.headers_mut().insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(SVG_CONTENT_SECURITY_POLICY),
        );
    }
    response
}

fn validate_accent_color(value: &str) -> Result<(), DashboardConfigError> {
    if value.len() != 7
        || !value.starts_with('#')
        || !value.as_bytes()[1..].iter().all(u8::is_ascii_hexdigit)
    {
        return Err(DashboardConfigError::InvalidAccentColor {
            value: value.to_owned(),
        });
    }
    Ok(())
}

fn load_logo(path: &Path) -> Result<DashboardLogo, DashboardConfigError> {
    let mut file = File::open(path).map_err(|source| DashboardConfigError::OpenLogo {
        path: path.to_path_buf(),
        source,
    })?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_DASHBOARD_LOGO_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| DashboardConfigError::ReadLogo {
            path: path.to_path_buf(),
            source,
        })?;
    if bytes.len() as u64 > MAX_DASHBOARD_LOGO_BYTES {
        return Err(DashboardConfigError::LogoTooLarge {
            path: path.to_path_buf(),
        });
    }
    let content_type = detect_logo_type(path, &bytes)?;
    Ok(DashboardLogo {
        bytes: Bytes::from(bytes),
        content_type,
    })
}

fn detect_logo_type(path: &Path, bytes: &[u8]) -> Result<&'static str, DashboardConfigError> {
    if valid_png(bytes) {
        return Ok("image/png");
    }
    if valid_jpeg(bytes) {
        return Ok("image/jpeg");
    }
    if valid_webp(bytes) {
        return Ok("image/webp");
    }
    if let Ok(svg) = std::str::from_utf8(bytes) {
        validate_svg(path, svg)?;
        return Ok("image/svg+xml");
    }
    Err(DashboardConfigError::UnsupportedLogo {
        path: path.to_path_buf(),
    })
}

fn valid_png(bytes: &[u8]) -> bool {
    bytes.len() >= 24
        && bytes.starts_with(b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR")
        && bytes[16..20] != [0, 0, 0, 0]
        && bytes[20..24] != [0, 0, 0, 0]
}

fn valid_jpeg(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes.starts_with(&[0xff, 0xd8, 0xff]) && bytes.ends_with(&[0xff, 0xd9])
}

fn valid_webp(bytes: &[u8]) -> bool {
    if bytes.len() < 20
        || !bytes.starts_with(b"RIFF")
        || &bytes[8..12] != b"WEBP"
        || !matches!(&bytes[12..16], b"VP8 " | b"VP8L" | b"VP8X")
    {
        return false;
    }
    u32::from_le_bytes(bytes[4..8].try_into().expect("WebP length slice is fixed")) as usize + 8
        == bytes.len()
}

fn validate_svg(path: &Path, svg: &str) -> Result<(), DashboardConfigError> {
    if svg.contains("<!DOCTYPE") || svg.contains("<!ENTITY") {
        return Err(invalid_svg(
            path,
            "document types and entities are not allowed",
        ));
    }
    let document = roxmltree::Document::parse(svg)
        .map_err(|error| invalid_svg(path, &format!("malformed XML: {error}")))?;
    let root = document.root_element();
    if root.tag_name().name() != "svg" || root.tag_name().namespace() != Some(SVG_NAMESPACE) {
        return Err(invalid_svg(
            path,
            "the root must be an SVG element in the SVG namespace",
        ));
    }
    for node in document.descendants().filter(roxmltree::Node::is_element) {
        let name = node.tag_name().name();
        if ["script", "foreignObject", "iframe", "object", "embed", "a"]
            .iter()
            .any(|blocked| name.eq_ignore_ascii_case(blocked))
        {
            return Err(invalid_svg(
                path,
                &format!("element {name:?} is not allowed"),
            ));
        }
        for attribute in node.attributes() {
            let attribute_name = attribute.name();
            let normalized_value = attribute.value().trim().to_ascii_lowercase();
            if attribute_name.len() > 2 && attribute_name[..2].eq_ignore_ascii_case("on") {
                return Err(invalid_svg(
                    path,
                    &format!("event attribute {attribute_name:?} is not allowed"),
                ));
            }
            if normalized_value.contains("javascript:") {
                return Err(invalid_svg(path, "javascript URLs are not allowed"));
            }
            if attribute_name.eq_ignore_ascii_case("href")
                && !normalized_value.is_empty()
                && !normalized_value.starts_with('#')
            {
                return Err(invalid_svg(path, "external references are not allowed"));
            }
        }
    }
    Ok(())
}

fn invalid_svg(path: &Path, message: &str) -> DashboardConfigError {
    DashboardConfigError::InvalidSvg {
        path: path.to_path_buf(),
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{DashboardConfig, DashboardConfigError, MAX_DASHBOARD_LOGO_BYTES};

    #[test]
    fn defaults_are_deterministic() {
        let config = DashboardConfig::default();
        assert_eq!(config.response.accent_color, "#2766ad");
        assert_eq!(config.response.logo_url, None);
        assert!(config.logo.is_none());
    }

    #[test]
    fn accent_color_requires_exact_hex_syntax() {
        for valid in ["#000000", "#2766ad", "#A1B2C3"] {
            assert!(DashboardConfig::new(valid, None).is_ok());
        }
        for invalid in ["2766ad", "#fff", "#12345678", "red", " #2766ad", "#gggggg"] {
            assert!(matches!(
                DashboardConfig::new(invalid, None),
                Err(DashboardConfigError::InvalidAccentColor { .. })
            ));
        }
    }

    #[test]
    fn loads_supported_logo_formats_once() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let fixtures = [
            (
                "logo.png",
                b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR\0\0\0\x01\0\0\0\x01".as_slice(),
                "image/png",
            ),
            ("logo.jpg", b"\xff\xd8\xff\xe0\xff\xd9".as_slice(), "image/jpeg"),
            (
                "logo.webp",
                b"RIFF\x0c\0\0\0WEBPVP8 \0\0\0\0".as_slice(),
                "image/webp",
            ),
            (
                "logo.svg",
                br#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1 1"><path d="M0 0h1v1z"/></svg>"#.as_slice(),
                "image/svg+xml",
            ),
        ];
        for (name, bytes, content_type) in fixtures {
            let path = directory.path().join(name);
            fs::write(&path, bytes)?;
            let config = DashboardConfig::new("#123456", Some(&path))?;
            let logo = config.logo.expect("configured logo must be loaded");
            assert_eq!(logo.bytes.as_ref(), bytes);
            assert_eq!(logo.content_type, content_type);
            fs::write(&path, b"changed after startup")?;
            assert_eq!(logo.bytes.as_ref(), bytes);
        }
        Ok(())
    }

    #[test]
    fn rejects_oversized_and_unsupported_logos() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let oversized = directory.path().join("oversized.svg");
        fs::write(
            &oversized,
            vec![b' '; MAX_DASHBOARD_LOGO_BYTES as usize + 1],
        )?;
        assert!(matches!(
            DashboardConfig::new("#123456", Some(&oversized)),
            Err(DashboardConfigError::LogoTooLarge { .. })
        ));

        let unsupported = directory.path().join("logo.txt");
        fs::write(&unsupported, b"not an image")?;
        assert!(matches!(
            DashboardConfig::new("#123456", Some(&unsupported)),
            Err(DashboardConfigError::InvalidSvg { .. })
        ));
        Ok(())
    }

    #[test]
    fn svg_must_be_well_formed_static_and_self_contained() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempdir()?;
        for (index, svg) in [
            r#"<svg xmlns="http://www.w3.org/2000/svg">"#,
            r#"<svg xmlns="http://www.w3.org/2000/svg"><script>bad()</script></svg>"#,
            r#"<svg xmlns="http://www.w3.org/2000/svg" onload="bad()"/>"#,
            r#"<svg xmlns="http://www.w3.org/2000/svg"><image href="https://example.com/logo.png"/></svg>"#,
            r#"<!DOCTYPE svg><svg xmlns="http://www.w3.org/2000/svg"/>"#,
        ]
        .into_iter()
        .enumerate()
        {
            let path = directory.path().join(format!("invalid-{index}.svg"));
            fs::write(&path, svg)?;
            assert!(matches!(
                DashboardConfig::new("#123456", Some(&path)),
                Err(DashboardConfigError::InvalidSvg { .. })
            ));
        }
        Ok(())
    }
}
