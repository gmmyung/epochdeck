use std::ffi::OsStr;
use std::fmt::Write as _;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use epochdeck_protocol::DashboardConfigResponse;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::AppState;

const DEFAULT_ACCENT_COLOR: &str = "#2766ad";
const DASHBOARD_LOGO_URL: &str = "/api/v1/dashboard/logo";
const DASHBOARD_FAVICON_URL: &str = "/api/v1/dashboard/favicon";
const MAX_DASHBOARD_IMAGE_BYTES: u64 = 1024 * 1024;
const SVG_NAMESPACE: &str = "http://www.w3.org/2000/svg";
const SVG_CONTENT_SECURITY_POLICY: &str = "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; style-src 'unsafe-inline'; sandbox";

#[derive(Debug, Clone)]
pub struct DashboardConfig {
    response: DashboardConfigResponse,
    logo: Option<DashboardImage>,
    favicon: Option<DashboardImage>,
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
        let logo_path = optional_image_path("EPOCHDECK_DASHBOARD_LOGO_PATH")?;
        let favicon_path = optional_image_path("EPOCHDECK_DASHBOARD_FAVICON_PATH")?;
        Self::new(&accent_color, logo_path.as_deref(), favicon_path.as_deref())
    }

    pub fn new(
        accent_color: &str,
        logo_path: Option<&Path>,
        favicon_path: Option<&Path>,
    ) -> Result<Self, DashboardConfigError> {
        validate_accent_color(accent_color)?;
        let logo = logo_path
            .map(|path| load_image(path, DashboardImageKind::Logo))
            .transpose()?;
        let favicon = favicon_path
            .map(|path| load_image(path, DashboardImageKind::Favicon))
            .transpose()?;
        let favicon_source = favicon.as_ref().or(logo.as_ref());
        Ok(Self {
            response: DashboardConfigResponse {
                accent_color: accent_color.to_owned(),
                logo_url: logo
                    .as_ref()
                    .map(|image| versioned_image_url(DASHBOARD_LOGO_URL, image)),
                favicon_url: favicon_source
                    .map(|image| versioned_image_url(DASHBOARD_FAVICON_URL, image)),
            },
            logo,
            favicon,
        })
    }
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            response: DashboardConfigResponse {
                accent_color: DEFAULT_ACCENT_COLOR.to_owned(),
                logo_url: None,
                favicon_url: None,
            },
            logo: None,
            favicon: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum DashboardImageKind {
    Logo,
    Favicon,
}

impl DashboardImageKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Logo => "logo",
            Self::Favicon => "favicon",
        }
    }

    const fn allows_ico(self) -> bool {
        matches!(self, Self::Favicon)
    }
}

#[derive(Debug, Error)]
pub enum DashboardConfigError {
    #[error("{name} is not valid Unicode")]
    NonUnicodeEnvironment { name: &'static str },
    #[error("EPOCHDECK_DASHBOARD_ACCENT_COLOR must be exactly #RRGGBB, got {value:?}")]
    InvalidAccentColor { value: String },
    #[error("{name} cannot be empty")]
    EmptyImagePath { name: &'static str },
    #[error("failed to open dashboard {kind} {path}")]
    OpenImage {
        kind: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read dashboard {kind} {path}")]
    ReadImage {
        kind: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("dashboard {kind} {path} exceeds the 1 MiB limit")]
    ImageTooLarge { kind: &'static str, path: PathBuf },
    #[error("dashboard {kind} {path} has an unsupported image format")]
    UnsupportedImage { kind: &'static str, path: PathBuf },
    #[error("dashboard SVG {kind} {path} is invalid: {message}")]
    InvalidSvg {
        kind: &'static str,
        path: PathBuf,
        message: String,
    },
}

#[derive(Debug, Clone)]
struct DashboardImage {
    bytes: Bytes,
    content_type: &'static str,
}

pub(super) async fn dashboard_config(
    State(state): State<AppState>,
) -> Json<DashboardConfigResponse> {
    Json(state.dashboard.response)
}

pub(super) async fn dashboard_logo(State(state): State<AppState>) -> Response {
    let Some(logo) = &state.dashboard.logo else {
        return StatusCode::NOT_FOUND.into_response();
    };
    image_response(logo)
}

pub(super) async fn dashboard_favicon(State(state): State<AppState>) -> Response {
    let Some(favicon) = state
        .dashboard
        .favicon
        .as_ref()
        .or(state.dashboard.logo.as_ref())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    image_response(favicon)
}

fn image_response(image: &DashboardImage) -> Response {
    let mut response = Body::from(image.bytes.clone()).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(image.content_type),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    if image.content_type == "image/svg+xml" {
        response.headers_mut().insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static(SVG_CONTENT_SECURITY_POLICY),
        );
    }
    response
}

fn optional_image_path(name: &'static str) -> Result<Option<PathBuf>, DashboardConfigError> {
    let path = std::env::var_os(name);
    if path.as_deref().is_some_and(OsStr::is_empty) {
        return Err(DashboardConfigError::EmptyImagePath { name });
    }
    Ok(path.map(PathBuf::from))
}

fn versioned_image_url(path: &str, image: &DashboardImage) -> String {
    let digest = Sha256::digest(&image.bytes);
    let mut version = String::with_capacity(16);
    for byte in digest.iter().take(8) {
        write!(version, "{byte:02x}").expect("writing to a String cannot fail");
    }
    format!("{path}?v={version}")
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

fn load_image(
    path: &Path,
    kind: DashboardImageKind,
) -> Result<DashboardImage, DashboardConfigError> {
    let mut file = File::open(path).map_err(|source| DashboardConfigError::OpenImage {
        kind: kind.label(),
        path: path.to_path_buf(),
        source,
    })?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_DASHBOARD_IMAGE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| DashboardConfigError::ReadImage {
            kind: kind.label(),
            path: path.to_path_buf(),
            source,
        })?;
    if bytes.len() as u64 > MAX_DASHBOARD_IMAGE_BYTES {
        return Err(DashboardConfigError::ImageTooLarge {
            kind: kind.label(),
            path: path.to_path_buf(),
        });
    }
    let content_type = detect_image_type(path, &bytes, kind)?;
    Ok(DashboardImage {
        bytes: Bytes::from(bytes),
        content_type,
    })
}

fn detect_image_type(
    path: &Path,
    bytes: &[u8],
    kind: DashboardImageKind,
) -> Result<&'static str, DashboardConfigError> {
    if valid_png(bytes) {
        return Ok("image/png");
    }
    if valid_jpeg(bytes) {
        return Ok("image/jpeg");
    }
    if valid_webp(bytes) {
        return Ok("image/webp");
    }
    if kind.allows_ico() && valid_ico(bytes) {
        return Ok("image/x-icon");
    }
    if let Ok(svg) = std::str::from_utf8(bytes) {
        validate_svg(path, svg, kind)?;
        return Ok("image/svg+xml");
    }
    Err(DashboardConfigError::UnsupportedImage {
        kind: kind.label(),
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

fn valid_ico(bytes: &[u8]) -> bool {
    if bytes.len() < 22 || bytes[..4] != [0, 0, 1, 0] {
        return false;
    }
    let count = usize::from(u16::from_le_bytes([bytes[4], bytes[5]]));
    if count == 0 || count > 256 || bytes.len() < 6 + count * 16 {
        return false;
    }
    bytes[6..6 + count * 16].chunks_exact(16).all(|entry| {
        let size = u32::from_le_bytes(entry[8..12].try_into().expect("ICO size slice is fixed"));
        let offset =
            u32::from_le_bytes(entry[12..16].try_into().expect("ICO offset slice is fixed"));
        size > 0
            && offset >= u32::try_from(6 + count * 16).unwrap_or(u32::MAX)
            && usize::try_from(offset)
                .ok()
                .zip(usize::try_from(size).ok())
                .and_then(|(offset, size)| offset.checked_add(size))
                .is_some_and(|end| end <= bytes.len())
    })
}

fn validate_svg(
    path: &Path,
    svg: &str,
    kind: DashboardImageKind,
) -> Result<(), DashboardConfigError> {
    if svg.contains("<!DOCTYPE") || svg.contains("<!ENTITY") {
        return Err(invalid_svg(
            path,
            kind,
            "document types and entities are not allowed",
        ));
    }
    let document = roxmltree::Document::parse(svg)
        .map_err(|error| invalid_svg(path, kind, &format!("malformed XML: {error}")))?;
    let root = document.root_element();
    if root.tag_name().name() != "svg" || root.tag_name().namespace() != Some(SVG_NAMESPACE) {
        return Err(invalid_svg(
            path,
            kind,
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
                kind,
                &format!("element {name:?} is not allowed"),
            ));
        }
        for attribute in node.attributes() {
            let attribute_name = attribute.name();
            let normalized_value = attribute.value().trim().to_ascii_lowercase();
            if attribute_name.len() > 2 && attribute_name[..2].eq_ignore_ascii_case("on") {
                return Err(invalid_svg(
                    path,
                    kind,
                    &format!("event attribute {attribute_name:?} is not allowed"),
                ));
            }
            if normalized_value.contains("javascript:") {
                return Err(invalid_svg(path, kind, "javascript URLs are not allowed"));
            }
            if attribute_name.eq_ignore_ascii_case("href")
                && !normalized_value.is_empty()
                && !normalized_value.starts_with('#')
            {
                return Err(invalid_svg(
                    path,
                    kind,
                    "external references are not allowed",
                ));
            }
        }
    }
    Ok(())
}

fn invalid_svg(path: &Path, kind: DashboardImageKind, message: &str) -> DashboardConfigError {
    DashboardConfigError::InvalidSvg {
        kind: kind.label(),
        path: path.to_path_buf(),
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{DashboardConfig, DashboardConfigError, MAX_DASHBOARD_IMAGE_BYTES};

    #[test]
    fn defaults_are_deterministic() {
        let config = DashboardConfig::default();
        assert_eq!(config.response.accent_color, "#2766ad");
        assert_eq!(config.response.logo_url, None);
        assert_eq!(config.response.favicon_url, None);
        assert!(config.logo.is_none());
        assert!(config.favicon.is_none());
    }

    #[test]
    fn accent_color_requires_exact_hex_syntax() {
        for valid in ["#000000", "#2766ad", "#A1B2C3"] {
            assert!(DashboardConfig::new(valid, None, None).is_ok());
        }
        for invalid in ["2766ad", "#fff", "#12345678", "red", " #2766ad", "#gggggg"] {
            assert!(matches!(
                DashboardConfig::new(invalid, None, None),
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
            let config = DashboardConfig::new("#123456", Some(&path), None)?;
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
            vec![b' '; MAX_DASHBOARD_IMAGE_BYTES as usize + 1],
        )?;
        assert!(matches!(
            DashboardConfig::new("#123456", Some(&oversized), None),
            Err(DashboardConfigError::ImageTooLarge { .. })
        ));

        let unsupported = directory.path().join("logo.txt");
        fs::write(&unsupported, b"not an image")?;
        assert!(matches!(
            DashboardConfig::new("#123456", Some(&unsupported), None),
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
                DashboardConfig::new("#123456", Some(&path), None),
                Err(DashboardConfigError::InvalidSvg { .. })
            ));
        }
        Ok(())
    }

    #[test]
    fn favicon_accepts_ico_and_overrides_the_logo_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let logo_path = directory.path().join("logo.svg");
        let logo = br#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1 1"><path d="M0 0h1v1z"/></svg>"#;
        fs::write(&logo_path, logo)?;

        let favicon_path = directory.path().join("favicon.ico");
        let mut favicon = vec![0, 0, 1, 0, 1, 0];
        favicon.extend_from_slice(&[1, 1, 0, 0, 1, 0, 32, 0, 4, 0, 0, 0, 22, 0, 0, 0]);
        favicon.extend_from_slice(&[0, 0, 0, 0]);
        fs::write(&favicon_path, &favicon)?;

        let config = DashboardConfig::new("#123456", Some(&logo_path), Some(&favicon_path))?;
        assert_eq!(config.logo.expect("logo").content_type, "image/svg+xml");
        let loaded_favicon = config.favicon.expect("favicon");
        assert_eq!(loaded_favicon.content_type, "image/x-icon");
        assert_eq!(loaded_favicon.bytes.as_ref(), favicon);
        assert!(
            config
                .response
                .logo_url
                .as_deref()
                .is_some_and(|url| url.starts_with("/api/v1/dashboard/logo?v="))
        );
        assert!(
            config
                .response
                .favicon_url
                .as_deref()
                .is_some_and(|url| url.starts_with("/api/v1/dashboard/favicon?v="))
        );
        Ok(())
    }

    #[test]
    fn logo_is_reused_as_favicon_when_no_favicon_is_configured()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let logo_path = directory.path().join("logo.svg");
        fs::write(
            &logo_path,
            br#"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1 1"/>"#,
        )?;

        let config = DashboardConfig::new("#123456", Some(&logo_path), None)?;
        assert!(config.favicon.is_none());
        assert!(config.response.favicon_url.is_some());
        Ok(())
    }
}
