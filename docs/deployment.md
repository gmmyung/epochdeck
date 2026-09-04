# Self-hosting

Run EpochDeck as one loopback-bound service behind an ordinary HTTPS reverse
proxy. The proxy owns public TLS and access control; EpochDeck owns the API,
dashboard, catalog, metrics, and blobs. No VPN, hosted control plane, external
database, or object store is required. See [ADR 0016](adr/0016-standard-reverse-proxy-hosting.md)
for the security-boundary decision.

```text
Browser / Python SDK -> https://epochdeck.example.com
                                |
                 HTTPS reverse proxy + authentication
                                |
                       http://127.0.0.1:8787
                                |
                    one epochdeck-server binary
```

## Build and install

### Install a GitHub prerelease

Download `SHA256SUMS` and the server archive for the host from one GitHub
prerelease. Release binaries are compiled and exercised with ordinary Cargo on
the matching operating system and architecture.

| Host | Target and archive |
| --- | --- |
| Linux x86_64 | `x86_64-unknown-linux-musl.tar.gz` |
| Linux ARM64 | `aarch64-unknown-linux-musl.tar.gz` |
| Intel macOS | `x86_64-apple-darwin.zip` |
| Apple Silicon macOS | `aarch64-apple-darwin.zip` |
| Windows x86_64 | `x86_64-pc-windows-msvc.zip` |

The complete filename is
`epochdeck-server-<version>-<target>.<archive-extension>`. Linux builds are
statically linked with musl and do not require the host's glibc. macOS and
Windows builds use their native system interfaces; the Windows executable
statically links the MSVC C runtime.

On Linux, verify and install the matching archive:

```bash
sha256sum --ignore-missing --check --strict SHA256SUMS
tar -xzf epochdeck-server-<version>-<target>.tar.gz
sudo install -o root -g root -m 0755 \
  epochdeck-server-<version>-<target>/epochdeck-server \
  /usr/local/bin/epochdeck-server
```

On macOS, verify the one downloaded archive, expand it, and place the executable
on `PATH`:

```bash
archive=epochdeck-server-<version>-<target>.zip
expected="$(awk -v name="$archive" '$2 == name { print $1 }' SHA256SUMS)"
test -n "$expected"
test "$(shasum -a 256 "$archive" | awk '{ print $1 }')" = "$expected"
ditto -x -k "$archive" .
sudo install -m 0755 \
  "${archive%.zip}/epochdeck-server" /usr/local/bin/epochdeck-server
```

On Windows, use PowerShell to verify and expand the archive. Keep the extracted
directory or move `epochdeck-server.exe` to a permanent location on `PATH`.

```powershell
$Archive = "epochdeck-server-<version>-x86_64-pc-windows-msvc.zip"
$Lines = @(Get-Content SHA256SUMS | Where-Object { $_ -match "  $([regex]::Escape($Archive))$" })
if ($Lines.Count -ne 1) { throw "archive is missing from SHA256SUMS" }
$Expected = ($Lines[0] -split '\s+')[0].ToLowerInvariant()
$Actual = (Get-FileHash -Algorithm SHA256 $Archive).Hash.ToLowerInvariant()
if ($Actual -ne $Expected) { throw "archive checksum mismatch" }
Expand-Archive -LiteralPath $Archive -DestinationPath .
```

That executable is the complete hosted service, including the dashboard. If
you also want the `epochdeck doctor`, backup, and restore administration
commands on the server, download the wheel from the same release and install it
as an isolated optional tool:

```bash
uv tool install --force ./epochdeck-*.whl
```

When installed, the server and wheel must come from the same release. The wheel
is attached to GitHub and is not installed from PyPI. Linux operators can
continue with the system account, storage, and unit setup below. macOS and
Windows archives run the same complete service directly; this repository does
not yet ship launchd or Windows Service definitions.

### Build from source

The release archive above is the recommended hosting package. It contains one
complete server executable with the dashboard embedded; the host does
not need Nix, Cargo, Node.js, pnpm, uv, or `just` at runtime.

If you deliberately build from a source checkout on Linux or macOS, install the
locked dependencies, build the dashboard, and embed it into the Rust executable
with the underlying tools directly:

```bash
nix develop --command cargo fetch --locked
nix develop --command uv sync --project python --all-groups --locked
nix develop --command pnpm --dir web install --frozen-lockfile
nix develop --command pnpm --dir web build
nix develop --command cargo build --release --locked \
  --package epochdeck-server --features embedded-dashboard
sudo install -o root -g root -m 0755 \
  target/release/epochdeck-server /usr/local/bin/epochdeck-server
epochdeck_uv="$(nix develop --command which uv)"
sudo env UV_TOOL_DIR=/opt/epochdeck-cli UV_TOOL_BIN_DIR=/usr/local/bin \
  "$epochdeck_uv" tool install --force "$PWD/python"
```

For an incremental source rebuild after the locked dependencies are installed,
repeat the `pnpm ... build` and `cargo ... --features embedded-dashboard`
commands. `just` remains an optional contributor task runner and is not part of
the hosting or runtime contract.

No `web/dist` directory is needed at runtime. Root and client-side routes are
served from the executable; missing `/api/v1/...` routes remain API 404s.
The separately installed `epochdeck` administration command supplies the doctor,
backup, and restore operations used below. Install it from the same checkout as
the server binary. EpochDeck has one disposable pre-alpha storage definition with
no internal generation marker: when that definition changes, archive all three
storage roots together and start the replacement build with empty roots.

## Linux system service

Create a dedicated service account and install the unit. The example environment
keeps all three storage roots below `/var/lib/epochdeck`; change them independently
when the host has a different storage layout.

```bash
sudo useradd --system --home /var/lib/epochdeck --shell /usr/sbin/nologin epochdeck
sudo install -d -o root -g epochdeck -m 0750 /etc/epochdeck
sudo install -d -o epochdeck -g epochdeck -m 0750 /var/lib/epochdeck
sudo install -o root -g root -m 0644 deploy/epochdeck.service \
  /etc/systemd/system/epochdeck.service
sudo install -o root -g epochdeck -m 0640 deploy/epochdeck.env.example \
  /etc/epochdeck/epochdeck.env
sudo systemctl daemon-reload
sudo systemctl enable --now epochdeck
```

The hardened unit permits writes only below `/var/lib/epochdeck`. When a storage
root lives elsewhere, add that exact path to `ReadWritePaths` in a systemd
override. If it is a separate mount, also add `RequiresMountsFor` so a missing
mount cannot redirect writes into the host filesystem.

## HTTPS reverse proxy

EpochDeck deliberately binds `127.0.0.1:8787`. Put Caddy, nginx, Apache, or an
identity-aware proxy in front of it. The current pre-alpha server has no native
authentication, so the proxy must authenticate the entire site, including API
and blob routes. Do not expose the EpochDeck listener directly to the internet.

For a small single-user installation, Caddy's HTTP Basic authentication gives
the dashboard and Python SDK one consistent access boundary. Generate a password
hash without placing the plaintext password in the Caddyfile:

```bash
caddy hash-password
```

```caddyfile
epochdeck.example.com {
    basic_auth {
        epochdeck <paste-the-generated-hash>
    }
    reverse_proxy 127.0.0.1:8787
}
```

Point the domain's DNS records at the host and allow Caddy to receive ports 80
and 443. Caddy then obtains and renews the public certificate automatically; see
its [automatic HTTPS](https://caddyserver.com/docs/automatic-https) and
[`basic_auth`](https://caddyserver.com/docs/caddyfile/directives/basic_auth)
documentation. Other reverse proxies are equally valid if they enforce HTTPS
and authentication before forwarding any EpochDeck route.

Configure non-interactive Python processes with the same credentials. They are
added as an HTTP Basic `Authorization` header and are not written into the local
EpochDeck spool:

```bash
export EPOCHDECK_SERVER_URL=https://epochdeck.example.com
export EPOCHDECK_HTTP_USERNAME=epochdeck
read -rs EPOCHDECK_HTTP_PASSWORD
export EPOCHDECK_HTTP_PASSWORD
```

## Verification

```bash
curl --fail http://127.0.0.1:8787/api/v1/health
curl --fail --user "$EPOCHDECK_HTTP_USERNAME:$EPOCHDECK_HTTP_PASSWORD" \
  https://epochdeck.example.com/api/v1/health
epochdeck doctor --server-url https://epochdeck.example.com
journalctl -u epochdeck --since today
```

`EPOCHDECK_SLOW_REQUEST_MS` controls the 1–60,000 ms slow-request threshold. Slow
requests are logged and the most recent 64 appear in `epochdeck doctor`.

## Dashboard branding

Dashboard branding is immutable process configuration. Set
`EPOCHDECK_DASHBOARD_ACCENT_COLOR` to an exact `#RRGGBB` color and optionally set
`EPOCHDECK_DASHBOARD_LOGO_PATH` to a PNG, JPEG, WebP, or SVG file readable by the
`epochdeck` service account. The defaults are `#2766ad` and no image logo. For
example:

```bash
sudo install -o root -g epochdeck -m 0640 logo.svg /etc/epochdeck/logo.svg
sudoedit /etc/epochdeck/epochdeck.env
```

```text
EPOCHDECK_DASHBOARD_ACCENT_COLOR="#8a3ffc"
EPOCHDECK_DASHBOARD_LOGO_PATH=/etc/epochdeck/logo.svg
```

Restart the service after either value or the logo file changes. The server
reads the logo once during startup, caps it at 1 MiB, checks supported raster
format signatures, and refuses to start on an invalid color, missing file,
oversized file, or unrecognized logo format. Browsers remain responsible for
decoding PNG, JPEG, and WebP image data. SVG logos must be well-formed,
self-contained SVG documents: scripts,
embedded HTML, event handlers, links, document types, entities, and external
references are rejected. SVG responses also receive a sandboxed
`default-src 'none'` content security policy. The dashboard receives only a
same-origin logo URL; the configured filesystem path is never exposed.
