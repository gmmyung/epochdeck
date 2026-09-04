# Self-hosting

EpochDeck ships as one server binary with the dashboard embedded. It binds to
`127.0.0.1:8787` by default.

For remote access, follow the [security policy](../SECURITY.md) and place an
authenticated HTTPS reverse proxy in front of the loopback listener.

## Install a prerelease

Download `SHA256SUMS` and the server archive for your host from the same
[GitHub release](https://github.com/gmmyung/epochdeck/releases).

| Host                | Archive suffix                      |
| ------------------- | ----------------------------------- |
| Linux x86_64        | `x86_64-unknown-linux-musl.tar.gz`  |
| Linux ARM64         | `aarch64-unknown-linux-musl.tar.gz` |
| Apple Silicon macOS | `aarch64-apple-darwin.zip`          |
| Windows x86_64      | `x86_64-pc-windows-msvc.zip`        |

Linux archives are static musl builds. macOS and Windows use native system
interfaces.

### Linux

```bash
sha256sum --ignore-missing --check --strict SHA256SUMS
tar -xzf epochdeck-server-<version>-<target>.tar.gz
sudo install -o root -g root -m 0755 \
  epochdeck-server-<version>-<target>/epochdeck-server \
  /usr/local/bin/epochdeck-server
```

### macOS

```bash
archive=epochdeck-server-<version>-aarch64-apple-darwin.zip
expected="$(awk -v name="$archive" '$2 == name { print $1 }' SHA256SUMS)"
test -n "$expected"
test "$(shasum -a 256 "$archive" | awk '{ print $1 }')" = "$expected"
ditto -x -k "$archive" .
sudo install -m 0755 \
  "${archive%.zip}/epochdeck-server" /usr/local/bin/epochdeck-server
```

### Windows

```powershell
$Archive = "epochdeck-server-<version>-x86_64-pc-windows-msvc.zip"
$Line = Get-Content SHA256SUMS | Where-Object { $_ -match "  $([regex]::Escape($Archive))$" }
if (@($Line).Count -ne 1) { throw "archive is missing from SHA256SUMS" }
$Expected = ($Line -split '\s+')[0].ToLowerInvariant()
$Actual = (Get-FileHash -Algorithm SHA256 $Archive).Hash.ToLowerInvariant()
if ($Actual -ne $Expected) { throw "archive checksum mismatch" }
Expand-Archive -LiteralPath $Archive -DestinationPath .
```

The release also contains a matching Python wheel. Add it to training projects
with `uv add ./epochdeck-*.whl`. Install it with `uv tool install` only when you
want the administration CLI in an isolated environment.

## Run locally

```bash
epochdeck-server
curl --fail http://127.0.0.1:8787/api/v1/health
```

Open [http://127.0.0.1:8787](http://127.0.0.1:8787) for the dashboard.

## Run as a Linux service

Create a service account and install the included templates:

```bash
cd epochdeck-server-<version>-<target>
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

The unit permits writes below `/var/lib/epochdeck`. If a storage root is
elsewhere, add its exact path to `ReadWritePaths` and `RequiresMountsFor` in a
systemd override.

## Add HTTPS and authentication

The server has no native authentication yet. The reverse proxy must protect the
dashboard, API, and blob routes together.

For a small installation, Caddy can provide TLS and HTTP Basic authentication:

```bash
caddy hash-password
```

```caddyfile
epochdeck.example.com {
    basic_auth {
        epochdeck <generated-password-hash>
    }
    reverse_proxy 127.0.0.1:8787
}
```

Configure clients with the same endpoint and credentials as described in the
[Python SDK guide](../python/README.md#configure-a-server).

## Configure storage

Set storage roots in `/etc/epochdeck/epochdeck.env` or the process environment:

```text
EPOCHDECK_DATA_DIR=/var/lib/epochdeck/data
EPOCHDECK_METRICS_DIR=/var/lib/epochdeck/metrics
EPOCHDECK_BLOBS_DIR=/var/lib/epochdeck/blobs
```

The roots may use different mounted filesystems. Metric and blob roots must
support same-filesystem hard links and must not overlap.

## Customize the dashboard

```text
EPOCHDECK_DASHBOARD_ACCENT_COLOR="#8a3ffc"
EPOCHDECK_DASHBOARD_LOGO_PATH=/etc/epochdeck/logo.svg
EPOCHDECK_DASHBOARD_FAVICON_PATH=/etc/epochdeck/favicon.ico
```

The color must be `#RRGGBB`. Logos may be PNG, JPEG, WebP, or self-contained
SVG files up to 1 MiB. Favicons accept those formats plus ICO. When no favicon
is configured, EpochDeck reuses the custom logo or its bundled browser icons.
Restart the server after changing a value.

## Next steps

- [Backup, restore, upgrades, and diagnostics](operations.md)
- [Build from source](../CONTRIBUTING.md)
- [HTTP API](api.md)
