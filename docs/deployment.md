# Tailnet-only deployment

The production topology keeps EpochDeck on loopback and lets Tailscale terminate
HTTPS. There is no public listener, reverse-proxy database, or external object
store.

```text
Tailnet client -> https://epochdeck.<tailnet>.ts.net
                         |
                  Tailscale Serve
                         |
                 http://127.0.0.1:8787
                         |
             one epochdeck-server binary
```

## Build and install

### Install a GitHub prerelease

Download the server archive for the host architecture and `SHA256SUMS` from one
GitHub prerelease. Select the archive target with `uname -m`: `x86_64` uses
`x86_64-unknown-linux-musl`, while `aarch64` or `arm64` uses
`aarch64-unknown-linux-musl`. Reject any other architecture rather than
guessing.

```bash
sha256sum --ignore-missing --check --strict SHA256SUMS
tar -xzf epochdeck-server-<version>-<target>.tar.gz
sudo install -o root -g root -m 0755 \
  epochdeck-server-<version>-<target>/epochdeck-server \
  /usr/local/bin/epochdeck-server
```

That executable is the complete hosted service, including the dashboard. If
you also want the `epochdeck doctor`, backup, and restore administration
commands on the server, download the wheel from the same release and install it
as an isolated optional tool:

```bash
sudo env UV_TOOL_DIR=/opt/epochdeck-cli UV_TOOL_BIN_DIR=/usr/local/bin \
  uv tool install --force ./epochdeck-*.whl
```

When installed, the server and wheel must come from the same release. The wheel
is attached to GitHub and is not installed from PyPI. The checksum command
verifies every downloaded asset named by `SHA256SUMS`, fails if none are
present, and safely ignores the other architecture, wheel, and source
distribution when they were not downloaded. Continue with the system account,
storage, and unit setup below.

### Build from source

The release archive above is the recommended hosting package. It contains one
self-contained server executable with the dashboard embedded; the host does
not need Nix, Cargo, Node.js, pnpm, uv, or `just` at runtime.

If you deliberately build from a source checkout, install the locked
dependencies, build the dashboard, and embed it into the Rust executable with
the underlying tools directly:

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

Mount or bind the large pool at `/srv/epochdeck` first, then create the dedicated
account and storage locations. Keep catalog and metrics on SSD, and place only
the CAS blob root on that mounted pool:

```bash
sudo useradd --system --home /var/lib/epochdeck --shell /usr/sbin/nologin epochdeck
sudo install -d -o root -g epochdeck -m 0750 /etc/epochdeck
sudo install -d -o epochdeck -g epochdeck -m 0750 /srv/epochdeck/blobs
sudo install -o root -g root -m 0644 deploy/epochdeck.service \
  /etc/systemd/system/epochdeck.service
sudo install -o root -g epochdeck -m 0640 deploy/epochdeck.env.example \
  /etc/epochdeck/epochdeck.env
sudo systemctl daemon-reload
sudo systemctl enable --now epochdeck
```

`/srv/epochdeck` must be a real bind mount or ZFS dataset before the unit starts;
the unit refuses an ordinary directory so a missing pool cannot silently place
large blobs on the container root disk. If the blob mount is elsewhere, update
`RequiresMountsFor`, `ConditionPathIsMountPoint`, and `ReadWritePaths` together.

## Tailnet-only HTTPS

EpochDeck deliberately binds `127.0.0.1:8787`. Configure Tailscale Serve once as
root; its background configuration survives the invoking shell:

```bash
sudo tailscale serve --bg --https=443 http://127.0.0.1:8787
tailscale serve status
```

Tailscale Serve provisions and terminates HTTPS and exposes Serve only inside
the tailnet. Do not use `tailscale funnel`, which is the public-internet
counterpart. The current command form is documented in the
[official Tailscale Serve CLI reference](https://tailscale.com/docs/reference/tailscale-cli/serve).

Restrict the node and port further with tailnet grants/ACLs. EpochDeck does not yet
provide application-level multi-user authorization, so Tailnet policy is the
security boundary for this deployment.

## Verification

```bash
curl --fail http://127.0.0.1:8787/api/v1/health
curl --fail https://epochdeck.<tailnet>.ts.net/api/v1/health
epochdeck doctor --server-url https://epochdeck.<tailnet>.ts.net
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
