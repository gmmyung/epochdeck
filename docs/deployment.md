# Tailnet-only deployment

The production topology keeps Runloom on loopback and lets Tailscale terminate
HTTPS. There is no public listener, reverse-proxy database, or external object
store.

```text
Tailnet client -> https://runloom.<tailnet>.ts.net
                         |
                  Tailscale Serve
                         |
                 http://127.0.0.1:8787
                         |
             one runloom-server binary
```

## Build and install

Build the Svelte dashboard and embed it into the Rust executable:

```bash
nix develop --command just bootstrap
nix develop --command just single-binary
sudo install -o root -g root -m 0755 \
  target/release/runloom-server /usr/local/bin/runloom-server
runloom_uv="$(nix develop --command which uv)"
sudo env UV_TOOL_DIR=/opt/runloom-cli UV_TOOL_BIN_DIR=/usr/local/bin \
  "$runloom_uv" tool install --force "$PWD/python"
```

Run `just bootstrap` once after a fresh clone and again when a lockfile changes;
it installs the exact Cargo, uv, and pnpm dependency sets recorded by the
repository. Ordinary incremental production rebuilds need only
`nix develop --command just single-binary`.

No `web/dist` directory is needed at runtime. Root and client-side routes are
served from the executable; missing `/api/v1/...` routes remain API 404s.
The separately installed `runloom` administration command supplies the doctor,
backup, and restore operations used below. Install it from the same checkout as
the server binary. Runloom has one disposable pre-alpha storage definition with
no internal generation marker: when that definition changes, archive all three
storage roots together and start the replacement build with empty roots.

Mount or bind the large pool at `/srv/runloom` first, then create the dedicated
account and storage locations. Keep catalog and metrics on SSD, and place only
the CAS blob root on that mounted pool:

```bash
sudo useradd --system --home /var/lib/runloom --shell /usr/sbin/nologin runloom
sudo install -d -o root -g runloom -m 0750 /etc/runloom
sudo install -d -o runloom -g runloom -m 0750 /srv/runloom/blobs
sudo install -o root -g root -m 0644 deploy/runloom.service \
  /etc/systemd/system/runloom.service
sudo install -o root -g runloom -m 0640 deploy/runloom.env.example \
  /etc/runloom/runloom.env
sudo systemctl daemon-reload
sudo systemctl enable --now runloom
```

`/srv/runloom` must be a real bind mount or ZFS dataset before the unit starts;
the unit refuses an ordinary directory so a missing pool cannot silently place
large blobs on the container root disk. If the blob mount is elsewhere, update
`RequiresMountsFor`, `ConditionPathIsMountPoint`, and `ReadWritePaths` together.

## Tailnet-only HTTPS

Runloom deliberately binds `127.0.0.1:8787`. Configure Tailscale Serve once as
root; its background configuration survives the invoking shell:

```bash
sudo tailscale serve --bg --https=443 http://127.0.0.1:8787
tailscale serve status
```

Tailscale Serve provisions and terminates HTTPS and exposes Serve only inside
the tailnet. Do not use `tailscale funnel`, which is the public-internet
counterpart. The current command form is documented in the
[official Tailscale Serve CLI reference](https://tailscale.com/docs/reference/tailscale-cli/serve).

Restrict the node and port further with tailnet grants/ACLs. Runloom does not yet
provide application-level multi-user authorization, so Tailnet policy is the
security boundary for this deployment.

## Verification

```bash
curl --fail http://127.0.0.1:8787/api/v1/health
curl --fail https://runloom.<tailnet>.ts.net/api/v1/health
runloom doctor --server-url https://runloom.<tailnet>.ts.net
journalctl -u runloom --since today
```

`RUNLOOM_SLOW_REQUEST_MS` controls the 1–60,000 ms slow-request threshold. Slow
requests are logged and the most recent 64 appear in `runloom doctor`.
