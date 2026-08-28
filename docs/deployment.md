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
nix develop --command just single-binary
sudo install -o root -g root -m 0755 \
  target/release/runloom-server /usr/local/bin/runloom-server
```

No `web/dist` directory is needed at runtime. Root and client-side routes are
served from the executable; missing `/api/v1/...` routes remain API 404s.

Create a dedicated account and storage locations. Keep catalog and metrics on
SSD, and place only the CAS blob root on the large pool:

```bash
sudo useradd --system --home /var/lib/runloom --shell /usr/sbin/nologin runloom
sudo install -d -o runloom -g runloom -m 0750 /etc/runloom /srv/runloom/blobs
sudo install -o root -g root -m 0644 deploy/runloom.service \
  /etc/systemd/system/runloom.service
sudo install -o root -g runloom -m 0640 deploy/runloom.env.example \
  /etc/runloom/runloom.env
sudo systemctl daemon-reload
sudo systemctl enable --now runloom
```

If `/srv/runloom` is a bind mount or ZFS dataset, mount it before starting the
unit. Adjust `ReadWritePaths` in the unit if the blob root is elsewhere.

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
