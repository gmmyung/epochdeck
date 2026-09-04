# ADR 0016: Standard reverse-proxy hosting

- Status: Accepted
- Date: 2026-09-03
- Supersedes: the Tailnet-specific deployment boundary in ADR 0001 and ADR 0011

## Context

EpochDeck's first deployment guide made Tailscale Serve the TLS and access-control
boundary. That is a valid private-network arrangement, but making one network
product part of the supported topology prevents ordinary hosting on a domain.
EpochDeck still has no application-level authentication or multi-user
authorization, so exposing its listener directly would make private runs and all
mutation routes public.

The dashboard and Python SDK must cross the same access boundary. Proxy secrets
must not be embedded in server URLs or persisted in the SDK's durable spool.

## Decision

The server remains bound to `127.0.0.1:8787` by default and continues to serve
the embedded dashboard and `/api/v1` from one process. A supported remote
installation places an HTTPS reverse proxy in front of that loopback listener.
The proxy must authenticate every path before forwarding it; Caddy with HTTP
Basic authentication is the documented minimal single-user example, while
equivalent reverse proxies and identity-aware access gateways are valid.

The Python SDK accepts one paired proxy-credential configuration through
`EPOCHDECK_HTTP_USERNAME` and `EPOCHDECK_HTTP_PASSWORD`, sends it as HTTP Basic
authentication, and rejects partial configuration or a URL containing user
information. Credentials remain process configuration and are never written to
the run spool.

The systemd unit and release package have no Tailscale dependency. EpochDeck does
not prescribe DNS, certificate authority, reverse-proxy implementation, or
physical storage media.

## Consequences

An operator can host EpochDeck with ordinary DNS and HTTPS infrastructure, and
the browser and Python SDK use the same origin and authentication policy. The
reverse proxy is an additional service, but it does not own dashboard assets or
EpochDeck data.

HTTP Basic authentication is suitable only over HTTPS and provides a shared
single-user boundary, not application identities, project authorization, or
multi-tenancy. Until EpochDeck implements those features, its listener must stay
on a trusted interface and the proxy remains mandatory for remote access.

## Rejected alternatives

### Expose the server directly

The current server would expose every read and mutation route without
authentication.

### Add TLS and authentication to the Rust server now

Certificate automation, identity integration, and authorization are separate
product concerns. Established reverse proxies provide the necessary pre-alpha
boundary without coupling the storage and query service to one hosting setup.

### Keep one VPN product as the supported deployment

It needlessly restricts otherwise ordinary HTTP hosting and makes the service
depend on a network topology unrelated to experiment tracking.
