# Security policy

Only the latest GitHub prerelease is supported. EpochDeck is pre-alpha and its
server is not approved for direct public exposure or multi-tenant isolation.

Report vulnerabilities privately through this repository's GitHub Security
Advisories. Do not open a public issue containing exploit details, credentials,
private run data, or deployment information.

EpochDeck has no native application authentication or multi-user authorization.
Bind the EpochDeck HTTP server to loopback and place an authenticated HTTPS
reverse proxy in front of it. The proxy must terminate TLS and enforce access
control before forwarding requests to the loopback listener. Never expose the
EpochDeck HTTP port directly on an externally reachable interface.

Verify downloaded prerelease assets against `SHA256SUMS` from the same GitHub
release. macOS and Windows prerelease binaries are not yet notarized or
platform-signed, so those operating systems may display an unknown-publisher
warning even when the checksum is correct.
