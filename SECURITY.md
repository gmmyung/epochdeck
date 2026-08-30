# Security policy

Only the latest GitHub prerelease is supported. EpochDeck is pre-alpha and is not
approved for public or multi-tenant exposure.

Report vulnerabilities privately through this repository's GitHub Security
Advisories. Do not open a public issue containing exploit details, credentials,
private run data, or Tailnet information.

EpochDeck currently has no application-level authentication or multi-user
authorization. A supported deployment binds to loopback and uses Tailscale Serve
with restrictive Tailnet grants or ACLs. Do not use Tailscale Funnel or expose
the EpochDeck port on a public interface.
