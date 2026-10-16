# Orbit Workers: security guidance

The worker executes application handlers for messages received from a queue adapter. Treat the
broker and every producer as trust boundaries unless the deployment's identity and authorization
controls establish otherwise. Validate message schemas, tenant ownership, authorization, and
business invariants inside the handler before performing side effects.

## Message data and logs

`WorkMessage` bounds payload and header sizes and omits payload and headers from its repr. Header
values reject ASCII control characters; queue and message identifiers use a restricted ASCII
identifier syntax. These checks are input bounds, not content validation or proof that a message is
authentic. Verify signatures or broker identity where the application threat model requires them.

The built-in failure report omits handler exception text and payload, and constrains the reported
exception type to a safe identifier. Queue and message IDs are included in the default warning log;
use opaque non-sensitive IDs and configure retention/access controls for logs. A custom failure
observer receives those IDs and should avoid logging message data or secrets. Handler code and
provider SDKs can still emit their own sensitive diagnostics; this package cannot redact logs they
produce.

## Broker access and transport

Queue credentials, network clients, TLS settings, and access policies belong to the adapter and
deployment. Use a secret manager, least-privilege publish/consume permissions, TLS certificate
verification, and separate identities for environments. Restrict who may publish privileged work
and prevent untrusted producers from selecting arbitrary queue names or routing destinations.
Rotate credentials without exposing them in logs or process arguments.

## Retry and side-effect safety

Messages may be delivered more than once after a process crash, lease expiry, lost acknowledgement,
or retry. Handlers that issue payments, send mail, provision resources, or perform other external
side effects need an application-level idempotency key or durable deduplication record. Do not rely
on the in-process runner to make a broker operation transactional with handler work. Bound retry
attempts and ensure poison-message handling does not create an unbounded hot loop.

Cancellation leaves a delivery unsettled, so the broker may redeliver it after its lease timeout.
Avoid exposing sensitive payloads through exception messages or tracing attributes. The failure
callback receives a message ID and queue name; treat those identifiers as potentially sensitive if
they encode customer or business data.

## Scope and vulnerability reporting

This package does not provide encryption at rest, broker authorization, payload encryption,
dead-letter policy, or an audit trail. Configure those controls in the selected provider and host
application. Follow the repository [security policy](../../SECURITY.md) for vulnerability
disclosure and avoid publishing exploitable broker or credential details in public issues.
