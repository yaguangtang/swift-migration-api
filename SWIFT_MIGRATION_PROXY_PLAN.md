# Swift Migration Proxy Implementation Plan

## Summary
Build a new Go service that exposes a Swift-compatible front-end API in front of two backends:

- A new Ceph RGW deployment with Swift API support
- The legacy OpenStack Swift API

The service should always try Ceph RGW first. If Ceph returns an exact `404 Not Found`, the service should fall back to the legacy Swift backend. If Swift also returns `404`, the service returns `404` to the client.

This service is both:

- A live request proxy for clients
- A migration coordinator that gradually moves data from legacy Swift to Ceph RGW while preserving container ownership, ACLs, metadata, and object data

The initial design assumes:

- Same Keystone identity source for both Ceph RGW and Swift
- Shared users and tenants/projects
- Single active service instance for v1
- Swift-compatible client behavior preserved at the URL level

## Public API

### Swift-Compatible Data Plane
The service keeps Swift-style URLs so existing clients only change the endpoint hostname.

Supported URLs:

- `GET /v1/{account}`
- `HEAD /v1/{account}`
- `GET /v1/{account}/{container}`
- `HEAD /v1/{account}/{container}`
- `PUT /v1/{account}/{container}`
- `POST /v1/{account}/{container}`
- `DELETE /v1/{account}/{container}`
- `GET /v1/{account}/{container}/{object...}`
- `HEAD /v1/{account}/{container}/{object...}`
- `PUT /v1/{account}/{container}/{object...}`
- `POST /v1/{account}/{container}/{object...}`
- `DELETE /v1/{account}/{container}/{object...}`

### Migration and Admin Endpoints
Add internal-only management endpoints under `/_migration/*`.

- `GET /_migration/healthz`
- `GET /_migration/readyz`
- `GET /_migration/stats`
- `GET /_migration/jobs`
- `GET /_migration/jobs/{id}`
- `POST /_migration/jobs/object/{account}/{container}/{object...}`
- `POST /_migration/jobs/container/{account}/{container}`
- `POST /_migration/jobs/account/{account}`

## Configuration
Support both YAML and JSON with the same schema.

Example YAML:

```yaml
server:
  listen: ":8443"
  read_timeout: 60s
  write_timeout: 0s
  tls:
    enabled: true
    cert_file: /etc/swift-migration/tls/tls.crt
    key_file: /etc/swift-migration/tls/tls.key

backends:
  ceph:
    base_url: "https://rgw.example.com/swift/v1"
    timeout: 120s
  swift:
    base_url: "https://swift.example.com/v1"
    timeout: 120s

auth:
  forward_headers:
    - X-Auth-Token
    - Authorization
  worker_keystone:
    auth_url: "https://keystone.example.com/v3"
    application_credential_id: "..."
    application_credential_secret: "..."
    region: "RegionOne"
    interface: "public"

migration:
  mode: "lazy_queue"
  queue_store: "/var/lib/swift-migration-api/jobs.sqlite"
  worker_concurrency: 4
  copy_on_head_miss: true
  scan_on_container_list: true
  verify_etag: true
  delete_source_after_copy: false

admin:
  auth_token: "..."
```

## Request Processing Policy

### Global Rule
For every supported data-plane request:

1. Send request to Ceph RGW first
2. Only if Ceph returns exact `404`, try legacy Swift
3. If Swift also returns `404`, return `404`
4. If Ceph returns `401`, `403`, `409`, `5xx`, timeout, or other non-404 failure, do not fall back to Swift
5. Return the Ceph error directly in those cases

### Account Listing
For `GET /v1/{account}`:

- Query both Ceph and Swift
- Merge listing results
- De-duplicate by container name
- Prefer Ceph entry when the same container exists in both

### Container Listing
For `GET /v1/{account}/{container}`:

- Query both backends
- Merge and de-duplicate object names
- Prefer Ceph objects when duplicates exist
- When Swift contributes objects to the merged response, trigger migration for that container
- Enqueue the container migration job when trusted caller-token expiry metadata exists
- Start immediate best-effort container sync with the caller token when caller auth exists but expiry metadata is unavailable

### Container Create and Metadata Update
For `PUT` and `POST /v1/{account}/{container}`:

- Write only to Ceph
- If the container exists only in Swift, create it in Ceph first
- Copy container ACLs and metadata to Ceph

### Container Delete
For `DELETE /v1/{account}/{container}`:

- Check both backends for emptiness
- Return `409 Conflict` if either backend still has objects
- Delete from both when empty
- Return `404` only if the container does not exist in either backend

### Object Read
For `GET` and `HEAD /v1/{account}/{container}/{object...}`:

- Try Ceph first
- If Ceph returns `404`, read from Swift
- If Swift returns the object successfully, stream it back immediately
- Enqueue an asynchronous object migration job after successful fallback read when trusted caller-token expiry metadata exists
- Start immediate best-effort object migration with the caller token when caller auth exists but expiry metadata is unavailable

### Object Create or Replace
For `PUT /v1/{account}/{container}/{object...}`:

- Write only to Ceph
- If the container exists only in Swift, recreate it in Ceph first with the same ACLs and metadata

### Object Metadata Update
For `POST /v1/{account}/{container}/{object...}`:

- Apply updates only to Ceph
- If the object exists only in Swift, copy it to Ceph first and then update metadata

### Object Delete
For `DELETE /v1/{account}/{container}/{object...}`:

- Attempt delete on both Ceph and Swift
- Return success if the object existed and was deleted from at least one backend
- Return `404` only if the object does not exist in either backend

## Migration Policy

### Migration Strategy
Use `new-write + lazy read-through migration + hybrid queue/immediate sync`.

Behavior:

- All new writes go to Ceph only
- Reads can be served from Swift only when Ceph returns exact `404`
- When a read falls back to Swift successfully, migrate into Ceph through the queue when trusted caller-token expiry metadata exists
- When a fallback-triggered migration has caller auth but no trusted expiry metadata, start immediate best-effort migration with the caller token
- Background jobs can also migrate containers or whole accounts proactively

### What Must Be Preserved
For migrated containers:

- Container name
- Account path
- Container ACLs
- `X-Container-Read`
- `X-Container-Write`
- `X-Container-Meta-*`

For migrated objects:

- Object path
- Object body
- `Content-Type`
- `Content-Encoding`
- `Content-Disposition`
- `Cache-Control`
- `ETag`
- `X-Object-Meta-*`

### Migration Workers
Background workers should:

- Use a dedicated Keystone service account
- Read from legacy Swift
- Write to Ceph RGW
- Verify migrated object integrity with `ETag` where possible
- Retry failed jobs with backoff
- Persist job state in SQLite for v1

Immediate request-triggered migration should:

- Use the original caller token instead of worker credentials
- Run only when the service cannot safely queue caller-token work because trusted expiry metadata is unavailable
- Keep the client response path fast by doing migration work after the fallback response is ready

## Implementation Layout

- `cmd/swift-migration-api`
- `internal/config`
- `internal/backend`
- `internal/proxy`
- `internal/migration`
- `internal/store`
- `internal/admin`
- `internal/auth`
- `internal/logging`

## Testing Plan

- Unit tests for config parsing, path parsing, listing merge logic, and SQLite queue behavior
- Integration-style tests for Ceph-first object reads, Swift fallback, and merged account listings
- Real-environment validation later for Keystone auth, ACL preservation, object metadata preservation, and large-object behavior
