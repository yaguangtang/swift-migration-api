# Swift Migration API

Swift Migration API is a Go service that sits in front of two Swift-compatible backends:

- A new Ceph RGW deployment with Swift API support
- A legacy OpenStack Swift API

It gives clients a single Swift-compatible endpoint while data is being migrated from the old Swift cluster to the new Ceph RGW cluster.

## What It Does

- Tries Ceph RGW first for all Swift-style requests
- Falls back to legacy Swift only when Ceph returns an exact `404 Not Found`
- Streams fallback reads back to the client immediately
- Enqueues background migration jobs when trusted caller-token expiry metadata is available
- Starts immediate caller-token migration work when fallback migration lacks trusted expiry metadata
- Uses the original caller token for fallback-triggered migration work
- Rejects queued caller-token jobs when the token is already stale or too close to expiry
- Preserves container metadata and ACL-related headers when recreating containers in Ceph
- Supports YAML and JSON configuration files
- Supports optional TLS for the public HTTP listener

## Architecture

```text
                     +----------------------+
                     |   Swift Clients      |
                     |  CLI / SDK / Apps    |
                     +----------+-----------+
                                |
                                v
                 +----------------------------------+
                 |      Swift Migration API         |
                 |                                  |
                 |  - Swift-compatible front end    |
                 |  - Ceph-first request routing    |
                 |  - 404 fallback to old Swift     |
                 |  - Queue + immediate sync paths  |
                 +-----------+--------------+-------+
                             |              |
                  read/write |              | fallback read
                  first      |              | on exact 404
                             v              v
                  +----------------+   +-------------------+
                  |   Ceph RGW     |   | Legacy Swift API  |
                  |  Swift API     |   | OpenStack Swift   |
                  +--------+-------+   +---------+---------+
                           ^                     |
                           |                     |
                           +----------+----------+
                                      |
                                      | queued or immediate copy
                                      | metadata + objects
                                      v
                            +----------------------+
                            | Migration Workers    |
                            | Queue + SQLite store |
                            +----------------------+
```

## Request Flow

1. A client sends a Swift-compatible request to this service.
2. The service sends the request to Ceph RGW first.
3. If Ceph returns an exact `404`, the service retries against legacy Swift.
4. If legacy Swift returns data successfully, the service serves the response and starts migration work.
5. If trusted caller-token expiry metadata is present, the service enqueues background migration work.
6. If caller auth is present but expiry metadata is missing, the service starts immediate best-effort migration with the caller token.
7. New writes go to Ceph only.

## Request Processing Policy

### Global Rule

For every supported data-plane request:

1. Send the request to Ceph RGW first.
2. Only if Ceph returns exact `404`, try legacy Swift.
3. If Swift also returns `404`, return `404`.
4. If Ceph returns `401`, `403`, `409`, `5xx`, timeout, or any other non-404 failure, do not fall back to Swift.
5. Return the Ceph result directly in those cases.

### Account Listing

For `GET /v1/{account}`:

- Query both Ceph and Swift.
- Merge listing results.
- De-duplicate by container name.
- Prefer the Ceph entry when the same container exists in both.

### Container Listing

For `GET /v1/{account}/{container}`:

- Query both backends.
- Merge and de-duplicate object names.
- Prefer Ceph objects when duplicates exist.
- When Swift contributes objects to the merged response, start migration for that container.
- Enqueue the container migration job when trusted caller-token expiry metadata exists.
- Start immediate best-effort container sync with the caller token when caller auth exists but expiry metadata is unavailable.

### Container Create and Metadata Update

For `PUT` and `POST /v1/{account}/{container}`:

- Write only to Ceph.
- If the container exists only in Swift, create it in Ceph first.
- Copy container ACLs and metadata to Ceph.

### Container Delete

For `DELETE /v1/{account}/{container}`:

- Check both backends for emptiness.
- Return `409 Conflict` if either backend still has objects.
- Delete from both when empty.
- Return `404` only if the container does not exist in either backend.

### Object Read

For `GET` and `HEAD /v1/{account}/{container}/{object...}`:

- Try Ceph first.
- If Ceph returns `404`, read from Swift.
- If Swift returns the object successfully, stream it back immediately.
- Enqueue an asynchronous object migration job when trusted caller-token expiry metadata exists.
- Start immediate best-effort object migration with the caller token when caller auth exists but expiry metadata is unavailable.

### Object Create or Replace

For `PUT /v1/{account}/{container}/{object...}`:

- Write only to Ceph.
- If the container exists only in Swift, recreate it in Ceph first with the same ACLs and metadata.

### Object Metadata Update

For `POST /v1/{account}/{container}/{object...}`:

- Apply updates only to Ceph.
- If the object exists only in Swift, copy it to Ceph first and then update metadata.

### Object Delete

For `DELETE /v1/{account}/{container}/{object...}`:

- Attempt delete on both Ceph and Swift.
- Return success if the object existed and was deleted from at least one backend.
- Return `404` only if the object does not exist in either backend.

## Project Layout

```text
cmd/swift-migration-api     service entrypoint
internal/config             config parsing and validation
internal/backend            backend HTTP client logic
internal/proxy              request routing and fallback behavior
internal/migration          background migration workflows
internal/store              SQLite-backed job queue
internal/admin              /_migration admin endpoints
internal/auth               header forwarding and worker auth
internal/logging            structured logging
```

## Configuration

An example config file is provided at [config.example.yaml](./config.example.yaml).

The main configuration areas are:

- `server`: listen address, timeouts, optional TLS cert and key
- `backends.ceph`: Ceph RGW Swift API URL
- `backends.swift`: legacy Swift API URL
- `auth.worker_keystone`: background worker credentials
- `migration`: queue file path, concurrency, retry behavior
- `admin.auth_token`: token for `/_migration/*` endpoints

### Caller Token Expiry Headers

Queued caller-token migrations require trusted token-expiry metadata on the incoming request. The service currently checks these headers:

- `X-Auth-Token-Expires-At`
- `X-Token-Expires-At`
- `X-Authorization-Expires-At`

If a fallback read or merged container listing includes user auth but no usable expiry header, the service will still serve the client request. It will skip queueing that caller-token migration job and start immediate best-effort migration work with the caller token instead. If a queued caller-token job reaches the worker after the token is stale or too close to expiry, the job is marked as rejected instead of being replayed with the wrong identity.

## Local Development

Run tests:

```bash
go test ./...
```

Run the service:

```bash
go run ./cmd/swift-migration-api -config ./config.example.yaml
```

## CI And Releases

GitHub Actions is configured to run on every pull request and will:

- Set up Go
- Run `go test ./...`
- Build the service binary from `./cmd/swift-migration-api`

When you push a tag that starts with `v`, for example `v0.1.0`, the same workflow will also:

- Build release archives for Linux and macOS on `amd64` and `arm64`
- Generate SHA-256 checksum files for each archive
- Publish those files to a GitHub Release for the tag
