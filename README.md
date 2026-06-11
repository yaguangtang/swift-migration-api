# Swift Migration API

Swift Migration API is a Go service that sits in front of two Swift-compatible backends:

- A new Ceph RGW deployment with Swift API support
- A legacy OpenStack Swift API

It gives clients a single Swift-compatible endpoint while data is being migrated from the old Swift cluster to the new Ceph RGW cluster.

## What It Does

- Tries Ceph RGW first for all Swift-style requests
- Falls back to legacy Swift only when Ceph returns an exact `404 Not Found`
- Streams fallback reads back to the client immediately
- Enqueues background migration jobs so objects can be copied into Ceph after a fallback read
- Uses the original caller token for lazy fallback migration when token expiry metadata is available
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
                 |  - Background migration queue    |
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
                                      | background copy
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
4. If legacy Swift returns data successfully, the service serves the response and schedules migration work.
5. New writes go to Ceph only.

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

If a fallback read includes user auth but no usable expiry header, the service will serve the read but skip enqueuing that caller-token migration job. If a queued caller-token job reaches the worker after the token is stale or too close to expiry, the job is marked as rejected instead of being replayed with the wrong identity.

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
