# File publication, discovery and transfer

Implemented September 24, 2026. This document describes the file-sharing extension to the existing user/group and two-tracker system. ReadMe is intentionally unchanged.

## Commands

```
upload_file <group_id> <file_path>
list_files <group_id>
download_file <group_id> <file_name> <destination_directory>
show_downloads
stop_share <group_id> <file_name>
```

Quote arguments containing spaces. Upload inspects a readable regular file, publishes its metadata and activates its local share. File bytes travel only between clients. Downloads run in the background; completed entries are printed as `[C] [group_id] filename`. Ongoing entries show verified/total pieces; failed entries include their reason. Logout and leaving a group cancel that session's affected downloads and disable local shares. Stop-share disables serving without deleting the user's source or cancelling its download.

The destination must be an existing directory. Downloads use a random, exclusive `.part` file in a pinned directory descriptor. After all piece hashes and a streaming whole-file hash pass, the file is flushed and installed using an atomic hard link that refuses to replace an existing name. The temporary link is removed. Failed temporary files remain until client shutdown; persistent restart/resume is not implemented.

## File metadata and discovery

SHA1 is implemented in standard C++, with incremental updates and a 64-byte compression buffer. File inspection uses a reusable 512 KiB buffer and an open descriptor retained for serving. Whole-file and per-piece digests are computed together; size and modification/change timestamps are checked before and after inspection. Each outgoing piece is rehashed. A changed/unreadable source is disabled locally and its withdrawal is retried after tracker outages.

Metadata contains a safe basename, 64-bit size, fixed piece size (524288), whole-file SHA1 and one SHA1 per piece. Empty files have zero pieces and the SHA1 of the empty stream. The size limit is 1 GiB. Metadata uses the existing length-prefixed field codec, with lowercase hexadecimal hashes. Upload is the only request allowed a metadata field up to 128 KiB; journal records allow up to 132 KiB, accommodating a complete 1 GiB manifest. Other request field limits remain unchanged.

Files are scoped to groups. Only members publish, list, discover or request pieces. Identical metadata under an existing filename adds a seeder; conflicting content is rejected. A record remains reserved for its original content until its group is deleted, even after every share is withdrawn. `list_files` lists files with registered shares; it does not promise a live source. Discovery returns metadata plus currently live candidate endpoints. With no peers, a nonempty download waits and periodically refreshes discovery.

Shares bind the publishing user to their current session. Logout, replacement login and leaving the group remove the affected registrations. File mutations use the existing replicated journal, request IDs and deterministic replay. Partition behavior remains eventual reconciliation, not consensus: conflicting publications can be provisionally accepted on isolated trackers, with one rejected during replay after reconnection.

Clients heartbeat both trackers approximately every two seconds while background maintenance is idle. Heartbeat observations are local, monotonic and expire after 15 seconds; they are not journaled. Both trackers start with empty liveness tables after restart. A busy/unreachable tracker or prolonged maintenance can delay heartbeats, so discovery is a candidate list rather than a guarantee of reachability.

## Peer protocol and scheduling

Peer messages reuse `P2P1` framing. Each TCP connection handles one request:

- `BITFIELD, group, filename, whole_hash, requester_token` returns `OK, whole_hash, ASCII_bitfield`.
- `PIECE, group, filename, whole_hash, requester_token, index` returns `OK, whole_hash, index, bytes`.
- Errors return `ERROR, message`.

The seeder checks local share state for every request and asks a tracker to authorize the requester membership, file identity and source session. Successful authorization is rechecked every two seconds. If both trackers become unavailable, an already-authorized requester/file pair can continue for up to two minutes since its last successful authorization. No fresh authorization is granted offline. Explicit tracker denials discard cached authorization. Local logout/stop-share is immediate; remote revocation can be delayed by the cache/grace period. The educational protocol does not provide transport encryption.

There are four download workers total per client, shared round-robin across jobs, plus four peer-server workers and a bounded 64-connection queue. Piece reservations prevent duplicate ownership. Initial peer assignment is distributed across sources; each failed piece independently advances to another peer. Retry backoff is capped at two seconds. Workers query peer bitfields, validate response identity/index/length, verify SHA1, complete offset-based writes and only then expose a verified bit. Partial availability is published after the first verified piece. Final verification uses a worker and never blocks the CLI.

The scheduler uses sequential missing pieces, not rarest-first or duplicate endgame requests. Existing peers remain usable during tracker outages within the authorization grace period. Discovery refreshes resume after recovery. A job fails after 120 seconds without verified progress. Publication retries retain their original request IDs; publication, background announcements and withdrawal are ordered to avoid reactivating a stopped share.

## Resource and persistence limits

- 16 active downloads and 64 retained jobs per client; restart clears job history.
- 256 locally published shares; downloaded shares are also bounded by job history.
- 1 GiB per file; buffers and piece state are bounded independently of file contents.
- The existing 8 MiB / 50,000-event tracker history limit remains. Full-history replication is intended for the assignment scale, not an unbounded catalogue. There is no journal compaction.
- Source files are not copied into a private immutable store. Timestamp checks detect ordinary concurrent changes; piece verification detects content inconsistent with published hashes.
- Client shutdown cleans temporary files, joins its bounded worker threads and releases descriptors. A crashed client's temporary files require manual cleanup; no restart/resume is claimed.

## Validation

Run `make test` for hashing, user/group regression, publication, live client transfer, fault injection and availability suites. `make test-sanitize` repeats these with AddressSanitizer and UndefinedBehaviorSanitizer; `make test-thread-sanitize` repeats them with ThreadSanitizer. `python3 tests/large_file.py` runs the optional 1 GiB transfer and stops both trackers mid-transfer. Tests use isolated temporary files, ports and journals.

Coverage includes independent SHA1 comparisons and piece boundaries, empty/binary files, quoted paths, malformed metadata, membership restrictions, same-name conflicts, duplicate requests, multiple seeders, partial seeding, corrupt/dropped peers, complementary partial sources, concurrent file progress, worker bounds, whole-file mismatch, destination races, source changes during tracker loss, logout, heartbeat expiry, tracker restart/failover and conflicting publications during a real tracker-link partition.

Measured on this macOS arm64 workspace on September 24, 2026:

| Scenario | Preparation | Transfer plus final verification | Sampled peak client RSS |
|---|---:|---:|---|
| 1 GiB, both trackers available | 11.48 s | 14.09 s | Seeder 12,896 KiB; downloader 8,816 KiB |
| 1 GiB, both trackers stopped mid-transfer | 10.48 s | 14.21 s | Seeder 13,504 KiB; downloader 7,680 KiB |

Both runs independently streamed and compared SHA1 over the complete source and destination. The input is a sparse zero-filled file; these timings are local validation evidence rather than general throughput claims. RSS was sampled every 200 ms during transfer and can miss transient peaks. Linux execution has not been validated here.

Final validation after the recovery and publication-ordering fixes: `make test`, `make test-sanitize` and `make test-thread-sanitize` all passed on this workspace, with no sanitizer diagnostics. The complete source/destination verification also passed for the 1 GiB transfer during a simultaneous two-tracker outage.
