# P2P System

This project is a C++17 peer to peer distributed file sharing system. Users create accounts, form groups, approve membership requests, and share files with other group members. Two trackers store account, group, session, and file metadata. File contents travel directly between clients over TCP; trackers do not store or relay the shared files. Files are divided into 512 KiB pieces, with SHA-1 hashes for every piece and for the complete file. Four download workers per client fetch pieces concurrently, potentially from different peers, while four peer-serving workers handle incoming requests. Verified pieces can be shared before a download finishes.

## Build

### Requirements

- A POSIX environment with IPv4 TCP sockets and the file APIs listed below; the source includes Linux/macOS timestamp handling. There is no native Windows build configuration.
- A C++17 compiler, such as GCC or Clang, and `make`, with thread support (`-pthread`). SHA-1 is implemented within the project; no external crypto library is required.
- Python 3 to run the standalone regression scripts under `tests/`. The current Makefile does not execute test suites through its declared test targets.

Run from the project root:

```sh
make
```

The build produces `tracker.out` and `client.out` in the project root and object/dependency files under `build/`. The default flags include C++17, optimization, debug symbols, warnings, threads, and 64-bit file offsets.

| Shell command | What it does |
| --- | --- |
| `make` / `make all` | Builds both executables and rebuilds changed dependencies.
| `make clean` | Removes the executable binary.

## Working

### Tracker configuration

`tracker_info.txt` must contain exactly two distinct endpoints, each written as `IP PORT`. Blank lines and `#` comments are allowed. The supplied local configuration is:

```text
127.0.0.1 5001
127.0.0.1 5002
```

For different machines, use addresses reachable by the trackers and clients, and permit the configured TCP ports through the network. The client address is both its local listening address and the endpoint it advertises to peers; use a reachable interface address.

### Start the system

Run each command in a separate terminal and keep both client processes open during transfers:

```sh
# Tracker 1: uses the first configuration entry.
./tracker.out tracker_info.txt 1

# Tracker 2: uses the second configuration entry.
./tracker.out tracker_info.txt 2

# Two clients, each with its own listening port.
./client.out 127.0.0.1:6001 tracker_info.txt
./client.out 127.0.0.1:6002 tracker_info.txt
```

The default tracker journals are `tracker_info.txt.tracker1.wal` and `tracker_info.txt.tracker2.wal`. Restarting with the same files restores tracker state. Each tracker needs a different journal; the journal is locked against concurrent use.

| Environment variable | Applies to | Working |
| --- | --- | --- |
| `P2P_STATE_FILE` | Tracker | Overrides that process's WAL path. Its parent directory must exist. |
| `P2P_CLUSTER_KEY` | Tracker | Sets the shared replication/forwarding credential. Both trackers must use the same value; accepted length is 1–256 bytes. Default: `p2p-interim-demo`. |
| `P2P_PREFERRED_TRACKER` | Client | Selects `1` or `2` as the initial preference for ordinary CLI tracker requests. Default: `1`. Transfer-related requests have their own tracker retry loop; tracker 2 normally forwards requests to tracker 1. |

## Commands and working of each command

### Input and response rules

Enter client commands in the running client's terminal. Arguments support single quotes, double quotes, and backslash escaping, so paths and filenames containing spaces should be quoted. Commands are case-sensitive and limited to 4,096 bytes per input line.

User and group IDs must be 1–64 characters using letters, digits, `.`, `_`, or `-`. Passwords must be 1–128 printable ASCII characters. Published filenames must be 1–255 bytes, cannot be `.` or `..`, and cannot contain slashes, backslashes, or control characters.

`create_user` and `login` do not require a current session. Group and file operations require login; file operations also require group membership. `help` and `show_downloads` operate locally; exit commands also attempt tracker logout. Responses begin with `OK` or `ERROR`; a degraded suffix means tracker synchronization is pending.

### Relative and absolute file paths

Both `upload_file` and `download_file` accept relative and absolute paths. Relative paths are resolved from the **working directory where that client was started**, independently for each client. They are not relative to the executable, tracker configuration, or the other peer. `.` means that working directory and `..` means its parent.

| Operation | Relative path example | Absolute path example |
| --- | --- | --- |
| Upload a file | `upload_file study "./files/notes.pdf"` | `upload_file study "/Users/alice/files/notes.pdf"` |
| Download into a directory | `download_file study notes.pdf "./downloads"` | `download_file study notes.pdf "/Users/bob/Downloads"` |
| Use a parent directory | `upload_file study "../shared/notes.pdf"` | — |
| Download into the current directory | `download_file study notes.pdf .` | — |

The upload path must identify a readable regular file. The download destination must be an **existing directory**; the client saves the published filename inside it and does not create missing directories or overwrite existing files. The `file_name` argument to `download_file` remains the published basename, not the uploader's path. Quote paths containing spaces, for example `"../shared files/notes.pdf"`. The interactive client does not expand `~`, environment variables, or shell wildcards; use a full absolute path or a literal relative path.

### Client commands

| Command | Working and expected behavior |
| --- | --- |
| `help` | Prints the supported client command syntax. |
| `create_user <user_id> <password>` | Registers an account on the tracker. Rejects invalid IDs/passwords and duplicate users. Does not log in automatically. |
| `login <user_id> <password>` | Validates credentials, creates a session token, and registers the client's listening endpoint. If the account already has a session on any client, returns `ALREADY_LOGGED_IN` without changing that session or its shares. Log out or exit the first client before logging in elsewhere. New logins require tracker 1; tracker 2 forwards them and never issues a session independently. |
| `create_group <group_id>` | Creates a unique group and makes the caller its owner and first member. |
| `join_group <group_id>` | Adds a pending join request. Membership begins only after owner approval; duplicate pending requests and requests from existing members are rejected. |
| `list_groups` | Lists all group IDs, including groups the caller has not joined. Requires login. |
| `list_requests <group_id>` | Lists pending user IDs. Only the group owner can run it successfully. |
| `accept_request <group_id> <user_id>` | Lets the owner approve an existing pending request and add that user to the group. |
| `leave_group <group_id>` | Removes membership and the caller's tracker shares in that group. Locally stops the group's shares and fails its active downloads. If the owner leaves, ownership passes to the earliest remaining member; if nobody remains, the group is deleted. |
| `upload_file <group_id> <file_path>` | Opens and hashes a local regular file, publishes its basename, size, whole-file hash, and piece hashes, then serves it from the client. File bytes are not uploaded to the tracker. Identical content can have multiple sharers; different content under the same group filename is rejected. **Group ID comes before file path.** |
| `list_files <group_id>` | Lists filenames with at least one registered share in the group. It does not prove the peers are currently reachable or hold a complete copy. |
| `download_file <group_id> <file_name> <destination_path>` | Discovers metadata and live peers, creates a temporary file, and queues a background download. **The destination must be an existing directory**, not a new filename. The final file is `<destination_path>/<file_name>`, and an existing file or symlink at that name is rejected. `OK: Download queued` is not completion; wait for the automatic completion/failure message or inspect `show_downloads`. |
| `show_downloads` | Prints this process's download history: `[D]` downloading with verified/total piece counts, `[C]` completed, or `[F]` failed. Displays the latest error when present; a downloading job may show a transient retry error. Does not query the tracker. |
| `stop_share <group_id> <file_name>` | Disables the local source and removes the caller's tracker share registration. Does not delete local data or other users' shares. It does not cancel an active download; that job can continue fetching while local serving stays disabled. |
| `logout` | Clears the session and tracker share registrations. Local sharing stops and active downloads fail with `Session ended`; existing completed files remain on disk. |
| `retry` | Resends an uncertain pending CLI tracker request with its original request ID and payload. Use it after an “outcome unknown” error before another tracker command. It does not restart a failed download; use `download_file` again for that. |
| `quit` / `exit` | Stops local sharing/download work, logs out the current user through a reachable tracker, and shuts down worker threads. End-of-input, Ctrl+C (`SIGINT`), `SIGTERM`, and `SIGHUP` use the same cleanup path. An uncertain pending login is retried with its original ID so its session can also be logged out. If trackers are unreachable, logout cannot be confirmed. |

Local stop actions for `logout`, `leave_group`, and `stop_share` happen before the tracker response. A tracker failure does not automatically restore the stopped local activity. Keep published source files unchanged while serving them; a piece mismatch disables the source and triggers withdrawal of its tracker share.

### Automatic download results

After a queued download finishes, the client prints a technical result automatically, even while waiting for the next command. You do not need to run `show_downloads` to see completion or failure.

Example completion for a three-byte file containing `abc`:

```text
[DOWNLOAD COMPLETED] group="study" file="sample.bin" bytes=3 pieces=1/1 integrity=VERIFIED sha1=a9993e364706816aba3e25717850c26c9cd0d89d
```

Example failure:

```text
[DOWNLOAD FAILED] group="study" file="sample.bin" bytes=3 pieces=1/1 reason="Whole-file SHA1 mismatch"
```

`bytes` is the expected total file size and `pieces` is the verified-piece count divided by the total count. `COMPLETED` is reported only after piece/whole-file SHA-1 checks, data flushing, and successful installation of the final filename. A failure includes its specific reason, such as a hash mismatch, disk-write error, destination conflict, or `Session ended` when logout/exit cancels a job. Even `pieces=1/1` can fail final verification or installation.

Each queued job produces one terminal notification. Transient piece failures remain retries and do not produce a final failure notification. `show_downloads` still lists `[D]`, `[C]`, and `[F]` history without repeating automatic messages. Notifications wait until the CLI regains control if a foreground command is blocked on a tracker request.

### Session exclusivity and exit behavior

An account can have one current login. A rejected second login leaves the first client and its published shares intact. Tracker 1 is the sole issuer of new sessions; tracker 2 forwards login requests. If tracker 1 cannot be reached, new logins wait/retry instead of falling back to independent session creation. Existing sessions and other operations retain tracker failover. Both trackers must run the updated implementation.

`quit`, `exit`, EOF, Ctrl+C, `SIGTERM`, and `SIGHUP` stop local transfers and attempt logout before termination. A signal received during a request is handled after that request returns; signal handlers do not perform network operations. Successful logout clears shares but preserves groups, ownership, and memberships.

`SIGKILL`, process crashes, power loss, or an outage preventing logout cannot guarantee tracker cleanup. Such a session remains registered and blocks a fresh login until it is explicitly logged out using its token; heartbeat expiry only removes peers from discovery and does not release the login. There is currently no automatic session expiry or account-reset command. Restarting trackers preserves sessions. An exit warning reports unconfirmed cleanup; it does not mean the account was logged out.

Run the session regression checks from the project root after building:

```sh
python3 tests/session_lifecycle.py .
```

The suite uses isolated temporary trackers and files; it covers concurrent login rejection, exit cleanup, tracker partitions/restarts, lost replies, and older journal compatibility.

### Tracker console commands

| Command/action | Working |
| --- | --- |
| `status` | Prints the current replicated/degraded state, event count, user count, group count, and reconciliation-conflict count. |
| `quit` | Stops accepting work, joins tracker threads, and exits while preserving the WAL. |
| Ctrl+C / `SIGTERM` | Requests tracker shutdown through its signal handler. Tracker stdin reaching EOF merely disables the console; it does not stop the server. |

### Example sharing workflow

Create an existing destination directory from a shell:

```sh
mkdir -p downloads
```

In client A, using an existing local file:

```text
create_user alice demo-alice
login alice demo-alice
create_group study
upload_file study "/absolute/path/notes.pdf"
```

In client B:

```text
create_user bob demo-bob
login bob demo-bob
join_group study
```

Back in client A:

```text
list_requests study
accept_request study bob
```

Then in client B, with `downloads` relative to that client's working directory:

```text
list_files study
download_file study notes.pdf downloads
show_downloads
```

Wait for `[DOWNLOAD COMPLETED]` for `notes.pdf`, or check for `[C] [study] notes.pdf` using `show_downloads`. The result is `downloads/notes.pdf`. Keep a client logged in and running to serve its published or verified downloaded pieces.

### Internal command flow

These messages are used by the implementation and are not additional interactive CLI commands:

| Message/operation | Working |
| --- | --- |
| `CLIENT` | Wraps a tracker request with its request ID, session token, command, and arguments. |
| `FORWARD` | Lets tracker 2 forward a request to tracker 1 using the cluster key. If forwarding fails, tracker 2 can execute locally in degraded mode. |
| `SYNC` / `SYNC_OK` | Exchanges the trackers' complete event histories and merges missing events. Background synchronization runs between requests as well. |
| `PING` / `PONG` | Provides a basic tracker reachability/identity response. |
| `heartbeat` | Refreshes session liveness at each tracker. The client maintenance loop normally waits two seconds between cycles; discovery excludes sessions without a heartbeat in the last 15 seconds. |
| `discover` | Returns a file's metadata and live peer endpoints to a group member. The downloader refreshes this list while working. |
| `authorize` | Checks the requesting user's membership, file identity, and serving peer's current share/session. Successful checks are cached for two seconds, with the bounded outage grace described above. |
| `BITFIELD` | Asks a peer which pieces it has verified and can serve. |
| `PIECE` | Requests one indexed piece. The server reads and hashes it; the receiver validates its identity, size, and hash, then writes it at the correct offset. Failed requests are retried with backoff and peer rotation. |

Messages use a custom `P2P1` frame with a length-prefixed payload and a 16 MiB frame limit. After verifying pieces, the client automatically publishes its availability using the tracker `upload_file` operation. Once all pieces are present, it rechecks the entire file, flushes its data, and installs the final filename without replacement.

## All the syscalls used

The inventory below covers all explicitly called system-call interfaces in the application source under `src/`. Calls are made through C/POSIX library wrappers; their underlying kernel syscall names can differ between operating systems. Related library APIs are separated below, and implicit calls made by the C++ runtime are not presented as directly used syscalls.

### Networking and readiness

| Interface | Purpose in this project | Source files |
| --- | --- | --- |
| `socket()` | Creates IPv4 TCP listener and outgoing connection sockets (`AF_INET`, `SOCK_STREAM`). | `src/common/net.cpp` |
| `setsockopt()` | Enables `SO_REUSEADDR` on listening sockets. | `src/common/net.cpp` |
| `bind()` | Assigns the configured IP and port to a tracker or peer listener. | `src/common/net.cpp` |
| `listen()` | Enables incoming connections with a backlog of 64. | `src/common/net.cpp` |
| `accept()` | Accepts client, tracker, or peer connections and places them in worker queues. | `src/tracker/server.cpp`, `src/client/transfer.cpp` |
| `connect()` | Starts an outgoing connection to a tracker or peer. | `src/common/net.cpp` |
| `getsockopt()` | Reads `SO_ERROR` to check whether a nonblocking connection completed successfully. | `src/common/net.cpp` |
| `poll()` | Waits for socket readability/writability with deadlines, monitors listeners, and polls tracker/client stdin so exit signals can trigger cleanup. | `src/common/net.cpp`, `src/tracker/server.cpp`, `src/client/transfer.cpp`, `src/client/cli.cpp` |
| `send()` | Sends framed requests, responses, metadata, and piece bytes using `MSG_DONTWAIT`, handling partial sends. | `src/common/net.cpp` |
| `recv()` | Receives frame headers and payloads using `MSG_DONTWAIT`, handling partial reads. | `src/common/net.cpp` |

### Signals

| Interface | Purpose in this project | Source files |
| --- | --- | --- |
| `sigaction()` | Installs/restores client handlers for `SIGINT`, `SIGTERM`, and `SIGHUP`. Handlers only set a flag; the command loop performs logout and cleanup. `sigemptyset()` initializes the handler mask. | `src/client/cli.cpp` |

### Files, descriptors, and persistence

| Interface | Purpose in this project | Source files |
| --- | --- | --- |
| `open()` | Opens source files, destination directories, `/dev/urandom`, and tracker WAL files. WAL creation uses mode `0600` and refuses a final-component symlink. | `src/common/file.cpp`, `src/common/protocol.cpp`, `src/client/transfer.cpp`, `src/tracker/journal.cpp` |
| `openat()` | Creates a unique temporary download file relative to the opened destination directory, using `O_EXCL`, `O_NOFOLLOW`, and mode `0600`. | `src/client/transfer.cpp` |
| `close()` | Releases file and socket descriptors through the `Fd` ownership wrapper. | `src/common/net.cpp` |
| `fcntl()` | Sets `O_NONBLOCK` on connecting/listening sockets; takes an `F_SETLK` write lock on each WAL. | `src/common/net.cpp`, `src/client/transfer.cpp`, `src/tracker/server.cpp`, `src/tracker/journal.cpp` |
| `read()` | Reads random bytes for request/session IDs and reads tracker/client console input. | `src/common/protocol.cpp`, `src/tracker/server.cpp`, `src/client/cli.cpp` |
| `write()` | Appends framed records to the tracker WAL, handling partial writes and interruption. | `src/tracker/journal.cpp` |
| `pread()` | Reads file pieces and WAL records at explicit offsets without changing the shared descriptor offset. | `src/common/file.cpp`, `src/client/transfer.cpp`, `src/tracker/journal.cpp` |
| `pwrite()` | Writes downloaded pieces at their offsets so workers can write separate pieces concurrently. | `src/client/transfer.cpp` |
| `fstat()` | Checks source type, size, and timestamps; validates destination directories and WAL regular files. | `src/common/file.cpp`, `src/client/transfer.cpp`, `src/tracker/journal.cpp` |
| `fstatat()` | Checks for an existing destination entry relative to the directory, without following a final symlink (`AT_SYMLINK_NOFOLLOW`). | `src/client/transfer.cpp` |
| `fchmod()` | Restricts WAL permissions to owner read/write (`0600`). | `src/tracker/journal.cpp` |
| `ftruncate()` | Sets the temporary download's logical size; removes an incomplete WAL tail or rolls back a failed append. It does not guarantee disk-space reservation. | `src/client/transfer.cpp`, `src/tracker/journal.cpp` |
| `fsync()` | Flushes completed download data and WAL writes/repairs before proceeding. Download finalization does not separately sync the destination directory. | `src/client/transfer.cpp`, `src/tracker/journal.cpp` |
| `lseek()` | Finds the WAL's end offset before appending so a failed write can be rolled back. | `src/tracker/journal.cpp` |
| `linkat()` | Creates the verified download's final name as a hard link within the destination directory; fails if the name already exists. The destination filesystem must support hard links. | `src/client/transfer.cpp` |
| `unlinkat()` | Removes a completed download's temporary name and cleans up remaining temporary files when download-job objects are destroyed. | `src/client/transfer.cpp` |
