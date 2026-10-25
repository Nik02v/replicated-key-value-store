# Replicated Key-Value Store

A C++17 project with one fixed leader and two replicas. Processes communicate
over ordinary TCP on the same machine. The leader accepts GET/PUT/DELETE; a
write receives OK only after the leader and at least one replica have stored
it durably.

The leader is not elected automatically. If it crashes, the service waits for
its restart. The leader's WAL remains the authoritative history; this project
has no Raft, transactions, membership changes, snapshots, or compaction. Writes
are deliberately serialized: at most one write can be pending.

## Build

Linux, CMake 3.20+, and GCC/Clang:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
python3 tools/demo.py --bin build --scenario normal
python3 tools/demo.py --bin build --scenario unknown
python3 tools/demo.py --bin build --scenario crash
```

The only external C++ library is nlohmann/json, already included in `third_party`.
The build downloads nothing. Python 3 with only its standard library is used
for tests and demos. Add `-DBUILD_TESTING=OFF` to build without tests.

On Windows with MinGW, put `g++`, `mingw32-make`, `cmake`, and `python` in
PATH, then run from the project directory:

```powershell
cmake -S . -B build -G "MinGW Makefiles" -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j 2
ctest --test-dir build --output-on-failure
python tools/demo.py --bin build --scenario normal
python tools/demo.py --bin build --scenario unknown
python tools/demo.py --bin build --scenario crash
```

## Manual execution

Use four terminals from the project directory (add `.exe` to program names on
Windows):

```sh
./build/kv_replica --wal r1.wal --id r1 --port 9101
./build/kv_replica --wal r2.wal --id r2 --port 9102
./build/kv_leader --wal leader.wal --port 9000 --replica1 9101 --replica2 9102
./build/kv_client --port 9000 put name Ana
./build/kv_client --port 9000 get name
./build/kv_client --port 9000 put name Mina
./build/kv_client --port 9000 delete name
./build/kv_client --port 9000 get name
```

Each process needs its own WAL. The file's parent directory must already exist.
All connections use `127.0.0.1`; `--replica1` and `--replica2` specify two fixed
ports. An optional replica ID identifies the peer; if omitted, it defaults to
`replica-<port>`. Explicit IDs should be distinct.

Without a command argument, the client reads interactively, using quotes for
strings with spaces:

```text
put "course name" "distributed systems"
get "course name"
delete "course name"
quit
```

Each command opens one TCP connection, sends a request, and reads one JSON
response. Examples: `{"seq":1,"status":"OK"}` or
`{"status":"VALUE","value":"Ana"}`. Client exit codes are 0 for
OK/VALUE/NOT_FOUND, 1 for UNAVAILABLE, 2 for UNKNOWN, and 3 for invalid input or
responses. Servers support Ctrl+C/SIGTERM for shutdown; demos and tests
deliberately kill processes in crash scenarios and during cleanup.

## Response meanings

| Status | Meaning |
|---|---|
| OK | The specific write is durable on the leader and at least one replica. |
| VALUE / NOT_FOUND | GET result from the leader's available state. |
| UNAVAILABLE | The request was not executed; a write has no ready replica, or an earlier write is unresolved. |
| UNKNOWN | The write may have executed, but its confirmation was not obtained. |
| INVALID | Invalid request, with no write. |

Keys may contain up to 256 bytes and values up to 4096 bytes. Empty strings are
allowed. The network carries JSON strings in UTF-8; an escaped NUL is stored as
a byte in the WAL. DELETE creates its own record and returns OK even for a
missing key, once a majority confirms it.

If both replicas disappear after an earlier successful recovery, GET of the
previously confirmed state still works. New PUT/DELETE requests return
UNAVAILABLE before a local append. This is a fixed-leader system; there is no
other leader concurrently changing the history.

UNKNOWN does not mean rollback. The leader retains the same sequence number and
tries to obtain confirmation after reconnecting. Meanwhile, all new
GET/PUT/DELETE requests receive UNAVAILABLE; the map is not read and no new
record is added. Once confirmation arrives, the history becomes available and
the uncertain write may be visible.

If a client connection fails after sending a PUT/DELETE request has begun, the
client also reports UNKNOWN with `source=client`, because it does not know
whether the leader handled it. A failed connect returns UNAVAILABLE: the request
was not sent. There is no request-ID deduplication for client commands; a
repeated PUT after recovery may create a new record. Deduplication applies to
the replica's record sequence number.

## Leader operation

On startup, the leader loads its WAL and builds the map and history vector.
The complete recovered WAL is also synchronized before use. It then logs
RECOVERING and connects to both known replica ports.

The client port opens only after at least one replica has durably confirmed the
leader's entire history. Until then, connections to the client port fail. This
includes a final record that may not have received a response before a crash.
LEADER_READY marks completion of this barrier.

For a new write:

1. Check for a connected replica synchronized through the latest sequence.
2. Assign the next sequence number and append + sync the local WAL.
3. Send APPEND to replicas that have reached that point in the history.
4. Wait for confirmation of that specific record. The first valid confirmation
   gives a majority: leader plus one replica.
5. Return OK; if the deadline expires, return UNKNOWN and continue recovering
   the same record.

The local map changes after the durable local append. While the record is
unconfirmed, the pending state rejects GET, preventing clients from reading the
unresolved change. A separate commit-index file is not needed for this scope:
writes are serialized, an unresolved record blocks the next write, and restart
again requires confirmation of the leader's entire WAL.

Only the main thread modifies the map, pending write, and replica state. One
thread reads each replica socket; short-lived client reader threads receive
one request each. They only append events under `queue_mutex`. The main thread
drains the queue and handles events without holding that lock. The queue lock
is not held during disk I/O, network sends, or joins. At most 32 client
connections may be active at once.

Each new replica connection gets a new generation number. Messages from old
connections are ignored. ACK/CHECKED must match the type and sequence of the
currently expected confirmation; a wrong confirmation closes the connection
and cannot produce OK for a client.

## Replica catch-up and history conflicts

On connection, the replica sends HELLO with its last durable sequence number.
The leader checks existing records using CHECK, from the replica's first record
to its last. Every field is checked, rather than just the last record or record
count. This deliberately simple full-prefix verification takes multiple
round trips.

After verification, the leader sends APPEND only for the missing suffix. The
replica appends and synchronizes each new record before ACK. It counts as ready
only after reaching the leader's latest sequence number.

An identical duplicate APPEND is confirmed without another write or application.
An old duplicate PUT therefore cannot undo a later overwrite or DELETE. A gap
receives NACK/GAP; different contents at an existing sequence receive
NACK/CONFLICT. The leader exits with a clear error on a history conflict or a
replica ahead of its WAL. It does not truncate a replica's valid WAL or change
the leader's authority.

## Heartbeats and deadlines

The leader sends PING, and the replica replies with PONG using the same nonce.
PONG keeps the connection alive but does not confirm a write. A specific APPEND
needs its own ACK. Leader parameters:

- `--heartbeat-ms 300`: liveness check interval.
- `--timeout-ms 1500`: deadline for HELLO, CHECKED, ACK, PONG, and a complete
  incoming TCP frame.
- `--write-timeout-ms 1500`: client confirmation wait after the local sync.
- `--retry-ms 250`: interval between reconnection attempts.

The replica uses `--timeout-ms 5000` for incoming frames and an inactive leader.
Intervals use the monotonic `steady_clock`. Keep the heartbeat interval below
the timeout.

Sockets use blocking send/recv. Only connect temporarily switches to nonblocking
mode, with a select timeout of at most 100 ms in the leader, before returning
to blocking mode. An offline replica therefore does not stall heartbeats to a
healthy replica. Sends have a one-second socket timeout; reception has a deadline
for the entire frame, including a partial prefix and body. A descriptor is
closed only after shutting down and joining its reader.

## WAL and durability

The format and low-level File code are copied from project 4. There is no runtime
dependency on that directory; this project contains its own copy.

Record format: u32 body length, u64 seq, u8 operation (PUT=1, DELETE=2), u32 key
length, u32 value length, key/value bytes, and u32 CRC32. Numbers are little-endian.
CRC32 covers the body before the checksum, starting with the sequence number.
Body length is 21..4373 bytes. No C++ structure with padding is dumped to disk.

Linux uses fdatasync and Windows uses _commit. An incomplete final prefix/body
is truncated to the previous valid boundary, with a sync before the next append.
A complete record with a bad CRC, lengths, or sequence stops recovery. The WAL
is retained from the beginning. Each open process holds an exclusive lock or
share-deny open on its WAL. An append/sync error stops the node; it does not
continue after a possible partial write without restarting.

Tests preserve the disk during process crashes. Loss of the leader's disk,
automatic failover, and recovery by arbitrarily choosing a different history
are not supported.

## Network messages

The prefix is a u32 in network byte order, followed by a UTF-8 JSON object.
Frames may contain at most 1 MiB of body data. Loops handle short send/recv calls;
TCP read boundaries are not message boundaries. WAL and network formats are
intentionally separate.

| Direction | Messages |
|---|---|
| client -> leader | REQUEST: op=GET/PUT/DELETE, key, value for PUT |
| leader -> client | status, optionally value, seq, or error |
| replica -> leader on connection | HELLO: replica_id, last_seq |
| leader -> replica | CHECK/APPEND with record: {seq, op, key, value} |
| replica -> leader | CHECKED/ACK with seq; NACK with code/error |
| leader <-> replica | PING/PONG with nonce |

## Demo and tests

`tools/demo.py` starts the three server processes and creates a new directory
in `demo-output` for each scenario. WALs, logs, and result.json are retained.

- `normal`: CRUD, operation with one replica down, and its later catch-up.
- `unknown`: a replica durably stores the first record and deliberately drops
  the connection before ACK. The client receives UNKNOWN, GET first returns
  UNAVAILABLE, and later returns the recovered value.
- `crash`: the leader is killed immediately after OK; restart returns the
  confirmed value.

The unknown scenario uses `--drop-first-ack` on the replica. This is a one-time
simulation after a real append+sync; the reconnected replica behaves normally.

Unit tests use a small CHECK runner; process tests use Python unittest. Test
waits have deadlines. Test coverage and commands are described in TESTING.md.

Source files are grouped simply: `store.*` for the map/history, `wal.*` and
`file.*` for disk storage, `protocol.*`/`net.*` for TCP, and separate
leader/replica/client programs. The TCP foundation comes from project 1, with
message deadlines and bounded connection time added.
