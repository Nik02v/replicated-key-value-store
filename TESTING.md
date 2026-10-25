# Testing

Build with testing enabled, then run from the project directory:

```sh
ctest --test-dir build --output-on-failure
```

CMake registers `unit` and, when Python 3 is available, `integration`.
Python tests use only the standard library and start real server processes.

The unit executable contains 7 groups:

1. PUT, overwrite, GET copies, DELETE, empty/binary strings, and restart.
2. Identical duplicates are neither rewritten nor reapplied; GAP and CONFLICT
   leave the history unchanged.
3. Key/value limits and invalid WAL requests.
4. JSON/sequence validation and record round-trip conversion.
5. A differing earlier prefix is detected even when the last record matches.
6. Every incomplete final-byte position of the last record, followed by append
   and another recovery.
7. CRC corruption of a complete record, a skipped sequence number, and exclusive
   WAL opening.

Python unittest contains 17 integration tests:

- CRUD and killing/restarting the leader immediately after OK.
- One replica down, continued writes, and catch-up including DELETE records.
- Both replicas down: PUT/DELETE return UNAVAILABLE and the leader's WAL size
  remains unchanged; GET of a previously confirmed value works.
- A durable write on a real replica, lost ACK, UNKNOWN, temporary UNAVAILABLE
  for all operations, and recovery of the same sequence with a visible value.
- Restart does not open the client port until at least one replica confirms
  the history.
- Valid heartbeat replies without ACK cannot produce OK for a write.
- ACK for the wrong sequence is ignored as a confirmation; reconnection and a
  valid confirmation resolve the same pending write.
- The leader is killed after a durable local append but before replying. The
  client reports UNKNOWN, and restart may make that write visible.
- A real replica confirms an identical duplicate without another application
  or write, and rejects gaps and conflicts.
- An equal final record does not hide a conflicting earlier prefix.
- A replica ahead of the leader's history is rejected without rewriting its log.
- Incomplete tails on the leader and both replicas are truncated before append.
- Complete CRC corruption prevents process startup and leaves the WAL unchanged.
- Fragmented, invalid, oversized, and unfinished client frames.
- Missing PONG removes a previously ready replica.
- Invalid requests, CLI commands, interactive quotes, and failed-connect responses.
- Two replicas with default IDs work together.

Fake peer sockets are used only where ACK/PONG timing needs to be controlled.
Other scenarios start real C++ processes. Crashes are real process kills with
the disk preserved. Deadlines and CTest timeouts bound test waits.

## Demo scenarios

After building, run:

```sh
python tools/demo.py --bin build --scenario normal
python tools/demo.py --bin build --scenario unknown
python tools/demo.py --bin build --scenario crash
```

The normal scenario checks CRUD, continued operation with one replica, and
catch-up of the other. The unknown scenario expects UNKNOWN, then UNAVAILABLE
for GET, followed by VALUE after confirmation of the same record. The crash
scenario kills the leader after OK, restarts it, and checks the saved value.
Each scenario creates its own generated WALs, logs, and result.json under
`demo-output`; these files are excluded from version control.

Tests simulate process crashes while preserving storage. They do not simulate
disk loss or automatic leader election. Platform and sanitizer behavior should
be checked in the environment where the project is built.
