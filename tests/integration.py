import argparse
import json
from pathlib import Path
import re
import select
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest
import zlib


def packet(message):
    body = json.dumps(message, ensure_ascii=False).encode("utf-8")
    return struct.pack("!I", len(body)) + body


def receive(sock):
    def exact(count):
        data = b""
        while len(data) < count:
            part = sock.recv(count - len(data))
            if not part:
                raise EOFError("Prekinuta poruka")
            data += part
        return data
    count, = struct.unpack("!I", exact(4))
    if not 0 < count <= 1024 * 1024:
        raise ValueError("Neispravna duzina")
    return json.loads(exact(count))


def record(seq, key="key", value="value", op="PUT"):
    return {"seq": seq, "op": op, "key": key, "value": value}


def encode_record(entry):
    key, value = entry["key"].encode(), entry["value"].encode()
    body = struct.pack("<QBII", entry["seq"], 1 if entry["op"] == "PUT" else 2,
                       len(key), len(value)) + key + value
    body += struct.pack("<I", zlib.crc32(body))
    return struct.pack("<I", len(body)) + body


def read_wal(path):
    data = path.read_bytes()
    records = []
    offset = 0
    while offset < len(data):
        count, = struct.unpack_from("<I", data, offset)
        body = data[offset + 4:offset + 4 + count]
        assert len(body) == count and count >= 21
        assert zlib.crc32(body[:-4]) == struct.unpack_from("<I", body, len(body) - 4)[0]
        seq, op, ks, vs = struct.unpack_from("<QBII", body)
        assert count == 21 + ks + vs and seq == len(records) + 1
        records.append(record(seq, body[17:17 + ks].decode(),
                              body[17 + ks:17 + ks + vs].decode(), "PUT" if op == 1 else "DELETE"))
        offset += 4 + count
    assert offset == len(data)
    return records


def free_ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


class Integration(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="replicated-kv-")
        self.root = Path(self.temp.name)
        self.r1_port, self.r2_port, self.port = free_ports(3)
        self.processes = []
        self.files = []
        self.sockets = []
        self.counter = 0

    def tearDown(self):
        for sock in self.sockets:
            sock.close()
        for proc, path in self.processes:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=5)
        for f in self.files:
            f.close()
        self.temp.cleanup()

    def spawn(self, name, args):
        self.counter += 1
        path = self.root / (str(self.counter) + "-" + name + ".log")
        output = path.open("wb")
        self.files.append(output)
        proc = subprocess.Popen([str(arg) for arg in args], stdout=output, stderr=subprocess.STDOUT)
        self.processes.append((proc, path))
        return proc, path

    def text(self, path):
        return path.read_text(encoding="utf-8", errors="replace")

    def wait_log(self, proc, path, pattern, timeout=7):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            text = self.text(path)
            found = re.search(pattern, text)
            if found:
                return found
            if proc.poll() is not None:
                break
            time.sleep(0.01)
        self.fail("Nema loga " + pattern + "\n" + self.text(path))

    def stop(self, proc):
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=5)

    def replica(self, number, extra=()):
        port = self.r1_port if number == 1 else self.r2_port
        proc, path = self.spawn("replica" + str(number), [REPLICA, "--wal", self.root / ("r" + str(number) + ".wal"),
            "--id", "r" + str(number), "--port", port, "--timeout-ms", 3000, *extra])
        self.wait_log(proc, path, "REPLICA_READY")
        return proc, path

    def leader(self, wait=True, extra=()):
        self.lp, self.ll = self.spawn("leader", [LEADER, "--wal", self.root / "leader.wal", "--port", self.port,
            "--replica1", self.r1_port, "--replica2", self.r2_port, "--heartbeat-ms", 50,
            "--timeout-ms", 700, "--write-timeout-ms", 500, "--retry-ms", 100, *extra])
        self.wait_log(self.lp, self.ll, "RECOVERING")
        if wait:
            self.wait_log(self.lp, self.ll, "LEADER_READY")
        return self.lp, self.ll

    def connect(self, port):
        sock = socket.create_connection(("127.0.0.1", port), timeout=3)
        sock.settimeout(4)
        self.sockets.append(sock)
        return sock

    def request(self, op, key, value=None):
        message = {"type": "REQUEST", "op": op, "key": key}
        if value is not None:
            message["value"] = value
        sock = self.connect(self.port)
        sock.sendall(packet(message))
        result = receive(sock)
        sock.close()
        return result

    def synced(self, port, sequence):
        self.wait_log(self.lp, self.ll, "SYNCED port=" + str(port) + r" .* seq=" + str(sequence) + r"\b")

    def fake_listener(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", self.r1_port))
        listener.listen(4)
        listener.settimeout(4)
        self.sockets.append(listener)
        return listener

    def accept_fake(self, listener, last=0):
        sock, _ = listener.accept()
        sock.settimeout(4)
        self.sockets.append(sock)
        sock.sendall(packet({"type": "HELLO", "replica_id": "fake", "last_seq": last}))
        return sock

    def next_command(self, sock, wanted):
        for _ in range(100):
            message = receive(sock)
            if message["type"] == "PING":
                sock.sendall(packet({"type": "PONG", "nonce": message["nonce"]}))
            elif message["type"] == wanted:
                return message
            else:
                self.fail("Neocekivana poruka: " + str(message))
        self.fail("Nema " + wanted)

    def begin_put(self, key="key", value="value"):
        sock = self.connect(self.port)
        sock.sendall(packet({"type": "REQUEST", "op": "PUT", "key": key, "value": value}))
        return sock

    def test_crud_and_leader_restart_after_ok(self):
        r1 = self.replica(1)
        r2 = self.replica(2)
        self.leader()
        self.assertEqual(self.request("PUT", "a", "one")["status"], "OK")
        self.assertEqual(self.request("PUT", "a", "two")["seq"], 2)
        self.assertEqual(self.request("GET", "a"), {"status": "VALUE", "value": "two"})
        self.assertEqual(self.request("DELETE", "a")["status"], "OK")
        self.assertEqual(self.request("DELETE", "missing")["seq"], 4)
        self.assertEqual(self.request("PUT", "saved", "durable")["seq"], 5)
        self.stop(self.lp)
        self.leader()
        self.assertEqual(self.request("GET", "a")["status"], "NOT_FOUND")
        self.assertEqual(self.request("GET", "saved")["value"], "durable")
        self.assertEqual(self.request("PUT", "next", "six")["seq"], 6)

    def test_one_replica_down_and_returning_suffix_includes_deletes(self):
        one = self.replica(1)
        two = self.replica(2)
        self.leader()
        self.synced(self.r2_port, 0)
        self.stop(two[0])
        self.wait_log(self.lp, self.ll, "PEER_DOWN port=" + str(self.r2_port))
        for op, key, value in [("PUT", "a", "one"), ("PUT", "b", "two"),
                               ("PUT", "a", "three"), ("DELETE", "b", None)]:
            self.assertEqual(self.request(op, key, value)["status"], "OK")
        two = self.replica(2)
        self.synced(self.r2_port, 4)
        self.stop(self.lp)
        self.stop(one[0])
        self.stop(two[0])
        leader_records = read_wal(self.root / "leader.wal")
        self.assertEqual(read_wal(self.root / "r1.wal"), leader_records)
        self.assertEqual(read_wal(self.root / "r2.wal"), leader_records)
        self.assertEqual(leader_records[-1]["op"], "DELETE")

    def test_no_replicas_rejects_before_local_append(self):
        one = self.replica(1)
        two = self.replica(2)
        self.leader()
        self.assertEqual(self.request("PUT", "saved", "value")["status"], "OK")
        self.stop(one[0]); self.stop(two[0])
        self.wait_log(self.lp, self.ll, "PEER_DOWN port=" + str(self.r1_port))
        self.wait_log(self.lp, self.ll, "PEER_DOWN port=" + str(self.r2_port))
        size = (self.root / "leader.wal").stat().st_size
        self.assertEqual(self.request("PUT", "new", "value")["status"], "UNAVAILABLE")
        self.assertEqual(self.request("DELETE", "missing")["status"], "UNAVAILABLE")
        self.assertEqual(self.request("GET", "saved")["value"], "value")
        self.assertEqual((self.root / "leader.wal").stat().st_size, size)
        self.stop(self.lp)
        self.assertEqual(len(read_wal(self.root / "leader.wal")), 1)

    def test_unknown_after_durable_append_then_recovery(self):
        one, one_log = self.replica(1, ["--drop-first-ack"])
        self.leader(extra=["--retry-ms", 900, "--write-timeout-ms", 150])
        result = self.request("PUT", "uncertain", "later visible")
        self.assertEqual(result["status"], "UNKNOWN")
        self.wait_log(one, one_log, "DROP_ACK seq=1")
        for op, key, value in [("GET", "uncertain", None), ("PUT", "other", "value"),
                               ("DELETE", "uncertain", None)]:
            self.assertEqual(self.request(op, key, value)["status"], "UNAVAILABLE")
        self.wait_log(self.lp, self.ll, "COMMITTED seq=1")
        self.assertEqual(self.request("GET", "uncertain")["value"], "later visible")
        self.assertEqual(self.request("PUT", "other", "works")["seq"], 2)

    def test_restart_waits_for_replica_before_opening_service(self):
        first = encode_record(record(1, "saved", "value"))
        (self.root / "leader.wal").write_bytes(first)
        self.leader(wait=False)
        self.assertNotIn("LEADER_READY", self.text(self.ll))
        with self.assertRaises(OSError):
            socket.create_connection(("127.0.0.1", self.port), timeout=0.1)
        self.assertIsNone(self.lp.poll())
        self.replica(1)
        self.wait_log(self.lp, self.ll, "LEADER_READY")
        self.assertEqual(self.request("GET", "saved")["value"], "value")

    def test_heartbeat_alone_is_not_a_write_ack(self):
        listener = self.fake_listener()
        self.leader(wait=False, extra=["--write-timeout-ms", 150, "--timeout-ms", 1500])
        peer = self.accept_fake(listener)
        self.wait_log(self.lp, self.ll, "LEADER_READY")
        client = self.begin_put()
        append = self.next_command(peer, "APPEND")
        pongs = 0
        end = time.monotonic() + 3
        result = None
        while time.monotonic() < end and result is None:
            readable, _, _ = select.select([client, peer], [], [], 1)
            for sock in readable:
                if sock is client:
                    result = receive(client)
                else:
                    ping = receive(peer)
                    self.assertEqual(ping["type"], "PING")
                    peer.sendall(packet({"type": "PONG", "nonce": ping["nonce"]}))
                    pongs += 1
        self.assertGreater(pongs, 0)
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertEqual(self.request("GET", "key")["status"], "UNAVAILABLE")
        peer.sendall(packet({"type": "ACK", "seq": append["record"]["seq"]}))
        self.wait_log(self.lp, self.ll, "COMMITTED seq=1")
        self.assertEqual(self.request("GET", "key")["value"], "value")

    def test_wrong_ack_is_not_counted(self):
        listener = self.fake_listener()
        self.leader(wait=False, extra=["--write-timeout-ms", 150, "--retry-ms", 700])
        peer = self.accept_fake(listener)
        self.wait_log(self.lp, self.ll, "LEADER_READY")
        client = self.begin_put()
        self.next_command(peer, "APPEND")
        peer.sendall(packet({"type": "ACK", "seq": 2}))
        self.assertEqual(receive(client)["status"], "UNKNOWN")
        self.assertNotIn("COMMITTED seq=1", self.text(self.ll))
        replacement = self.accept_fake(listener)
        append = self.next_command(replacement, "APPEND")
        self.assertEqual(append["record"]["seq"], 1)
        replacement.sendall(packet({"type": "ACK", "seq": 1}))
        self.wait_log(self.lp, self.ll, "COMMITTED seq=1")

    def test_leader_killed_after_local_write_can_recover_unanswered_write(self):
        listener = self.fake_listener()
        self.leader(wait=False)
        peer = self.accept_fake(listener)
        self.wait_log(self.lp, self.ll, "LEADER_READY")
        cli, cli_log = self.spawn("client", [CLIENT, "--port", self.port, "put", "pending", "value"])
        self.next_command(peer, "APPEND")
        self.wait_log(self.lp, self.ll, "LOCAL_DURABLE seq=1")
        self.stop(self.lp)
        self.assertEqual(cli.wait(timeout=5), 2)
        result = json.loads(self.text(cli_log))
        self.assertEqual(result["status"], "UNKNOWN")
        self.assertEqual(result["source"], "client")
        peer.close(); listener.close()
        self.replica(1)
        self.leader()
        self.assertEqual(self.request("GET", "pending")["value"], "value")

    def test_replica_duplicate_gap_and_conflict(self):
        one, _ = self.replica(1)
        sock = self.connect(self.r1_port)
        self.assertEqual(receive(sock)["last_seq"], 0)
        first, second = record(1, value="one"), record(2, value="two")
        for entry in [first, second, first]:
            sock.sendall(packet({"type": "APPEND", "record": entry}))
            ack = receive(sock)
            self.assertEqual(ack["type"], "ACK")
            self.assertEqual(ack["seq"], entry["seq"])
        self.assertTrue(ack["duplicate"])
        for entry, code in [(record(4), "GAP"), (record(2, value="conflict"), "CONFLICT")]:
            sock.sendall(packet({"type": "APPEND", "record": entry}))
            self.assertEqual(receive(sock)["code"], code)
        deletion = record(3, value="", op="DELETE")
        sock.sendall(packet({"type": "APPEND", "record": deletion}))
        self.assertEqual(receive(sock)["seq"], 3)
        sock.close()
        self.stop(one)
        self.assertEqual(read_wal(self.root / "r1.wal"), [first, second, deletion])

    def test_conflicting_prefix_is_not_hidden_by_same_last_record(self):
        leader_data = encode_record(record(1, value="leader")) + encode_record(record(2, "last", "same"))
        replica_data = encode_record(record(1, value="different")) + encode_record(record(2, "last", "same"))
        (self.root / "leader.wal").write_bytes(leader_data)
        (self.root / "r1.wal").write_bytes(replica_data)
        one = self.replica(1)
        self.leader(wait=False)
        self.assertEqual(self.lp.wait(timeout=5), 2, self.text(self.ll))
        self.assertIn("Konflikt", self.text(self.ll))
        self.assertNotIn("LEADER_READY", self.text(self.ll))
        self.stop(one[0])
        self.assertEqual((self.root / "leader.wal").read_bytes(), leader_data)
        self.assertEqual((self.root / "r1.wal").read_bytes(), replica_data)

    def test_replica_ahead_of_leader_is_refused(self):
        (self.root / "r1.wal").write_bytes(encode_record(record(1)))
        self.replica(1)
        self.leader(wait=False)
        self.assertEqual(self.lp.wait(timeout=5), 2)
        self.assertIn("ispred", self.text(self.ll))

    def test_partial_tails_recover_then_append(self):
        original = encode_record(record(1, "saved", "value"))
        for name in ["leader.wal", "r1.wal", "r2.wal"]:
            (self.root / name).write_bytes(original + struct.pack("<I", 30) + b"partial")
        one = self.replica(1)
        two = self.replica(2)
        self.leader()
        self.assertEqual(self.request("GET", "saved")["value"], "value")
        self.assertEqual(self.request("PUT", "next", "append")["seq"], 2)
        self.synced(self.r1_port, 2); self.synced(self.r2_port, 2)
        for proc in [self.lp, one[0], two[0]]:
            self.stop(proc)
        expected = [record(1, "saved", "value"), record(2, "next", "append")]
        for name in ["leader.wal", "r1.wal", "r2.wal"]:
            self.assertEqual(read_wal(self.root / name), expected)

    def test_corrupt_wal_prevents_process_start(self):
        data = bytearray(encode_record(record(1)))
        data[-1] ^= 0x55
        self.root.joinpath("bad.wal").write_bytes(data)
        for binary in [LEADER, REPLICA]:
            result = subprocess.run([binary, "--wal", str(self.root / "bad.wal")],
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 2)
            self.assertIn("CRC32", result.stderr)
            self.assertEqual(self.root.joinpath("bad.wal").read_bytes(), data)

    def test_fragmented_invalid_and_stalled_client_frames(self):
        self.replica(1)
        self.leader(extra=["--timeout-ms", 300])
        sock = self.connect(self.port)
        data = packet({"type": "REQUEST", "op": "PUT", "key": "a", "value": "value"})
        for i in range(0, len(data), 3):
            sock.sendall(data[i:i + 3]); time.sleep(0.001)
        self.assertEqual(receive(sock)["status"], "OK")
        for data in [struct.pack("!I", 0), struct.pack("!I", 1024*1024 + 1),
                     struct.pack("!I", 1) + b"!", struct.pack("!I", 30) + b"{"]:
            sock = self.connect(self.port)
            sock.sendall(data)
            self.assertEqual(receive(sock)["status"], "INVALID")
            sock.close()
        self.assertEqual(self.request("GET", "a")["value"], "value")
        self.assertEqual(self.request("PUT", "b", "v")["seq"], 2)

    def test_heartbeat_timeout_removes_ready_replica(self):
        listener = self.fake_listener()
        self.leader(wait=False, extra=["--timeout-ms", 250, "--retry-ms", 900])
        peer = self.accept_fake(listener)
        self.wait_log(self.lp, self.ll, "LEADER_READY")
        self.assertEqual(receive(peer)["type"], "PING")
        self.wait_log(self.lp, self.ll, "PEER_DOWN port=" + str(self.r1_port))
        self.assertEqual(self.request("PUT", "a", "v")["status"], "UNAVAILABLE")
        self.assertNotIn("LOCAL_DURABLE", self.text(self.ll))

    def test_invalid_requests_and_client_program(self):
        self.replica(1)
        self.leader()
        for message in [{"type": "REQUEST", "op": "PUT", "key": "a"},
                        {"type": "REQUEST", "op": "PUT", "key": "a", "value": "x"*4097},
                        {"type": "REQUEST", "op": "GET", "key": "k"*257}]:
            sock = self.connect(self.port); sock.sendall(packet(message))
            self.assertEqual(receive(sock)["status"], "INVALID"); sock.close()
        for command, status in [(["put", "a b", "quoted value"], "OK"),
                                 (["get", "a b"], "VALUE"), (["delete", "a b"], "OK"),
                                 (["get", "a b"], "NOT_FOUND")]:
            result = subprocess.run([CLIENT, "--port", str(self.port), *command],
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout)["status"], status)
        result = subprocess.run([CLIENT, "--port", str(self.r2_port), "put", "a", "v"],
                                capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout)["status"], "UNAVAILABLE")
        result = subprocess.run([CLIENT, "--port", str(self.port)],
            input='put "quoted key" "first"\nput "quoted key" "second" "unterminated\nget "quoted key"\nquit\n',
            capture_output=True, text=True, timeout=5)
        answers = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual([a["status"] for a in answers], ["OK", "INVALID", "VALUE"])
        self.assertEqual(answers[-1]["value"], "first")

    def test_default_replica_ids_allow_two_nodes(self):
        for number, port in [(1, self.r1_port), (2, self.r2_port)]:
            proc, path = self.spawn("default-replica", [REPLICA, "--wal", self.root / ("r" + str(number) + ".wal"),
                                                       "--port", port])
            self.wait_log(proc, path, "REPLICA_READY")
        self.leader()
        self.synced(self.r1_port, 0)
        self.synced(self.r2_port, 0)
        self.assertEqual(self.request("PUT", "a", "value")["status"], "OK")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--leader", required=True)
    parser.add_argument("--replica", required=True)
    parser.add_argument("--client", required=True)
    args, rest = parser.parse_known_args()
    LEADER = str(Path(args.leader).resolve())
    REPLICA = str(Path(args.replica).resolve())
    CLIENT = str(Path(args.client).resolve())
    unittest.main(argv=[sys.argv[0], *rest], verbosity=2)
