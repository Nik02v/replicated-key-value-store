import argparse
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time


def ports():
    sockets = [socket.socket(), socket.socket()]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def wait_log(proc, path, pattern):
    end = time.monotonic() + 10
    while time.monotonic() < end:
        text = path.read_text(encoding="utf-8", errors="replace")
        match = re.search(pattern, text)
        if match:
            return match
        if proc.poll() is not None:
            break
        time.sleep(0.01)
    raise RuntimeError("Nema loga " + pattern + "\n" + text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin", default="build")
    parser.add_argument("--scenario", choices=["normal", "unknown", "crash"], default="normal")
    parser.add_argument("--output", default="demo-output")
    args = parser.parse_args()
    binaries = Path(args.bin).resolve()
    suffix = ".exe" if os.name == "nt" else ""
    output = Path(args.output).resolve() / (args.scenario + "-" + str(time.monotonic_ns()))
    output.mkdir(parents=True)
    r1_port, r2_port = ports()
    processes, files, responses = [], [], []
    leader_number = 0
    replica_number = 0

    def spawn(name, command):
        path = output / (name + ".log")
        log = path.open("wb")
        files.append(log)
        proc = subprocess.Popen([str(a) for a in command], stdout=log, stderr=subprocess.STDOUT)
        processes.append(proc)
        return proc, path

    def replica(number, extra=()):
        nonlocal replica_number
        replica_number += 1
        port = r1_port if number == 1 else r2_port
        proc, path = spawn("replica-" + str(number) + "-" + str(replica_number),
            [binaries / ("kv_replica" + suffix), "--wal", output / ("r" + str(number) + ".wal"),
             "--id", "r" + str(number), "--port", port, *extra])
        wait_log(proc, path, "REPLICA_READY")
        return proc, path

    def leader():
        nonlocal leader_number
        leader_number += 1
        proc, path = spawn("leader-" + str(leader_number), [binaries / ("kv_leader" + suffix),
            "--wal", output / "leader.wal", "--port", 0, "--replica1", r1_port, "--replica2", r2_port,
            "--heartbeat-ms", 100, "--timeout-ms", 1000, "--retry-ms", 1000, "--write-timeout-ms", 250])
        port = int(wait_log(proc, path, r"LEADER_READY port=(\d+)")[1])
        return proc, path, port

    def call(port, command, expected):
        result = subprocess.run([str(binaries / ("kv_client" + suffix)), "--port", str(port), *command],
                                capture_output=True, text=True, encoding="utf-8", timeout=6)
        if not result.stdout:
            raise RuntimeError(result.stderr)
        response = json.loads(result.stdout)
        print(" ".join(command), "->", response, flush=True)
        responses.append(response)
        if response["status"] != expected:
            raise RuntimeError("Neocekivan odgovor: " + str(response))
        return response

    try:
        one, one_log = replica(1, ["--drop-first-ack"] if args.scenario == "unknown" else [])
        two = None
        if args.scenario != "unknown":
            two, _ = replica(2)
        lp, ll, port = leader()

        if args.scenario == "unknown":
            call(port, ["put", "uncertain", "later-visible"], "UNKNOWN")
            call(port, ["get", "uncertain"], "UNAVAILABLE")
            wait_log(lp, ll, "COMMITTED seq=1")
            result = call(port, ["get", "uncertain"], "VALUE")
            if result["value"] != "later-visible":
                raise RuntimeError("Nereseni upis nije oporavljen")
            two, _ = replica(2)
            wait_log(lp, ll, "SYNCED port=" + str(r2_port) + r" .* seq=1\b")
        elif args.scenario == "crash":
            call(port, ["put", "saved", "survives-kill"], "OK")
            print("Ubijam leader proces odmah posle OK.", flush=True)
            lp.kill(); lp.wait(timeout=5)
            lp, ll, port = leader()
            result = call(port, ["get", "saved"], "VALUE")
            if result["value"] != "survives-kill":
                raise RuntimeError("Potvrdjen upis je izgubljen")
        else:
            call(port, ["put", "name", "Ana"], "OK")
            call(port, ["put", "name", "Mina"], "OK")
            result = call(port, ["get", "name"], "VALUE")
            if result["value"] != "Mina":
                raise RuntimeError("Overwrite nije vidljiv")
            call(port, ["delete", "name"], "OK")
            call(port, ["get", "name"], "NOT_FOUND")
            call(port, ["delete", "missing"], "OK")
            two.kill(); two.wait(timeout=5)
            wait_log(lp, ll, "PEER_DOWN port=" + str(r2_port))
            call(port, ["put", "offline", "still-works"], "OK")
            two, _ = replica(2)
            wait_log(lp, ll, "SYNCED port=" + str(r2_port) + r" .* seq=5\b")

        report = {"scenario": args.scenario, "successful": True, "responses": responses}
        (output / "result.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        print("WAL, logovi i result.json:", output)
        return 0
    except Exception as e:
        print("Demo:", e, file=sys.stderr)
        print("Logovi:", output, file=sys.stderr)
        return 1
    finally:
        for proc in processes:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=5)
        for f in files:
            f.close()


if __name__ == "__main__":
    sys.exit(main())
