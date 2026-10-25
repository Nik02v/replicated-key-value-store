#include "cli.h"
#include "protocol.h"
#include "signals.h"

#include <iostream>

using Clock = std::chrono::steady_clock;

struct Options {
    std::string path;
    std::string id;
    int port = 9101;
    int timeout_ms = 5000;
    bool drop_first_ack = false;
};

void serve(Socket& socket, KVStore& store, Options& options)
{
    send_message(socket, {{"type", "HELLO"}, {"replica_id", options.id}, {"last_seq", store.last_sequence()}});
    auto last_received = Clock::now();
    while (!stop_requested.load()) {
        if (Clock::now() - last_received > std::chrono::milliseconds(options.timeout_ms))
            throw std::runtime_error("Leader heartbeat timeout");
        if (!wait_readable(socket, 20))
            continue;
        json message;
        if (!receive_message(socket, message, options.timeout_ms))
            return;
        last_received = Clock::now();
        std::string type = message.at("type").get<std::string>();
        if (type == "PING") {
            auto nonce = read_sequence(message, "nonce");
            send_message(socket, {{"type", "PONG"}, {"nonce", nonce}});
            continue;
        }

        try {
            WalRecord entry = parse_record(message.at("record"));
            if (type == "CHECK") {
                store.verify(entry);
                send_message(socket, {{"type", "CHECKED"}, {"seq", entry.sequence}});
            } else if (type == "APPEND") {
                bool added = store.append(entry);
                std::cout << "DURABLE seq=" << entry.sequence << " duplicate=" << !added << std::endl;
                if (options.drop_first_ack && added) {
                    options.drop_first_ack = false;
                    std::cout << "DROP_ACK seq=" << entry.sequence << std::endl;
                    socket.shutdown_all();
                    return;
                }
                send_message(socket, {{"type", "ACK"}, {"seq", entry.sequence}, {"duplicate", !added}});
            } else {
                throw std::invalid_argument("Neocekivana poruka za repliku");
            }
        } catch (const HistoryError& e) {
            std::cout << "REJECT " << e.code << " " << e.what() << std::endl;
            send_message(socket, {{"type", "NACK"}, {"code", e.code}, {"error", e.what()}});
        } catch (const std::invalid_argument& e) {
            send_message(socket, {{"type", "NACK"}, {"code", "INVALID"}, {"error", e.what()}});
        }
    }
}

int main(int argc, char** argv)
{
    try {
        Options options;
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "--wal") options.path = next_arg(i, argc, argv);
            else if (arg == "--id") options.id = next_arg(i, argc, argv);
            else if (arg == "--port") options.port = cli_int(next_arg(i, argc, argv), 0, 65535);
            else if (arg == "--timeout-ms") options.timeout_ms = cli_int(next_arg(i, argc, argv), 100, 60000);
            else if (arg == "--drop-first-ack") options.drop_first_ack = true;
            else if (arg == "--help") {
                std::cout << "kv_replica --wal r1.wal [--id r1] [--port 9101] [--timeout-ms 5000]\n"
                             "  [--drop-first-ack]  namerna simulacija izgubljene potvrde\n";
                return 0;
            } else throw std::invalid_argument("Nepoznat argument: " + arg);
        }
        if (options.path.empty() || options.id.size() > 64)
            throw std::invalid_argument("Obavezan je --wal; ID ima najvise 64 bajta");
        install_signals();
        KVStore store(options.path);
        Network network;
        Socket listener = listen_local(options.port);
        if (options.id.empty())
            options.id = "replica-" + std::to_string(local_port(listener));
        std::cout << "REPLICA_READY port=" << local_port(listener) << " id=" << options.id
                  << " seq=" << store.last_sequence() << " truncated=" << store.recovery_info().truncated_bytes << std::endl;
        while (!stop_requested.load()) {
            if (!wait_readable(listener, 20))
                continue;
            Socket connection = accept_one(listener);
            try {
                serve(connection, store, options);
            } catch (const std::exception& e) {
                if (store.failed())
                    throw;
                std::cout << "DISCONNECTED " << e.what() << std::endl;
            }
            connection.shutdown_all();
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "replica: " << e.what() << std::endl;
        return 2;
    }
}
