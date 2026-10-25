#include "cli.h"
#include "protocol.h"
#include "signals.h"

#include <array>
#include <algorithm>
#include <deque>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

using Clock = std::chrono::steady_clock;

struct Options {
    std::string path;
    int port = 9000;
    int replica1 = 9101, replica2 = 9102;
    int heartbeat_ms = 300;
    int timeout_ms = 1500;
    int write_timeout_ms = 1500;
    int retry_ms = 250;
};

struct Peer {
    int port = 0;
    std::string id;
    std::unique_ptr<Socket> socket;
    std::thread reader;
    std::uint64_t generation = 0;
    bool hello = false;
    bool announced = false;
    std::uint64_t reported = 0;
    std::uint64_t matched = 0;
    std::string waiting;
    std::uint64_t waiting_seq = 0;
    std::uint64_t nonce = 0;
    std::optional<std::uint64_t> ping;
    Clock::time_point command_deadline;
    Clock::time_point ping_deadline;
    Clock::time_point next_ping;
    Clock::time_point next_connect = Clock::now();
};

struct Client {
    explicit Client(Socket socket) : socket(std::move(socket)) {}
    Socket socket;
    std::thread reader;
};

struct Event {
    bool from_peer;
    int index;
    std::uint64_t generation;
    json message;
    std::string error;
};

struct Pending {
    std::uint64_t sequence;
    int client;
    Clock::time_point deadline;
};

class Leader {
public:
    explicit Leader(Options options) : options(std::move(options)), store(this->options.path)
    {
        peers[0].port = this->options.replica1;
        peers[1].port = this->options.replica2;
    }

    ~Leader()
    {
        for (auto& pair : clients)
            pair.second->socket.shutdown_all();
        for (auto& peer : peers) {
            if (peer.socket)
                peer.socket->shutdown_all();
        }
        for (auto& pair : clients) {
            if (pair.second->reader.joinable())
                pair.second->reader.join();
        }
        for (auto& peer : peers) {
            if (peer.reader.joinable())
                peer.reader.join();
        }
    }

    void run()
    {
        std::cout << "RECOVERING seq=" << store.last_sequence()
                  << " truncated=" << store.recovery_info().truncated_bytes << std::endl;
        while (!stop_requested.load()) {
            if (listener) {
                if (wait_readable(*listener, 10))
                    accept_client();
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }

            std::deque<Event> batch;
            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                batch.swap(events);
            }
            for (const auto& event : batch) {
                if (event.from_peer)
                    handle_peer(event);
                else
                    handle_client(event);
                finish_pending();
            }
            tick();
            finish_pending();
            open_service_if_recovered();
        }
        if (pending && pending->client >= 0)
            reply(pending->client, {{"status", "UNKNOWN"}, {"seq", pending->sequence}});
    }

private:
    void push(Event event)
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        events.push_back(std::move(event));
    }

    void read_peer(int index, std::uint64_t generation, Socket* socket)
    {
        try {
            json message;
            while (receive_message(*socket, message, options.timeout_ms))
                push({true, index, generation, std::move(message), ""});
            push({true, index, generation, {}, "EOF"});
        } catch (const std::exception& e) {
            push({true, index, generation, {}, e.what()});
        }
    }

    void read_client(int id, Client* client)
    {
        try {
            json message;
            if (!receive_message(client->socket, message, options.timeout_ms))
                throw std::runtime_error("EOF pre zahteva");
            push({false, id, 0, std::move(message), ""});
        } catch (const std::exception& e) {
            push({false, id, 0, {}, e.what()});
        }
    }

    void accept_client()
    {
        Socket socket = accept_one(*listener);
        if (clients.size() >= 32) {
            socket.shutdown_all();
            return;
        }
        int id = next_client++;
        auto client = std::make_unique<Client>(std::move(socket));
        Client* ptr = client.get();
        clients.emplace(id, std::move(client));
        ptr->reader = std::thread(&Leader::read_client, this, id, ptr);
    }

    void reply(int id, const json& message)
    {
        auto it = clients.find(id);
        if (it == clients.end())
            return;
        auto& client = *it->second;
        try {
            send_message(client.socket, message);
            client.socket.shutdown_write();
        } catch (const std::exception& e) {
            std::cout << "CLIENT_DISCONNECTED " << e.what() << std::endl;
        }
        if (client.reader.joinable())
            client.reader.join();
        clients.erase(it);
    }

    bool ready(const Peer& peer) const
    {
        return peer.socket && peer.hello && peer.waiting.empty() && peer.matched == store.last_sequence();
    }

    bool has_ready_replica() const
    {
        for (const auto& peer : peers) {
            if (ready(peer))
                return true;
        }
        return false;
    }

    void handle_client(const Event& event)
    {
        if (!clients.count(event.index))
            return;
        Request request;
        try {
            if (!event.error.empty())
                throw std::invalid_argument(event.error);
            request = parse_request(event.message);
        } catch (const std::exception& e) {
            reply(event.index, {{"status", "INVALID"}, {"error", std::string(e.what()).substr(0, 1024)}});
            return;
        }
        if (pending) {
            reply(event.index, {{"status", "UNAVAILABLE"}, {"error", "Neresen prethodni upis"}});
            return;
        }
        if (request.op == "GET") {
            auto value = store.get(request.key);
            if (value)
                reply(event.index, {{"status", "VALUE"}, {"value", *value}});
            else
                reply(event.index, {{"status", "NOT_FOUND"}});
            return;
        }
        if (!has_ready_replica()) {
            reply(event.index, {{"status", "UNAVAILABLE"}, {"error", "Nema sinhronizovane replike"}});
            return;
        }
        if (store.last_sequence() == std::numeric_limits<std::uint64_t>::max()) {
            reply(event.index, {{"status", "UNAVAILABLE"}, {"error", "Potroseni redni brojevi"}});
            return;
        }

        WalRecord entry{store.last_sequence() + 1,
                        request.op == "PUT" ? Operation::PUT : Operation::ERASE, request.key, request.value};
        try {
            store.append(entry);
        } catch (...) {
            reply(event.index, {{"status", "UNKNOWN"}, {"seq", entry.sequence}, {"error", "Greska lokalnog WAL-a"}});
            throw;
        }
        pending = Pending{entry.sequence, event.index, Clock::now() + std::chrono::milliseconds(options.write_timeout_ms)};
        std::cout << "LOCAL_DURABLE seq=" << entry.sequence << std::endl;
        for (int i = 0; i < 2; ++i)
            advance(i);
    }

    void drop_peer(int index, const std::string& reason)
    {
        auto& peer = peers[index];
        if (peer.socket) {
            std::cout << "PEER_DOWN port=" << peer.port << " reason=" << reason << std::endl;
            peer.socket->shutdown_all();
        }
        if (peer.reader.joinable())
            peer.reader.join();
        peer.socket.reset();
        peer.hello = false;
        peer.announced = false;
        peer.matched = 0;
        peer.waiting.clear();
        peer.ping.reset();
        peer.next_connect = Clock::now() + std::chrono::milliseconds(options.retry_ms);
    }

    void connect_peer(int index)
    {
        auto& peer = peers[index];
        try {
            peer.socket = std::make_unique<Socket>(connect_to("127.0.0.1", peer.port, std::min(options.timeout_ms, 100)));
            ++peer.generation;
            peer.waiting = "HELLO";
            peer.command_deadline = Clock::now() + std::chrono::milliseconds(options.timeout_ms);
            peer.reader = std::thread(&Leader::read_peer, this, index, peer.generation, peer.socket.get());
        } catch (const std::exception& e) {
            drop_peer(index, e.what());
        }
    }

    void advance(int index)
    {
        auto& peer = peers[index];
        if (!peer.socket || !peer.hello || !peer.waiting.empty())
            return;
        if (peer.matched == store.last_sequence()) {
            if (!peer.announced) {
                std::cout << "SYNCED port=" << peer.port << " id=" << peer.id
                          << " seq=" << peer.matched << std::endl;
                peer.announced = true;
            }
            return;
        }
        auto sequence = peer.matched + 1;
        peer.waiting = sequence <= peer.reported ? "CHECKED" : "ACK";
        peer.waiting_seq = sequence;
        peer.announced = false;
        peer.command_deadline = Clock::now() + std::chrono::milliseconds(options.timeout_ms);
        json message = {{"type", sequence <= peer.reported ? "CHECK" : "APPEND"},
                        {"record", to_json(store.record(sequence))}};
        try {
            send_message(*peer.socket, message);
        } catch (const std::exception& e) {
            drop_peer(index, e.what());
        }
    }

    void handle_peer(const Event& event)
    {
        auto& peer = peers[event.index];
        if (!peer.socket || event.generation != peer.generation)
            return;
        if (!event.error.empty()) {
            drop_peer(event.index, event.error);
            return;
        }
        try {
            const auto& msg = event.message;
            std::string type = msg.at("type").get<std::string>();
            if (type == "HELLO" && !peer.hello) {
                if (peer.waiting != "HELLO")
                    throw std::invalid_argument("Neocekivan HELLO");
                peer.id = msg.at("replica_id").get<std::string>();
                if (peer.id.empty() || peer.id.size() > 64)
                    throw std::invalid_argument("Neispravan ID replike");
                for (const auto& other : peers) {
                    if (&other != &peer && other.socket && other.hello && other.id == peer.id)
                        throw HistoryError("CONFLICT", "Dve replike imaju isti ID");
                }
                peer.reported = read_sequence(msg, "last_seq", true);
                if (peer.reported > store.last_sequence())
                    throw HistoryError("CONFLICT", "Replika je ispred leaderove istorije");
                peer.matched = 0;
                peer.hello = true;
                peer.waiting.clear();
                peer.next_ping = Clock::now();
                std::cout << "HELLO port=" << peer.port << " last_seq=" << peer.reported << std::endl;
                advance(event.index);
            } else if (type == "PONG" && peer.hello) {
                auto nonce = read_sequence(msg, "nonce");
                if (peer.ping && *peer.ping == nonce)
                    peer.ping.reset();
            } else if ((type == "ACK" || type == "CHECKED") && peer.hello) {
                auto sequence = read_sequence(msg, "seq");
                if (peer.waiting != type || peer.waiting_seq != sequence)
                    throw std::invalid_argument("Potvrda ne odgovara konkretnom zapisu");
                peer.matched = sequence;
                peer.waiting.clear();
                advance(event.index);
            } else if (type == "NACK") {
                std::string code = msg.at("code").get<std::string>();
                std::string error = msg.at("error").get<std::string>();
                if (code == "CONFLICT" || code == "GAP")
                    throw HistoryError(code, "Replika odbija istoriju: " + error);
                throw std::invalid_argument("Replika odbija poruku: " + error);
            } else {
                throw std::invalid_argument("Neocekivana poruka replike");
            }
        } catch (const HistoryError&) {
            throw;
        } catch (const std::exception& e) {
            drop_peer(event.index, e.what());
        }
    }

    void finish_pending()
    {
        if (!pending)
            return;
        for (const auto& peer : peers) {
            if (peer.socket && peer.hello && peer.matched >= pending->sequence) {
                auto sequence = pending->sequence;
                if (pending->client >= 0)
                    reply(pending->client, {{"status", "OK"}, {"seq", sequence}});
                pending.reset();
                std::cout << "COMMITTED seq=" << sequence << std::endl;
                return;
            }
        }
        if (pending->client >= 0 && Clock::now() >= pending->deadline) {
            reply(pending->client, {{"status", "UNKNOWN"}, {"seq", pending->sequence}});
            pending->client = -1;
            std::cout << "UNKNOWN seq=" << pending->sequence << std::endl;
        }
    }

    void open_service_if_recovered()
    {
        if (!listener && has_ready_replica()) {
            listener = std::make_unique<Socket>(listen_local(options.port));
            std::cout << "LEADER_READY port=" << local_port(*listener)
                      << " seq=" << store.last_sequence() << std::endl;
        }
    }

    void tick()
    {
        for (int i = 0; i < 2; ++i) {
            auto& peer = peers[i];
            auto now = Clock::now();
            if (!peer.socket) {
                if (now >= peer.next_connect)
                    connect_peer(i);
                continue;
            }
            if ((!peer.waiting.empty() && now >= peer.command_deadline) ||
                (peer.ping && now >= peer.ping_deadline)) {
                drop_peer(i, "heartbeat/ACK timeout");
                continue;
            }
            advance(i);
            if (peer.socket && peer.hello && !peer.ping && now >= peer.next_ping) {
                peer.ping = ++peer.nonce;
                peer.ping_deadline = now + std::chrono::milliseconds(options.timeout_ms);
                peer.next_ping = now + std::chrono::milliseconds(options.heartbeat_ms);
                try {
                    send_message(*peer.socket, {{"type", "PING"}, {"nonce", *peer.ping}});
                } catch (const std::exception& e) {
                    drop_peer(i, e.what());
                }
            }
        }
    }

    Options options;
    KVStore store;
    std::array<Peer, 2> peers;
    std::unique_ptr<Socket> listener;
    std::unordered_map<int, std::unique_ptr<Client>> clients;
    int next_client = 0;
    std::optional<Pending> pending;
    std::mutex queue_mutex;
    std::deque<Event> events;
};

int main(int argc, char** argv)
{
    try {
        Options options;
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "--wal") options.path = next_arg(i, argc, argv);
            else if (arg == "--port") options.port = cli_int(next_arg(i, argc, argv), 0, 65535);
            else if (arg == "--replica1") options.replica1 = cli_int(next_arg(i, argc, argv), 1, 65535);
            else if (arg == "--replica2") options.replica2 = cli_int(next_arg(i, argc, argv), 1, 65535);
            else if (arg == "--heartbeat-ms") options.heartbeat_ms = cli_int(next_arg(i, argc, argv), 10, 60000);
            else if (arg == "--timeout-ms") options.timeout_ms = cli_int(next_arg(i, argc, argv), 100, 60000);
            else if (arg == "--write-timeout-ms") options.write_timeout_ms = cli_int(next_arg(i, argc, argv), 50, 60000);
            else if (arg == "--retry-ms") options.retry_ms = cli_int(next_arg(i, argc, argv), 10, 60000);
            else if (arg == "--help") {
                std::cout << "kv_leader --wal leader.wal [--port 9000] [--replica1 9101] [--replica2 9102]\n"
                             "  [--heartbeat-ms 300] [--timeout-ms 1500] [--write-timeout-ms 1500] [--retry-ms 250]\n";
                return 0;
            } else throw std::invalid_argument("Nepoznat argument: " + arg);
        }
        if (options.path.empty() || options.replica1 == options.replica2 || options.heartbeat_ms >= options.timeout_ms ||
            (options.port != 0 && (options.port == options.replica1 || options.port == options.replica2)))
            throw std::invalid_argument("Obavezan --wal, razliciti portovi i heartbeat kraci od timeout-a");
        install_signals();
        Network network;
        Leader leader(options);
        leader.run();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "leader: " << e.what() << std::endl;
        return 2;
    }
}
