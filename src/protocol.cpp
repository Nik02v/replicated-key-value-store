#include "protocol.h"

#include <cstring>
#include <stdexcept>

void send_message(const Socket& socket, const json& message)
{
    if (!message.is_object())
        throw std::invalid_argument("Poruka mora biti JSON objekat");
    std::string body = message.dump();
    if (body.size() > max_message)
        throw std::invalid_argument("Poruka je veca od 1 MiB");
    std::uint32_t length = htonl(static_cast<std::uint32_t>(body.size()));
    std::string frame(sizeof(length), '\0');
    std::memcpy(frame.data(), &length, sizeof(length));
    frame += body;
    send_all(socket, frame.data(), frame.size());
}

bool receive_message(const Socket& socket, json& message, int timeout_ms)
{
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    std::uint32_t length;
    if (!receive_all(socket, reinterpret_cast<char*>(&length), sizeof(length), deadline))
        return false;
    length = ntohl(length);
    if (length == 0 || length > max_message)
        throw std::invalid_argument("Neispravna duzina TCP poruke");
    std::string body(length, '\0');
    if (!receive_all(socket, body.data(), body.size(), deadline))
        throw std::runtime_error("Nedostaje telo TCP poruke");
    message = json::parse(body);
    if (!message.is_object())
        throw std::invalid_argument("Poruka nije JSON objekat");
    return true;
}

std::uint64_t read_sequence(const json& object, const char* key, bool allow_zero)
{
    const auto& value = object.at(key);
    if (!value.is_number_integer() || (!value.is_number_unsigned() && value.get<long long>() < 0))
        throw std::invalid_argument(std::string(key) + " mora biti nenegativan ceo broj");
    auto n = value.get<std::uint64_t>();
    if (n == 0 && !allow_zero)
        throw std::invalid_argument(std::string(key) + " mora biti pozitivan");
    return n;
}

json to_json(const WalRecord& entry)
{
    return {{"seq", entry.sequence}, {"op", entry.operation == Operation::PUT ? "PUT" : "DELETE"},
            {"key", entry.key}, {"value", entry.value}};
}

WalRecord parse_record(const json& value)
{
    WalRecord entry;
    entry.sequence = read_sequence(value, "seq");
    auto op = value.at("op").get<std::string>();
    if (op != "PUT" && op != "DELETE")
        throw std::invalid_argument("Nepoznata operacija zapisa");
    entry.operation = op == "PUT" ? Operation::PUT : Operation::ERASE;
    entry.key = value.at("key").get<std::string>();
    entry.value = value.at("value").get<std::string>();
    check_key(entry.key);
    check_value(entry.value);
    if (entry.operation == Operation::ERASE && !entry.value.empty())
        throw std::invalid_argument("DELETE zapis mora imati praznu vrednost");
    return entry;
}

Request parse_request(const json& value)
{
    if (value.at("type") != "REQUEST")
        throw std::invalid_argument("Ocekujem REQUEST");
    Request request;
    request.op = value.at("op").get<std::string>();
    if (request.op != "GET" && request.op != "PUT" && request.op != "DELETE")
        throw std::invalid_argument("Nepoznata klijentska operacija");
    request.key = value.at("key").get<std::string>();
    check_key(request.key);
    if (request.op == "PUT") {
        request.value = value.at("value").get<std::string>();
        check_value(request.value);
    }
    return request;
}
