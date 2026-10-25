#pragma once

#include "json.hpp"
#include "net.h"
#include "store.h"

using json = nlohmann::json;
constexpr std::uint32_t max_message = 1024 * 1024;

void send_message(const Socket& socket, const json& message);
bool receive_message(const Socket& socket, json& message, int timeout_ms = 5000);
std::uint64_t read_sequence(const json& object, const char* key, bool allow_zero = false);
json to_json(const WalRecord& record);
WalRecord parse_record(const json& value);

struct Request {
    std::string op;
    std::string key;
    std::string value;
};
Request parse_request(const json& value);
