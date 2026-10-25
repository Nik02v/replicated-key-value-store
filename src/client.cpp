#include "cli.h"
#include "protocol.h"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

json make_request(const std::vector<std::string>& words)
{
    if (words.empty())
        throw std::invalid_argument("Ocekujem get/put/delete");
    std::string op = words[0];
    if (op == "get") op = "GET";
    if (op == "put") op = "PUT";
    if (op == "delete") op = "DELETE";
    if ((op != "PUT" && op != "GET" && op != "DELETE") ||
        words.size() != (op == "PUT" ? 3U : 2U))
        throw std::invalid_argument("Komande: get key, put key value, delete key");
    json request = {{"type", "REQUEST"}, {"op", op}, {"key", words[1]}};
    if (op == "PUT")
        request["value"] = words[2];
    parse_request(request);
    return request;
}

json call(int port, int timeout, const json& request)
{
    bool sending = false;
    try {
        Socket socket = connect_to("127.0.0.1", port);
        sending = true;
        send_message(socket, request);
        json response;
        if (!receive_message(socket, response, timeout))
            throw std::runtime_error("Veza prekinuta pre odgovora");
        response.at("status").get<std::string>();
        return response;
    } catch (const std::exception& e) {
        bool uncertain = sending && request.at("op") != "GET";
        return {{"status", uncertain ? "UNKNOWN" : "UNAVAILABLE"},
                {"source", "client"}, {"error", e.what()}};
    }
}

int result_code(const json& response)
{
    auto status = response.at("status").get<std::string>();
    if (status == "OK" || status == "VALUE" || status == "NOT_FOUND")
        return 0;
    if (status == "UNAVAILABLE")
        return 1;
    if (status == "UNKNOWN")
        return 2;
    return 3;
}

int main(int argc, char** argv)
{
    try {
        int port = 9000, timeout = 5000;
        std::vector<std::string> words;
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "--port") port = cli_int(next_arg(i, argc, argv), 1, 65535);
            else if (arg == "--timeout-ms") timeout = cli_int(next_arg(i, argc, argv), 100, 60000);
            else if (arg == "--help") {
                std::cout << "kv_client [--port 9000] [--timeout-ms 5000] put key value|get key|delete key\n"
                             "Bez komande: interaktivni unos sa navodnicima, quit za kraj.\n";
                return 0;
            } else {
                for (; i < argc; ++i)
                    words.emplace_back(argv[i]);
                break;
            }
        }
        Network network;
        if (!words.empty()) {
            auto response = call(port, timeout, make_request(words));
            std::cout << response.dump() << std::endl;
            return result_code(response);
        }
        std::string line;
        while (std::getline(std::cin, line)) {
            try {
                std::istringstream in(line);
                words.clear();
                std::string word;
                while (true) {
                    in >> std::ws;
                    if (in.eof())
                        break;
                    if (!(in >> std::quoted(word)))
                        throw std::invalid_argument("Neispravan unos");
                    words.push_back(word);
                }
                if (words.size() == 1 && words[0] == "quit")
                    break;
                if (!words.empty())
                    std::cout << call(port, timeout, make_request(words)).dump() << std::endl;
            } catch (const std::exception& e) {
                std::cout << json{{"status", "INVALID"}, {"error", e.what()}}.dump() << std::endl;
            }
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "client: " << e.what() << std::endl;
        return 3;
    }
}
