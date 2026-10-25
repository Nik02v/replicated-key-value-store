#include "protocol.h"

#include <fstream>
#include <iostream>
#include <iterator>
#include <utility>

namespace fs = std::filesystem;
#define CHECK(expr) do { if (!(expr)) throw std::runtime_error( \
    std::string(__FILE__) + ":" + std::to_string(__LINE__) + " " #expr); } while (false)

fs::path test_root;
int next_number = 0;
fs::path next_file() { return test_root / (std::to_string(++next_number) + ".wal"); }

template<class F>
void rejected(F f)
{
    bool caught = false;
    try { f(); } catch (const std::exception&) { caught = true; }
    CHECK(caught);
}

std::string bytes(const fs::path& path)
{
    std::ifstream in(path, std::ios::binary);
    CHECK(in.good());
    return std::string(std::istreambuf_iterator<char>(in), {});
}

void write_bytes(const fs::path& path, const std::string& value)
{
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(value.data(), static_cast<std::streamsize>(value.size()));
    out.close();
    CHECK(out.good());
}

void operations_and_recovery()
{
    auto path = next_file();
    {
        KVStore store(path);
        CHECK(store.last_sequence() == 0);
        CHECK(!store.get("missing"));
        CHECK(store.append({1, Operation::PUT, "key", "one"}));
        CHECK(store.append({2, Operation::PUT, "key", "two"}));
        CHECK(store.get("key") == "two");
        auto copy = store.get("key");
        *copy = "local";
        CHECK(store.get("key") == "two");
        CHECK(store.append({3, Operation::ERASE, "key", ""}));
        CHECK(store.append({4, Operation::ERASE, "missing", ""}));
        CHECK(!store.get("key"));
        store.append({5, Operation::PUT, "", ""});
        store.append({6, Operation::PUT, std::string("a\0b", 3), std::string("x\0y", 3)});
    }
    KVStore recovered(path);
    CHECK(recovered.last_sequence() == 6 && recovered.recovery_info().records == 6);
    CHECK(!recovered.get("key"));
    CHECK(recovered.get("") == "");
    CHECK(recovered.get(std::string("a\0b", 3)) == std::string("x\0y", 3));
    CHECK(recovered.append({7, Operation::PUT, "after", "restart"}));
}

void duplicate_gap_conflict()
{
    auto path = next_file();
    KVStore store(path);
    WalRecord first{1, Operation::PUT, "key", "one"};
    store.append(first);
    store.append({2, Operation::PUT, "key", "two"});
    auto length = fs::file_size(path);
    CHECK(!store.append(first));
    CHECK(store.get("key") == "two");
    CHECK(store.last_sequence() == 2 && fs::file_size(path) == length);
    rejected([&] { store.append({1, Operation::PUT, "key", "different"}); });
    rejected([&] { store.append({1, Operation::ERASE, "key", ""}); });
    rejected([&] { store.append({4, Operation::PUT, "gap", "value"}); });
    rejected([&] { store.verify({2, Operation::PUT, "other", "two"}); });
    CHECK(store.last_sequence() == 2 && !store.failed());
    store.append({3, Operation::ERASE, "key", ""});
    CHECK(!store.get("key"));
}

void validation()
{
    KVStore store(next_file());
    rejected([&] { store.append({0, Operation::PUT, "key", "value"}); });
    rejected([&] { store.append({1, Operation::PUT, std::string(257, 'k'), "value"}); });
    rejected([&] { store.append({1, Operation::PUT, "key", std::string(4097, 'v')}); });
    rejected([&] { store.append({1, Operation::ERASE, "key", "nonempty"}); });
    CHECK(store.last_sequence() == 0 && !store.failed());
    store.append({1, Operation::PUT, std::string(256, 'k'), std::string(4096, 'v')});
    CHECK(store.get(std::string(256, 'k')) == std::string(4096, 'v'));
}

void wire_validation()
{
    WalRecord entry{1, Operation::PUT, "key", "value"};
    CHECK(same_record(entry, parse_record(to_json(entry))));
    for (json number : {json(-1), json(1.5), json(true), json("1"), json(0)}) {
        rejected([&] { read_sequence(json{{"seq", number}}, "seq"); });
    }
    CHECK(read_sequence(json{{"seq", 0}}, "seq", true) == 0);
    CHECK(read_sequence(json{{"seq", 18446744073709551615ULL}}, "seq") == 18446744073709551615ULL);
    auto request = parse_request({{"type", "REQUEST"}, {"op", "PUT"}, {"key", "a"}, {"value", "b"}});
    CHECK(request.key == "a" && request.value == "b");
    rejected([] { parse_request({{"type", "REQUEST"}, {"op", "PUT"}, {"key", "a"}}); });
    rejected([] { parse_request({{"type", "REQUEST"}, {"op", "UNKNOWN"}, {"key", "a"}}); });
    rejected([] { parse_request({{"type", "REQUEST"}, {"op", "GET"}, {"key", 5}}); });
}

void prefix_verification()
{
    KVStore one(next_file()), two(next_file());
    one.append({1, Operation::PUT, "key", "one"});
    two.append({1, Operation::PUT, "key", "different"});
    one.append({2, Operation::PUT, "last", "same"});
    two.append({2, Operation::PUT, "last", "same"});
    CHECK(same_record(one.record(2), two.record(2)));
    rejected([&] { two.verify(one.record(1)); });
}

void incomplete_last_record()
{
    auto original = next_file();
    std::uint64_t first_length;
    {
        KVStore store(original);
        store.append({1, Operation::PUT, "key", "saved"});
        first_length = fs::file_size(original);
        store.append({2, Operation::PUT, "key", "new value"});
    }
    std::string data = bytes(original);
    for (std::size_t cut = 0; cut < data.size() - first_length; ++cut) {
        auto path = next_file();
        write_bytes(path, data.substr(0, first_length + cut));
        {
            KVStore store(path);
            CHECK(store.last_sequence() == 1);
            CHECK(store.recovery_info().truncated_bytes == cut);
            CHECK(store.get("key") == "saved");
            CHECK(fs::file_size(path) == first_length);
            store.append({2, Operation::PUT, "after", "append"});
        }
        KVStore again(path);
        CHECK(again.last_sequence() == 2 && again.get("after") == "append");
    }
}

void corruption_and_exclusive_open()
{
    auto path = next_file();
    {
        KVStore store(path);
        rejected([&] { KVStore second(path); });
        store.append({1, Operation::PUT, "key", "value"});
    }
    auto data = bytes(path);
    data.back() ^= 0x11;
    write_bytes(path, data);
    rejected([&] { KVStore corrupt(path); });
    CHECK(bytes(path) == data);
    auto gap = next_file();
    {
        WriteAheadLog wal(gap);
        wal.append({2, Operation::PUT, "key", "value"});
        wal.sync();
    }
    rejected([&] { KVStore corrupt(gap); });
}

int main()
{
    auto ticks = std::chrono::steady_clock::now().time_since_epoch().count();
    test_root = fs::current_path() / ("unit-data-" + std::to_string(ticks));
    if (!fs::create_directory(test_root))
        return 2;
    const std::pair<const char*, void(*)()> tests[] = {
        {"operations and recovery", operations_and_recovery},
        {"duplicates gaps conflicts", duplicate_gap_conflict}, {"limits", validation},
        {"wire validation", wire_validation}, {"full prefix verification", prefix_verification},
        {"incomplete last record", incomplete_last_record},
        {"corruption and exclusive WAL", corruption_and_exclusive_open}
    };
    int failed = 0;
    for (const auto& test : tests) {
        try {
            test.second();
            std::cout << "[OK] " << test.first << std::endl;
        } catch (const std::exception& e) {
            ++failed;
            std::cerr << "[FAIL] " << test.first << ": " << e.what() << std::endl;
        }
    }
    for (const auto& entry : fs::directory_iterator(test_root))
        fs::remove(entry.path());
    fs::remove(test_root);
    return failed ? 1 : 0;
}
