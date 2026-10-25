#pragma once

#include "kv_limits.h"
#include "wal.h"

#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

class HistoryError : public std::runtime_error {
public:
    HistoryError(std::string code, const std::string& message)
        : std::runtime_error(message), code(std::move(code)) {}
    std::string code;
};

bool same_record(const WalRecord& a, const WalRecord& b);

class KVStore {
public:
    explicit KVStore(const std::filesystem::path& path);
    std::optional<std::string> get(const std::string& key) const;
    std::uint64_t last_sequence() const { return history.size(); }
    const WalRecord& record(std::uint64_t sequence) const;
    bool append(const WalRecord& record);
    void verify(const WalRecord& record) const;
    bool failed() const { return poisoned; }
    const RecoveryInfo& recovery_info() const { return recovered; }

private:
    void apply(const WalRecord& record);
    WriteAheadLog wal;
    std::vector<WalRecord> history;
    std::unordered_map<std::string, std::string> values;
    RecoveryInfo recovered;
    bool poisoned = false;
};
