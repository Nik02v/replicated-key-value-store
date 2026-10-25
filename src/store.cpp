#include "store.h"

#include <limits>

bool same_record(const WalRecord& a, const WalRecord& b)
{
    return a.sequence == b.sequence && a.operation == b.operation && a.key == b.key && a.value == b.value;
}

KVStore::KVStore(const std::filesystem::path& path) : wal(path)
{
    recovered = wal.recover([this](const WalRecord& entry) {
        history.push_back(entry);
        apply(entry);
    });
    wal.sync();
}

void KVStore::apply(const WalRecord& entry)
{
    if (entry.operation == Operation::PUT)
        values[entry.key] = entry.value;
    else
        values.erase(entry.key);
}

std::optional<std::string> KVStore::get(const std::string& key) const
{
    check_key(key);
    if (poisoned)
        throw std::runtime_error("Store zahteva restart posle greske WAL-a");
    auto it = values.find(key);
    if (it == values.end())
        return std::nullopt;
    return it->second;
}

const WalRecord& KVStore::record(std::uint64_t sequence) const
{
    if (sequence == 0 || sequence > history.size())
        throw HistoryError("GAP", "Nepostojeci redni broj u istoriji");
    return history[static_cast<std::size_t>(sequence - 1)];
}

void KVStore::verify(const WalRecord& entry) const
{
    if (!same_record(record(entry.sequence), entry))
        throw HistoryError("CONFLICT", "Konflikt u istoriji na seq=" + std::to_string(entry.sequence));
}

bool KVStore::append(const WalRecord& entry)
{
    if (poisoned)
        throw std::runtime_error("Store zahteva restart");
    check_key(entry.key);
    check_value(entry.value);
    if ((entry.operation != Operation::PUT && entry.operation != Operation::ERASE) ||
        (entry.operation == Operation::ERASE && !entry.value.empty()) || entry.sequence == 0)
        throw std::invalid_argument("Neispravan zapis");
    if (entry.sequence <= history.size()) {
        verify(entry);
        return false;
    }
    if (history.size() == std::numeric_limits<std::uint64_t>::max() || entry.sequence != history.size() + 1)
        throw HistoryError("GAP", "Replika ocekuje sledeci redni broj");
    try {
        wal.append(entry);
        wal.sync();
        history.push_back(entry);
        apply(entry);
    } catch (...) {
        poisoned = true;
        throw;
    }
    return true;
}
