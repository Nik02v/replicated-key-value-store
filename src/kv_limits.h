#pragma once

#include <cstddef>
#include <stdexcept>
#include <string>

constexpr std::size_t max_key_size = 256;
constexpr std::size_t max_value_size = 4096;
inline void check_key(const std::string& key)
{
    if (key.size() > max_key_size)
        throw std::invalid_argument("Kljuc je duzi od 256 bajtova");
}
inline void check_value(const std::string& value)
{
    if (value.size() > max_value_size)
        throw std::invalid_argument("Vrednost je duza od 4096 bajtova");
}
