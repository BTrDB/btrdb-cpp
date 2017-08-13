#include "btrdb_mash.h"
#include <sstream>
#include "btrdb_util.h"

/* Based on the implementation in https://en.wikipedia.org/wiki/MurmurHash */
std::uint32_t murmur3(const void* data, std::size_t len, std::uint32_t seed=1) {
    using std::uint8_t;
    using std::uint32_t;
    using std::size_t;

    const uint8_t* key = reinterpret_cast<const uint8_t*>(data);

    uint32_t h = seed;
    if (len > 3) {
        for (size_t i = 0; i < len - 3; i++) {
            uint32_t k = ((uint32_t) key[i]) |
                         (((uint32_t) key[i+1]) << 8) |
                         (((uint32_t) key[i+2]) << 16) |
                         (((uint32_t) key[i+3]) << 24);
            k *= 0xcc9e2d51;
            k = (k << 15) | (k >> 17);
            k *= 0x1b873593;
            h ^= k;
            h = (h << 13) | (h >> 19);
            h = (h * 5) + 0xe6546b64;
        }
    }

    /*
     * This should never happen when hashing a UUID, but I have it here for
     * good measure.
     */
    if ((len & 3) != 0) {
        size_t i = (len & 3);
        uint32_t k = 0;
        const uint8_t* ptr = &key[len - 1];
        do {
            k <<= 8;
            k |= *ptr;
            ptr--;
        } while (--i);
        k *= 0xcc9e2d51;
        k = (k << 15) | (k >> 17);
        k *= 0x1b873593;
        h ^= k;
    }

    h ^= len;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

namespace btrdb {
    MASH::MASH(const grpcinterface::Mash& mash) : m_(mash) {
        this->precalculate();
    }

    void MASH::setProtoMash(const grpcinterface::Mash& mash) {
        this->m_ = mash;
        this->eps_.clear();
        this->precalculate();
    }

    bool MASH::endpointFor(const void* uuid, std::vector<std::string>* addrs, uint32_t* hash) {
        std::uint32_t hsh = murmur3(uuid, UUID_NUM_BYTES);
        for (const struct endpoint& e : this->eps_) {
            if (e.start <= hsh && e.end > hsh) {
                *addrs = e.grpc;
                *hash = e.hash;
                return true;
            }
        }
        return false;
    }

    void MASH::precalculate() {
        const auto& members = this->m_.members();
        int num_members = members.size();
        this->eps_.resize(num_members);
        for (int i = 0; i != num_members; i++) {
            const grpcinterface::Member& mbr = members[i];
            if (mbr.in() && mbr.up() && mbr.start() != mbr.end()) {
                struct endpoint& ep = this->eps_[i];
                ep.start = mbr.start();
                ep.end = mbr.end();
                ep.hash = mbr.hash();
                ep.grpc = split_string(mbr.grpcendpoints(), ';');
            }
        }
    }
}
