#ifndef BTRDB_MASH_H_
#define BTRDB_MASH_H_

#include <cstdint>
#include "btrdb.pb.h"

namespace btrdb {
    class MASH {
    public:
        MASH(const grpcinterface::Mash& mash);
        void setProtoMash(const grpcinterface::Mash& mash);
        bool endpointFor(const void* uuid, std::vector<std::string>* addrs, uint32_t* hash = nullptr);
    private:
        void precalculate();

        struct endpoint {
            std::int64_t start;
            std::int64_t end;
            std::uint32_t hash;
            std::vector<std::string> grpc;
        };

        grpcinterface::Mash m_;
        std::vector<struct endpoint> eps_;
    };
}

#endif //BTRDB_MASH_H_
