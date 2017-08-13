#ifndef BTRDB_STREAM_H_
#define BTRDB_STREAM_H_

#include <cstdint>

#include "btrdb_util.h"

namespace btrdb {
    class BTrDB;
    class Endpoint;

    class Stream {
    public:
        friend class BTrDB;

        Stream(const std::shared_ptr<BTrDB>& b, const void* uuid);
        Stream(const std::shared_ptr<BTrDB>& b, const grpcinterface::StreamDescriptor& descriptor);
        Status exists(std::function<void(grpc::ClientContext*)> ctx, bool* result);
        Status collection(std::function<void(grpc::ClientContext*)> ctx, const std::string** collection_ptr);
        Status tags(std::function<void(grpc::ClientContext*)> ctx, const std::map<std::string, std::string>** tags_ptr);
        Status annotations(std::function<void(grpc::ClientContext*)> ctx, const std::map<std::string, std::string>** annotations_ptr, std::uint64_t* annotations_ver);
        Status cachedAnnotations(std::function<void(grpc::ClientContext*)> ctx, const std::map<std::string, std::string>** annotations_ptr, std::uint64_t* annotations_ver);
        const void* UUID();
        Status version(std::function<void(grpc::ClientContext*)> ctx, std::uint64_t* version_ptr);

        /* Asynchronous API */
        Status rawValuesAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct RawPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint64_t version = 0);
        Status alignedWindowsAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint8_t pointwidth, std::uint64_t version = 0);
        Status windowsAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint64_t width, std::uint8_t depth, std::uint64_t version = 0);
        Status changesAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct ChangedRange>&, std::uint64_t)> on_data, std::uint64_t from_version, std::uint64_t to_version, std::uint8_t resolution = 0);
        Status nearestAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(Status, const RawPoint&, std::uint64_t)> on_data, std::int64_t timestamp, bool backward, std::uint64_t version = 0);

        /* Synchronous API */
        Status insert(std::function<void(grpc::ClientContext*)> ctx, std::uint64_t* version_ptr, std::vector<struct RawPoint>::const_iterator data_start, std::vector<struct RawPoint>::const_iterator data_end, bool sync = false);
        Status deleteRange(std::function<void(grpc::ClientContext*)> ctx, std::uint64_t* version_ptr, std::int64_t start, std::int64_t end);
        Status obliterate(std::function<void(grpc::ClientContext*)> ctx);
        Status rawValues(std::function<void(grpc::ClientContext*)> ctx, std::vector<struct RawPoint>* result, std::uint64_t* version_ptr, std::int64_t start, std::int64_t end, std::uint64_t version = 0);
        Status alignedWindows(std::function<void(grpc::ClientContext*)> ctx, std::vector<struct StatisticalPoint>* result, std::uint64_t* version_ptr, std::int64_t start, std::int64_t end, std::uint8_t pointwidth, std::uint64_t version = 0);
        Status windows(std::function<void(grpc::ClientContext*)> ctx, std::vector<struct StatisticalPoint>* result, std::uint64_t* version_ptr, std::int64_t start, std::int64_t end, std::uint64_t width, std::uint8_t depth, std::uint64_t version = 0);
        Status changes(std::function<void(grpc::ClientContext*)> ctx, std::vector<struct ChangedRange>* result, std::uint64_t* version_ptr, std::uint64_t from_version, std::uint64_t to_version, std::uint8_t resolution = 0);
        Status nearest(std::function<void(grpc::ClientContext*)> ctx, RawPoint* result, std::uint64_t* version_ptr, std::int64_t timestamp, bool backward, std::uint64_t version = 0);

        /* Generalized synchronous API for someone who is OK with dealing with callbacks. */
        Status rawValues(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct RawPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint64_t version = 0);
        Status alignedWindows(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint8_t pointwidth, std::uint64_t version = 0);
        Status windows(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint64_t width, std::uint8_t depth, std::uint64_t version = 0);
        Status changes(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct ChangedRange>&, std::uint64_t)> on_data, std::uint64_t from_version, std::uint64_t to_version, std::uint8_t resolution = 0);

    private:
        Status refreshMetadata(std::function<void(grpc::ClientContext*)> ctx);
        void updateFromDescriptor(const grpcinterface::StreamDescriptor& descriptor);

        std::shared_ptr<BTrDB> b_;
        char uuid_[16];
        bool known_to_exist_;

        bool has_tags_;
        std::map<std::string, std::string> tags_;

        bool has_annotations_;
        std::map<std::string, std::string> annotations_;
        std::uint64_t annotations_version_;

        bool has_collection_;
        std::string collection_;
    };
}

#endif // BTRDB_STREAM_H_
