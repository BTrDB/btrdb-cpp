#ifndef BTRDB_BTRDB_H_
#define BTRDB_BTRDB_H_

#include <cstdint>
#include <mutex>
#include <grpc++/grpc++.h>

#include "btrdb.grpc.pb.h"
#include "btrdb_endpoint.h"
#include "btrdb_mash.h"
#include "btrdb_stream.h"
#include "btrdb_util.h"

namespace btrdb {
    class Endpoint;
    class Stream;

    class BTrDB : public std::enable_shared_from_this<BTrDB> {
    public:
        friend class Stream;

        static const constexpr std::int64_t MAX_TIME = (INT64_C(48) << 56) - 1;
        static const constexpr std::int64_t MIN_TIME = -(INT64_C(16) << 56);
        static const constexpr std::uint8_t MAX_PWE = 63;

        ~BTrDB();
        static std::shared_ptr<BTrDB> connect(std::function<void(grpc::ClientContext*)> ctx, const std::vector<std::string>& endpoints);
        static void connectAsync(std::function<void(grpc::ClientContext*)> ctx, const std::vector<std::string>& endpoints, std::function<void(std::shared_ptr<BTrDB> btrdb)> on_done);
        std::unique_ptr<Stream> streamFromUUID(const void* uuid);

        /* Asynchronous API */
        Status listCollectionsAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, const std::vector<std::string>&)> on_data, const std::string& prefix);
        Status lookupStreamsAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<std::unique_ptr<Stream>>&)> on_data, const std::string& collection, bool is_prefix, const std::map<std::string, std::pair<std::string, bool>>& tags, const std::map<std::string, std::pair<std::string, bool>>& annotations);

        /* Synchronous API */
        Status listCollections(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<std::string>&)> on_data, const std::string& prefix);
        Status lookupStreams(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<std::unique_ptr<Stream>>&)> on_data, const std::string& collection, bool is_prefix, const std::map<std::string, std::pair<std::string, bool>>& tags, const std::map<std::string, std::pair<std::string, bool>>& annotations);

        /* Simplified synchronous API for those who don't want to deal with callbacks. */
        Status listCollections(std::function<void(grpc::ClientContext*)> ctx, std::vector<std::string>* collections, const std::string& prefix);
        Status lookupStreams(std::function<void(grpc::ClientContext*)> ctx, std::vector<std::unique_ptr<Stream>>* result, const std::string& collection, bool is_prefix, const std::map<std::string, std::pair<std::string, bool>>& tags, const std::map<std::string, std::pair<std::string, bool>>& annotations);

        Status create(std::function<void(grpc::ClientContext*)> ctx, const void* uuid,
                      const std::string& collection,
                      const std::map<std::string, std::string>& tags,
                      const std::map<std::string, std::string>& annotations);

        grpc::CompletionQueue* completion_queue;

    private:
        BTrDB(const MASH& activeMash, const std::vector<std::string>& bootstraps);
        static std::unique_ptr<grpcinterface::Mash> rawConnect(std::function<void(grpc::ClientContext*)> ctx, const std::vector<std::string>& endpoints);
        Status anyEndpoint(std::function<void(grpc::ClientContext*)> ctx, std::shared_ptr<Endpoint>* endpoint);
        Status endpointFor(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, std::shared_ptr<Endpoint>* endpoint);

        void asyncAnyEndpoint(std::function<void(grpc::ClientContext*)> ctx, std::function<void(Status, std::shared_ptr<Endpoint>&)> on_done);
        void asyncEndpointFor(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, std::function<void(Status, std::shared_ptr<Endpoint>&)> on_done);

        void asyncAnyEndpointOrError(std::function<void(grpc::ClientContext*)> ctx, std::function<void(Status, std::shared_ptr<Endpoint>&)> on_done);
        bool handleEndpointStatus(const Status& status);

        Status listCollectionsAsyncHelper(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, const std::vector<std::string>&)> on_data, const std::string& prefix, std::string from);

        static void eventLoop(grpc::CompletionQueue* completion_queue);
        static void asyncConnectEventLoop(std::function<void(grpc::ClientContext*)> ctx, const std::vector<std::string> endpoints, std::function<void(std::shared_ptr<BTrDB>)> on_done);

        MASH activeMash_;
        std::map<std::uint32_t, std::shared_ptr<Endpoint>> epcache_;
        std::mutex epcache_lock_;
        std::vector<std::string> bootstraps_;
    };

    void default_ctx(grpc::ClientContext* context);
    void connect_ctx(grpc::ClientContext* context);
}

#endif // BTRDB_BTRDB_H_
