#include "btrdb.h"
#include <grpc++/grpc++.h>
#include "btrdb.pb.h"
#include <memory>
#include <random>
#include <string>
#include <thread>
#include "btrdb_stream.h"
#include "btrdb_mash.h"
#include "btrdb_util.h"

namespace btrdb {
    BTrDB::~BTrDB() {
        this->completion_queue->Shutdown();
        /*
         * completion_queue is deleted in the event loop, after Next() or
         * AsyncNext() returns false
         */
    }

    std::unique_ptr<grpcinterface::Mash> BTrDB::rawConnect(std::function<void(grpc::ClientContext*)> ctx, const std::vector<std::string>& endpoints) {
        for (const std::string& hostport : endpoints) {
            Endpoint ep;
            const std::vector<std::string> hpv = { hostport };
            gpr_timespec timeout;
            timeout.tv_sec = 10;
            timeout.tv_nsec = 0;
            bool success = ep.connectBlocking(timeout, hpv);
            if (!success) {
                continue;
            }
            grpcinterface::InfoResponse ir;
            Status stat = ep.info(ctx, &ir);
            if (stat.isError()) {
                continue;
            }
            // TODO disconnect ep
            if (ir.has_mash()) {
                return std::unique_ptr<grpcinterface::Mash>(ir.release_mash());
            }
        }
        return std::unique_ptr<grpcinterface::Mash>(nullptr);
    }

    std::shared_ptr<BTrDB> BTrDB::connect(std::function<void(grpc::ClientContext*)> ctx, const std::vector<std::string>& endpoints) {
        std::unique_ptr<grpcinterface::Mash> mash = BTrDB::rawConnect(ctx, endpoints);
        if (!mash) {
            return std::shared_ptr<BTrDB>(nullptr);
        }

        std::shared_ptr<BTrDB> b(new BTrDB(MASH(*mash), endpoints));
        std::thread event_loop(BTrDB::eventLoop, b->completion_queue);
        event_loop.detach();
        return b;
    }

    void BTrDB::connectAsync(std::function<void(grpc::ClientContext*)> ctx, const std::vector<std::string>& endpoints, std::function<void(std::shared_ptr<BTrDB> btrdb)> on_done) {
        std::thread event_loop(BTrDB::asyncConnectEventLoop, ctx, endpoints, on_done);
        event_loop.detach();
    }

    std::unique_ptr<Stream> BTrDB::streamFromUUID(const void* uuid) {
        return std::unique_ptr<Stream>(new Stream(shared_from_this(), uuid));
    }

    Status BTrDB::listCollectionsAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, const std::vector<std::string>&)> on_data, const std::string& prefix) {
        return this->listCollectionsAsyncHelper(ctx, on_data, prefix, prefix);
    }

    Status BTrDB::listCollectionsAsyncHelper(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, const std::vector<std::string>&)> on_data, const std::string& prefix, std::string from) {
        const constexpr std::uint64_t max_results = 2;

        this->asyncAnyEndpointOrError(ctx, [=](Status stat, std::shared_ptr<Endpoint> ep) {
            if (stat.isError()) {
                std::vector<std::string> dummy;
                on_data(true, stat, dummy);
                return;
            }

            auto data_callback = [=](Status status, std::vector<std::string>& new_collections) {
                if (new_collections.size() != max_results || status.isError()) {
                    on_data(true, status, new_collections);
                    return;
                }

                std::string new_from = new_collections.back();
                new_collections.pop_back();
                on_data(false, status, new_collections);


                status = this->listCollectionsAsyncHelper(ctx, on_data, prefix, new_from);
                if (status.isError()) {
                    on_data(true, status, std::vector<std::string>());
                }
            };

            ep->listCollectionsAsync(ctx, this->completion_queue, data_callback, prefix, from, max_results);
        });

        return Status();
    }

    Status BTrDB::listCollections(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<std::string>&)> on_data, const std::string& prefix) {
        const constexpr std::uint64_t max_results = 10;

        Status status;
        std::string from = prefix;
        std::vector<std::string>::size_type num_new_elements;

        bool final_query = false;
        while (!final_query) {
            std::vector<std::string> collections;
            do {
                std::shared_ptr<Endpoint> ep;
                status = this->anyEndpoint(ctx, &ep);
                if (status.isError()) {
                    continue;
                }
                auto num_elements = collections.size();
                status = ep->listCollections(ctx, prefix, from, max_results, &collections);
                num_new_elements = collections.size() - num_elements;
            } while (this->handleEndpointStatus(status));

            final_query = (num_new_elements != max_results) || status.isError();
            if (!final_query) {
                // Prepare for the next query
                from = collections.back();
                collections.pop_back();
            }

            on_data(final_query, status, collections);
        }

        return status;
    }

    Status BTrDB::lookupStreams(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<std::unique_ptr<Stream>>&)> on_data, const std::string& collection, bool is_prefix, const std::map<std::string, std::pair<std::string, bool>>& tags, const std::map<std::string, std::pair<std::string, bool>>& annotations) {
        std::function<Status(std::function<void(bool, Status, std::vector<std::unique_ptr<Stream>>&)>)> callback = [=](std::function<void(bool, Status, std::vector<std::unique_ptr<Stream>>&)> callback) {
            return this->lookupStreamsAsync(ctx, callback, collection, is_prefix, tags, annotations);
        };
        return async_to_sync(std::move(callback), on_data);
    }

    Status BTrDB::listCollections(std::function<void(grpc::ClientContext*)> ctx, std::vector<std::string>* collections, const std::string& prefix) {
        return this->listCollections(ctx, collect_worker(collections), prefix);
    }

    Status BTrDB::lookupStreams(std::function<void(grpc::ClientContext*)> ctx, std::vector<std::unique_ptr<Stream>>* result, const std::string& collection, bool is_prefix, const std::map<std::string, std::pair<std::string, bool>>& tags, const std::map<std::string, std::pair<std::string, bool>>& annotations) {
        return this->lookupStreams(ctx, collect_worker(result), collection, is_prefix, tags, annotations);
    }

    Status BTrDB::lookupStreamsAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<std::unique_ptr<Stream>>&)> on_data, const std::string& collection, bool is_prefix, const std::map<std::string, std::pair<std::string, bool>>& tags, const std::map<std::string, std::pair<std::string, bool>>& annotations) {
        this->asyncAnyEndpointOrError(ctx, [=](Status status, std::shared_ptr<Endpoint> ep) {
            if (status.isError()) {
                std::vector<std::unique_ptr<Stream>> dummy;
                on_data(true, status, dummy);
                return;
            }

            auto self = shared_from_this();
            ep->lookupStreamsAsync(ctx, this->completion_queue, [self, on_data](bool finished, Status status, std::vector<std::unique_ptr<Stream>>& streams) {
                for (std::unique_ptr<Stream>& stream : streams) {
                    stream->b_ = self;
                }
                on_data(finished, status, streams);
            }, collection, is_prefix, tags, annotations);
        });

        return Status();
    }

    Status BTrDB::create(std::function<void(grpc::ClientContext*)> ctx, const void* uuid,
        const std::string& collection,
        const std::map<std::string, std::string>& tags,
        const std::map<std::string, std::string>& annotations) {
        Status status;
        do {
            std::shared_ptr<Endpoint> ep;
            status = this->endpointFor(ctx, uuid, &ep);
            if (status.isError()) {
                continue;
            }
            status = ep->create(ctx, uuid, collection, tags, annotations);
        } while (this->handleEndpointStatus(status));

        return status;
    }

    BTrDB::BTrDB(const MASH& activeMash, const std::vector<std::string>& bootstraps)
        : activeMash_(activeMash), bootstraps_(bootstraps) {
        this->completion_queue = new grpc::CompletionQueue;
    }

    Status BTrDB::anyEndpoint(std::function<void(grpc::ClientContext*)> ctx, std::shared_ptr<Endpoint>* endpoint) {
        {
            std::lock_guard<std::mutex> lock(this->epcache_lock_);
            auto it = this->epcache_.begin();
            if (it != this->epcache_.end()) {
                // TODO: maybe we want to select a random endpoint from the cache?
                *endpoint = it->second;
                return Status();
            }
        }

        /* The cache is empty, so we need to connect. */
        char uuid[UUID_NUM_BYTES];
        std::random_device random;
        std::size_t i = 0;
        while (i != UUID_NUM_BYTES) {
            unsigned int data = random();
            std::size_t to_copy = UUID_NUM_BYTES - i;
            if (to_copy > sizeof(data)) {
                to_copy = sizeof(data);
            }

            char* data_ptr = reinterpret_cast<char*>(&data);
            std::copy(data_ptr, &data_ptr[to_copy], &uuid[i]);
            i += to_copy;
        }

        /*
         * We have filled UUID with random bytes. So it may not actually be a
         * valid UUID, but it should work fine anyway.
         */
        return this->endpointFor(ctx, uuid, endpoint);
    }

    Status BTrDB::endpointFor(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, std::shared_ptr<Endpoint>* endpoint) {
        std::uint32_t hash;
        std::vector<std::string> addrs;
        bool ok = this->activeMash_.endpointFor(uuid, &addrs, &hash);
        if (!ok) {
            // Cluster is degraded
            return Status::ClusterDegraded;
        }

        // Check if it's in the cache
        {
            std::lock_guard<std::mutex> lock(this->epcache_lock_);
            auto it = this->epcache_.find(hash);
            if (it != this->epcache_.end()) {
                *endpoint = it->second;
                return Status();
            }
        }

        // It's not in the cache, so we need to connect, trying each address
        for (std::string addr : addrs) {
            Endpoint* ep = new Endpoint;

            grpc::ClientContext context;
            ctx(&context);
            bool success = ep->connectBlocking(context.raw_deadline(), { addr });
            if (!success) {
                delete ep;
                continue;
            }

            // Try a simple operation to force connection
            grpcinterface::InfoResponse ir;
            Status stat = ep->info(connect_ctx, &ir);

            if (stat.isError()) {
                continue;
            }

            *endpoint = std::shared_ptr<Endpoint>(ep);
            {
                std::lock_guard<std::mutex> lock(this->epcache_lock_);
                this->epcache_[hash] = *endpoint;
            }
            return Status();
        }

        return Status::Disconnected;
    }

    struct async_endpoint_state {
        std::vector<std::string>::size_type reqs_left;
        bool delivered;
    };

    void BTrDB::asyncAnyEndpoint(std::function<void(grpc::ClientContext*)> ctx, std::function<void(Status, std::shared_ptr<Endpoint>&)> on_done) {
        std::shared_ptr<Endpoint> ep;
        {
            std::lock_guard<std::mutex> lock(this->epcache_lock_);
            auto it = this->epcache_.begin();
            if (it != this->epcache_.end()) {
                // TODO: maybe we want to select a random endpoint from the cache?
                ep = it->second;
            }
        }
        if (ep != nullptr) {
            on_done(Status(), ep);
            return;
        }

        /* The cache is empty, so we need to connect. */
        char uuid[UUID_NUM_BYTES];
        std::random_device random;
        std::size_t i = 0;
        while (i != UUID_NUM_BYTES) {
            unsigned int data = random();
            std::size_t to_copy = UUID_NUM_BYTES - i;
            if (to_copy > sizeof(data)) {
                to_copy = sizeof(data);
            }

            char* data_ptr = reinterpret_cast<char*>(&data);
            std::copy(data_ptr, &data_ptr[to_copy], &uuid[i]);
            i += to_copy;
        }

        /*
         * We have filled UUID with random bytes. So it may not actually be a
         * valid UUID, but it should work fine anyway.
         */
        return this->asyncEndpointFor(ctx, uuid, std::move(on_done));
    }

    void BTrDB::asyncEndpointFor(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, std::function<void(Status, std::shared_ptr<Endpoint>&)> on_done) {
        std::uint32_t hash;
        std::vector<std::string> addrs;
        bool ok = this->activeMash_.endpointFor(uuid, &addrs, &hash);
        if (!ok) {
            // Cluster is degraded
            std::shared_ptr<Endpoint> dummy;
            on_done(Status::ClusterDegraded, dummy);
            return;
        }

        // Check if it's in the cache
        std::shared_ptr<Endpoint> ep;

        {
            std::lock_guard<std::mutex> lock(this->epcache_lock_);
            auto it = this->epcache_.find(hash);
            if (it != this->epcache_.end()) {
                ep = it->second;
            }
        }

        if (ep != nullptr) {
            on_done(Status(), ep);
            return;
        }

        // It's not in the cache, so we need to connect, trying each address
        // We make the requests concurrently.
        struct async_endpoint_state* state = new struct async_endpoint_state;
        state->reqs_left = addrs.size();
        state->delivered = false;
        for (std::string addr : addrs) {
            Endpoint* endpoint = new Endpoint;
            grpc::ClientContext context;
            ctx(&context);
            bool success = endpoint->connectBlocking(context.raw_deadline(), { addr });
            if (!success) {
                delete endpoint;
                continue;
            }

            // Try a simple operation to force connection
            endpoint->infoAsync(connect_ctx, this->completion_queue, [=](Status stat, const grpcinterface::InfoResponse& response) {
                if (state->delivered || stat.isError()) {
                    delete endpoint;
                } else if (!state->delivered && !stat.isError()) {
                    std::shared_ptr<Endpoint> ep(endpoint);
                    state->delivered = true;
                    {
                        std::lock_guard<std::mutex> lock(this->epcache_lock_);
                        this->epcache_[hash] = ep;
                    }
                    on_done(stat, ep);
                }

                state->reqs_left--;
                if (state->reqs_left == 0) {
                    delete state;
                }
            });
        }
    }

    void BTrDB::asyncAnyEndpointOrError(std::function<void(grpc::ClientContext*)> ctx, std::function<void(Status, std::shared_ptr<Endpoint>&)> on_done) {
        this->asyncAnyEndpoint(ctx, [=](Status status, std::shared_ptr<Endpoint>& ep) {
            if (this->handleEndpointStatus(status)) {
                this->asyncAnyEndpointOrError(ctx, on_done);
            } else {
                on_done(status, ep);
            }
        });
    }

    bool BTrDB::handleEndpointStatus(const Status& status) {
        if (!status.isError()) {
            return false;
        }
        if (status.code() == 405) {
            // Wrong Endpoint
            return true;
        }
        return false;
    }

    void BTrDB::eventLoop(grpc::CompletionQueue* completion_queue) {
        void* tag;
        bool ok;
        while (completion_queue->Next(&tag, &ok)) {
            AsyncRequest* reqdata = reinterpret_cast<AsyncRequest*>(tag);
            if (ok) {
                if (reqdata->process_batch()) {
                    /* An error ocurred. */
                    delete reqdata;
                }
            } else {
                /* No more data for this RPC. */
                reqdata->end_request();
                delete reqdata;
            }
        }

        delete completion_queue;
    }

    void BTrDB::asyncConnectEventLoop(std::function<void(grpc::ClientContext*)> ctx, const std::vector<std::string> endpoints, std::function<void(std::shared_ptr<BTrDB>)> on_done) {
        std::unique_ptr<grpcinterface::Mash> mash = BTrDB::rawConnect(ctx, endpoints);
        if (!mash) {
            on_done(std::shared_ptr<BTrDB>(nullptr));
            return;
        }

        std::shared_ptr<BTrDB> b(new BTrDB(MASH(*mash), endpoints));
        mash.reset(nullptr);
        on_done(b);
        BTrDB::eventLoop(b->completion_queue);
    }

    void default_ctx(grpc::ClientContext* context) {
        (void) context;
    }

    void connect_ctx(grpc::ClientContext* context) {
        gpr_timespec op_deadline;
        op_deadline.tv_sec = 2;
        op_deadline.tv_nsec = 0;
        op_deadline.clock_type = gpr_clock_type::GPR_TIMESPAN;
        context->set_deadline(op_deadline);
    }
}
