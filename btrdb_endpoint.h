#ifndef BTRDB_ENDPOINT_H_
#define BTRDB_ENDPOINT_H_

#include <cstdint>

#include <grpc++/grpc++.h>
#include <grpc/impl/codegen/gpr_types.h>

#include "btrdb.grpc.pb.h"
#include "btrdb_stream.h"
#include "btrdb_util.h"

namespace btrdb {
    class Stream;

    template <typename ResponseType, typename IntermediateType, typename ValueType>
    class AsyncRequestImpl : public AsyncRequest {
    public:
        AsyncRequestImpl() : got_metadata(false) {}
        virtual void response_to_value(ValueType* value, const IntermediateType& intermediate) = 0;

        bool process_batch() override {
            Status status(response_buffer.stat());

            if (status.isError()) {
                std::vector<ValueType> dummy;
                this->on_data(true, status, dummy);
                return true;
            }

            int num_values = response_buffer.values_size();
            if (num_values == 0) {
                if (this->got_metadata) {
                    std::vector<ValueType> dummy;
                    this->on_data(true, status, dummy);
                    return true;
                } else {
                    this->got_metadata = true;
                    return false;
                }
            }
            std::vector<ValueType> values(num_values);
            for (int i = 0; i != num_values; i++) {
                const IntermediateType& response = response_buffer.values(i);
                this->response_to_value(&values[i], response);
            }
            this->on_data(false, Status(), values);
            this->response_buffer.Clear();
            this->request_next();

            return false;
        }

        void end_request() override {
            std::vector<ValueType> dummy;
            this->on_data(true, Status(), dummy);
        }

        inline void request_next() {
            this->reader->Read(&this->response_buffer, static_cast<AsyncRequest*>(this));
        }

        bool got_metadata;
        ResponseType response_buffer;
        grpc::Status status;
        grpc::ClientContext context;
        std::function<void(bool, Status, std::vector<ValueType>&)> on_data;
        std::unique_ptr<grpc::ClientAsyncReader<ResponseType>> reader;
    };

    template <typename ResponseType, typename T, typename ValueType>
    std::function<void(bool finished, Status, std::vector<ValueType>&)> wrap_on_data_vernum(AsyncRequestImpl<ResponseType, T, ValueType>* reqdata, std::function<void(bool, Status, std::vector<ValueType>&, std::uint64_t)> on_data) {
        return [=](bool finished, Status status, std::vector<ValueType>& data) {
            std::uint64_t version = reqdata->response_buffer.versionmajor();
            return on_data(finished, status, data, version);
        };
    }

    template <typename ResponseType>
    class RawPointAsyncRequest : public AsyncRequestImpl<ResponseType, grpcinterface::RawPoint, struct RawPoint> {
        inline void response_to_value(struct RawPoint* value, const grpcinterface::RawPoint& intermediate) override {
            value->time = intermediate.time();
            value->value = intermediate.value();
        }
    };

    template <typename ResponseType>
    class StatisticalPointAsyncRequest : public AsyncRequestImpl<ResponseType, grpcinterface::StatPoint, struct StatisticalPoint> {
        inline void response_to_value(struct StatisticalPoint* value, const grpcinterface::StatPoint& intermediate) override {
            value->time = intermediate.time();
            value->min = intermediate.min();
            value->mean = intermediate.mean();
            value->max = intermediate.max();
            value->count = intermediate.count();
        }
    };

    template <typename ResponseType>
    class ChangedRangeAsyncRequest : public AsyncRequestImpl<ResponseType, grpcinterface::ChangedRange, struct ChangedRange> {
        inline void response_to_value(struct ChangedRange* value, const grpcinterface::ChangedRange& intermediate) override {
            value->start = intermediate.start();
            value->end = intermediate.end();
        }
    };

    template <typename ResponseType>
    class StreamAsyncRequest : public AsyncRequestImpl<ResponseType, grpcinterface::StreamDescriptor, std::unique_ptr<Stream>> {
        inline void response_to_value(std::unique_ptr<Stream>* value, const grpcinterface::StreamDescriptor& intermediate) {
            Stream* s = new Stream(nullptr, intermediate);
            value->reset(s);
        }
    };

    class Endpoint {
    public:
        Endpoint(): channel_(), stub_() {}
        bool connectBlocking(gpr_timespec deadline, const std::vector<std::string>& endpoints);
        void connect(const std::string& hostport);

        Status insert(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, std::vector<struct RawPoint>::const_iterator data_start, std::vector<struct RawPoint>::const_iterator data_end, bool sync = false, std::uint64_t* version = nullptr);
        Status deleteRange(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, std::int64_t start, std::int64_t end, std::uint64_t* version);
        Status obliterate(std::function<void(grpc::ClientContext*)> ctx, const void* uuid);
        Status listAllCollections(std::function<void(grpc::ClientContext*)> ctx, std::vector<std::string>* collections);
        Status listCollections(std::function<void(grpc::ClientContext*)> ctx, const std::string& prefix, const std::string& from, std::uint64_t limit, std::vector<std::string>* collections);
        void listCollectionsAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(Status, std::vector<std::string>&)> on_data, const std::string& prefix, const std::string& from, std::uint64_t limit);
        Status info(std::function<void(grpc::ClientContext*)> ctx, grpcinterface::InfoResponse* response);
        Status streamInfo(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, grpcinterface::StreamInfoResponse* response, bool omit_version, bool omit_descriptor);
        Status create(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, const std::string& collection, const std::map<std::string, std::string>& tags, const std::map<std::string, std::string>& annotations);

        void lookupStreamsAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(bool, Status, std::vector<std::unique_ptr<Stream>>&)> on_data, const std::string& collection, bool is_prefix, const std::map<std::string, std::pair<std::string, bool>>& tags, const std::map<std::string, std::pair<std::string, bool>>& annotations);
        void rawValuesAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(bool, Status, std::vector<RawPoint>&, std::uint64_t)> on_data, const void* uuid, std::int64_t start, std::int64_t end, std::uint64_t version = 0);
        void alignedWindowsAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, const void* uuid, std::int64_t start, std::int64_t end, std::uint8_t pointwidth, std::uint64_t version = 0);
        void windowsAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, const void* uuid, std::int64_t start, std::int64_t end, std::uint64_t width, std::uint8_t depth, std::uint64_t version = 0);
        void changesAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(bool, Status, std::vector<struct ChangedRange>&, std::uint64_t)> on_data, const void* uuid, std::uint64_t from_version, std::uint64_t to_version, std::uint8_t resolution = 0);
        void nearestAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(Status, const RawPoint& rawpoint, std::uint64_t)> on_data, const void* uuid, std::int64_t timestamp, bool backward, std::uint64_t version = 0);
        void infoAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(Status, const grpcinterface::InfoResponse& response)> on_data);

    private:
        std::shared_ptr<grpc::Channel> channel_;
        std::unique_ptr<grpcinterface::BTrDB::Stub> stub_;
    };
}

#endif // BTRDB_ENDPOINT_H_
