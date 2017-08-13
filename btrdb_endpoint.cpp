#include <grpc++/grpc++.h>
#include <grpc/impl/codegen/gpr_types.h>

#include "btrdb_endpoint.h"
#include "btrdb_util.h"

namespace btrdb {
    /* TODO: need to clean up state on failure... */
    bool Endpoint::connectBlocking(gpr_timespec deadline, const std::vector<std::string>& endpoints) {
        for (const std::string& endpoint : endpoints) {
            this->connect(endpoint);

            grpc_connectivity_state state = channel_->GetState(true);
            switch (state) {
            case grpc_connectivity_state::GRPC_CHANNEL_IDLE:
            case grpc_connectivity_state::GRPC_CHANNEL_CONNECTING:
            case grpc_connectivity_state::GRPC_CHANNEL_READY:
                return true;
            case grpc_connectivity_state::GRPC_CHANNEL_INIT:
            case grpc_connectivity_state::GRPC_CHANNEL_TRANSIENT_FAILURE:
            case grpc_connectivity_state::GRPC_CHANNEL_SHUTDOWN:
                if (channel_->WaitForConnected(deadline)) {
                    return true;
                }
            }
        }
        return false;
    }

    void Endpoint::connect(const std::string& hostport) {
        channel_ = grpc::CreateChannel(hostport, grpc::InsecureChannelCredentials());
        stub_ = std::move(grpcinterface::BTrDB::NewStub(channel_, grpc::StubOptions()));
    }

    Status Endpoint::insert(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, std::vector<struct RawPoint>::const_iterator data_start, std::vector<struct RawPoint>::const_iterator data_end, bool sync, std::uint64_t* version) {
        grpcinterface::InsertParams params;
        params.set_uuid(uuid, 16);
        params.set_sync(sync);

        auto& values = *params.mutable_values();
        values.Reserve(data_end - data_start);

        for (auto i = data_start; i != data_end; i++) {
            grpcinterface::RawPoint value;
            value.set_time(i->time);
            value.set_value(i->value);
        }

        grpc::ClientContext context;
        ctx(&context);

        grpcinterface::InsertResponse response;
        grpc::Status status = this->stub_->Insert(&context, params, &response);
        if (version != nullptr) {
            *version = response.versionmajor();
        }
        return Status::fromResponse(status, response);
    }

    Status Endpoint::deleteRange(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, std::int64_t start, std::int64_t end, std::uint64_t* version) {
        grpcinterface::DeleteParams params;
        params.set_uuid(uuid, 16);
        params.set_start(start);
        params.set_end(end);

        grpc::ClientContext context;
        ctx(&context);

        grpcinterface::DeleteResponse response;
        grpc::Status status = this->stub_->Delete(&context, params, &response);
        if (version != nullptr) {
            *version = response.versionmajor();
        }
        return Status::fromResponse(status, response);
    }

    Status Endpoint::obliterate(std::function<void(grpc::ClientContext*)> ctx, const void* uuid) {
        grpcinterface::ObliterateParams params;
        params.set_uuid(uuid, 16);

        grpc::ClientContext context;
        ctx(&context);

        grpcinterface::ObliterateResponse response;
        grpc::Status status = this->stub_->Obliterate(&context, params, &response);
        return Status::fromResponse(status, response);
    }

    Status Endpoint::listAllCollections(std::function<void(grpc::ClientContext*)> ctx, std::vector<std::string>* collections) {
        grpcinterface::ListCollectionsParams params;
        params.set_prefix("");
        params.set_startwith("");
        params.set_limit(1000000);

        grpc::ClientContext context;
        ctx(&context);

        grpcinterface::ListCollectionsResponse response;
        grpc::Status status = this->stub_->ListCollections(&context, params, &response);

        const auto& coll = response.collections();
        int num_colls = coll.size();
        collections->reserve(collections->size() + num_colls);

        for (int i = 0; i != num_colls; i++) {
            collections->push_back(coll[i]);
        }

        return Status::fromResponse(status, response);
    }

    Status Endpoint::listCollections(std::function<void(grpc::ClientContext*)> ctx, const std::string& prefix, const std::string& from, std::uint64_t limit, std::vector<std::string>* collections) {
        grpcinterface::ListCollectionsParams params;
        params.set_prefix(prefix);
        params.set_startwith(from);
        params.set_limit(limit);

        grpc::ClientContext context;
        ctx(&context);

        grpcinterface::ListCollectionsResponse response;
        grpc::Status status = this->stub_->ListCollections(&context, params, &response);

        const auto& coll = response.collections();
        int num_colls = coll.size();
        collections->reserve(collections->size() + num_colls);

        for (int i = 0; i != num_colls; i++) {
            collections->push_back(coll[i]);
        }

        return Status::fromResponse(status, response);
    }

    class ListCollectionsAsyncRequestImpl : public AsyncRequest {
    public:
        bool process_batch() override {
            std::vector<std::string> collections;
            Status status = Status::fromResponse(this->grpc_status, response_buffer);

            if (!status.isError()) {
                for (int i = 0; i != this->response_buffer.collections_size(); i++) {
                    collections.push_back(this->response_buffer.collections(i));
                }
            }

            this->on_data(status, collections);
            return true;
        }

        void end_request() override {
            std::vector<std::string> collections;
            this->on_data(Status(), collections);
        }

        inline void request_next() {
            this->reader->Finish(&this->response_buffer, &this->grpc_status, static_cast<AsyncRequest*>(this));
        }

        // The ClientContext is (and must be) deleted after the ClientAsyncResponseReader.
        grpcinterface::ListCollectionsResponse response_buffer;
        grpc::Status grpc_status;
        grpc::ClientContext context;
        std::function<void(Status, std::vector<std::string>&)> on_data;
        std::unique_ptr<grpc::ClientAsyncResponseReaderInterface<grpcinterface::ListCollectionsResponse>> reader;
    };

    void Endpoint::listCollectionsAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(Status, std::vector<std::string>&)> on_data, const std::string& prefix, const std::string& from, std::uint64_t limit) {
        grpcinterface::ListCollectionsParams params;
        params.set_prefix(prefix);
        params.set_startwith(from);
        params.set_limit(limit);

        ListCollectionsAsyncRequestImpl* reqdata = new ListCollectionsAsyncRequestImpl;
        ctx(&reqdata->context);
        reqdata->on_data = on_data;
        reqdata->reader = this->stub_->AsyncListCollections(&reqdata->context, params, cq);
        reqdata->request_next();
    }

    Status Endpoint::info(std::function<void(grpc::ClientContext*)> ctx, grpcinterface::InfoResponse* response) {
        grpcinterface::InfoParams params;

        grpc::ClientContext context;
        ctx(&context);

        grpc::Status status = this->stub_->Info(&context, params, response);
        return Status::fromResponse(status, *response);
    }

    Status Endpoint::streamInfo(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, grpcinterface::StreamInfoResponse* response, bool omit_version, bool omit_descriptor) {
        grpcinterface::StreamInfoParams params;
        params.set_uuid(uuid, 16);
        params.set_omitversion(omit_version);
        params.set_omitdescriptor(omit_descriptor);

        grpc::ClientContext context;
        ctx(&context);

        grpc::Status status = this->stub_->StreamInfo(&context, params, response);
        return Status::fromResponse(status, *response);
    }

    Status Endpoint::create(std::function<void(grpc::ClientContext*)> ctx, const void* uuid, const std::string& collection, const std::map<std::string, std::string>& tags, const std::map<std::string, std::string>& annotations) {
        grpcinterface::CreateParams params;
        params.set_uuid(uuid, 16);
        params.set_collection(collection);
        for (auto it = tags.begin(); it != tags.end(); it++) {
            grpcinterface::KeyValue* kv = params.add_tags();
            kv->set_key(it->first);
            kv->set_value(it->second);
        }
        for (auto it = annotations.begin(); it != annotations.end(); it++) {
            grpcinterface::KeyValue* kv = params.add_annotations();
            kv->set_key(it->first);
            kv->set_value(it->second);
        }

        grpc::ClientContext context;
        ctx(&context);

        grpcinterface::CreateResponse response;
        grpc::Status status = this->stub_->Create(&context, params, &response);
        return Status::fromResponse(status, response);
    }

    void Endpoint::lookupStreamsAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(bool, Status, std::vector<std::unique_ptr<Stream>>&)> on_data, const std::string& collection, bool is_prefix, const std::map<std::string, std::pair<std::string, bool>>& tags, const std::map<std::string, std::pair<std::string, bool>>& annotations) {
        grpcinterface::LookupStreamsParams params;
        params.set_collection(collection);
        params.set_iscollectionprefix(is_prefix);
        for (auto it = tags.begin(); it != tags.end(); it++) {
            grpcinterface::KeyOptValue* kov = params.add_tags();
            kov->set_key(it->first);
            if (it->second.second) {
                kov->mutable_val()->set_value(it->second.first);
            }
        }
        for (auto it = annotations.begin(); it != annotations.end(); it++) {
            grpcinterface::KeyOptValue* kov = params.add_annotations();
            kov->set_key(it->first);
            if (it->second.second) {
                kov->mutable_val()->set_value(it->second.first);
            }
        }

        StreamAsyncRequest<grpcinterface::LookupStreamsResponse>* reqdata = new StreamAsyncRequest<grpcinterface::LookupStreamsResponse>;
        ctx(&reqdata->context);
        reqdata->on_data = std::move(on_data);
        reqdata->reader = std::move(this->stub_->AsyncLookupStreams(&reqdata->context, params, cq, static_cast<AsyncRequest*>(reqdata)));
        reqdata->request_next();
    }

    void Endpoint::rawValuesAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(bool, Status, std::vector<RawPoint>&, std::uint64_t)> on_data, const void* uuid, std::int64_t start, std::int64_t end, std::uint64_t version) {
        grpcinterface::RawValuesParams params;
        params.set_uuid(uuid, 16);
        params.set_start(start);
        params.set_end(end);
        params.set_versionmajor(version);

        RawPointAsyncRequest<grpcinterface::RawValuesResponse>* reqdata = new RawPointAsyncRequest<grpcinterface::RawValuesResponse>;
        ctx(&reqdata->context);
        reqdata->on_data = wrap_on_data_vernum(reqdata, std::move(on_data));
        reqdata->reader = this->stub_->AsyncRawValues(&reqdata->context, params, cq, static_cast<AsyncRequest*>(reqdata));
        reqdata->request_next();
    }

    void Endpoint::alignedWindowsAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, const void* uuid, std::int64_t start, std::int64_t end, std::uint8_t pointwidth, std::uint64_t version) {
        grpcinterface::AlignedWindowsParams params;
        params.set_uuid(uuid, 16);
        params.set_start(start);
        params.set_end(end);
        params.set_versionmajor(version);
        params.set_pointwidth(pointwidth);

        StatisticalPointAsyncRequest<grpcinterface::AlignedWindowsResponse>* reqdata = new StatisticalPointAsyncRequest<grpcinterface::AlignedWindowsResponse>;
        ctx(&reqdata->context);
        reqdata->on_data = wrap_on_data_vernum(reqdata, std::move(on_data));
        reqdata->reader = this->stub_->AsyncAlignedWindows(&reqdata->context, params, cq, static_cast<AsyncRequest*>(reqdata));
        reqdata->request_next();
    }

    void Endpoint::windowsAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, const void* uuid, std::int64_t start, std::int64_t end, std::uint64_t width, std::uint8_t depth, std::uint64_t version) {
        grpcinterface::WindowsParams params;
        params.set_uuid(uuid, 16);
        params.set_start(start);
        params.set_end(end);
        params.set_versionmajor(version);
        params.set_width(width);
        params.set_depth(depth);

        StatisticalPointAsyncRequest<grpcinterface::WindowsResponse>* reqdata = new StatisticalPointAsyncRequest<grpcinterface::WindowsResponse>;
        ctx(&reqdata->context);
        reqdata->on_data = wrap_on_data_vernum(reqdata, std::move(on_data));
        reqdata->reader = this->stub_->AsyncWindows(&reqdata->context, params, cq, static_cast<AsyncRequest*>(reqdata));
        reqdata->request_next();
    }

    void Endpoint::changesAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(bool, Status, std::vector<struct ChangedRange>&, std::uint64_t)> on_data, const void* uuid, std::uint64_t from_version, std::uint64_t to_version, std::uint8_t resolution) {
        grpcinterface::ChangesParams params;
        params.set_uuid(uuid, 16);
        params.set_frommajor(from_version);
        params.set_tomajor(to_version);
        params.set_resolution(resolution);

        ChangedRangeAsyncRequest<grpcinterface::ChangesResponse>* reqdata = new ChangedRangeAsyncRequest<grpcinterface::ChangesResponse>;
        ctx(&reqdata->context);
        reqdata->on_data = wrap_on_data_vernum(reqdata, std::move(on_data));
        reqdata->reader = std::move(this->stub_->AsyncChanges(&reqdata->context, params, cq, static_cast<AsyncRequest*>(reqdata)));
        reqdata->request_next();
    }

    class NearestAsyncRequestImpl : public AsyncRequest {
    public:
        bool process_batch() override {
            struct RawPoint placeholder;
            Status status = Status::fromResponse(this->grpc_status, response_buffer);

            if (!status.isError()) {
                this->response_to_value(&placeholder, response_buffer.value());
            }

            this->on_data(status, placeholder, response_buffer.versionmajor());
            return true;
        }

        void end_request() override {
            struct RawPoint dummy;
            this->on_data(Status(), dummy, 0);
        }

        inline void request_next() {
            this->reader->Finish(&this->response_buffer, &this->grpc_status, static_cast<AsyncRequest*>(this));
        }

        inline void response_to_value(struct RawPoint* value, const grpcinterface::RawPoint& intermediate) {
            value->time = intermediate.time();
            value->value = intermediate.value();
        }

        grpcinterface::NearestResponse response_buffer;
        grpc::Status grpc_status;
        grpc::ClientContext context;
        std::function<void(Status, const struct RawPoint& rawpoint, std::uint64_t)> on_data;
        std::unique_ptr<grpc::ClientAsyncResponseReaderInterface<grpcinterface::NearestResponse>> reader;
    };

    void Endpoint::nearestAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(Status, const RawPoint&, std::uint64_t)> on_data, const void* uuid, std::int64_t timestamp, bool backward, std::uint64_t version) {
        grpcinterface::NearestParams params;
        params.set_uuid(uuid, 16);
        params.set_time(timestamp);
        params.set_versionmajor(version);
        params.set_backward(backward);

        NearestAsyncRequestImpl* reqdata = new NearestAsyncRequestImpl;
        ctx(&reqdata->context);
        reqdata->on_data = on_data;
        reqdata->reader = this->stub_->AsyncNearest(&reqdata->context, params, cq);
        reqdata->request_next();
    }

    class InfoAsyncRequestImpl : public AsyncRequest {
    public:
        bool process_batch() override {
            Status status = Status::fromResponse(this->grpc_status, this->response_buffer);
            this->on_data(status, this->response_buffer);
            return true;
        }

        void end_request() override {
            this->on_data(Status(), this->response_buffer);
        }

        inline void request_next() {
            this->reader->Finish(&this->response_buffer, &this->grpc_status, static_cast<AsyncRequest*>(this));
        }

        grpcinterface::InfoResponse response_buffer;
        grpc::Status grpc_status;
        grpc::ClientContext context;
        std::function<void(Status, const grpcinterface::InfoResponse&)> on_data;
        std::unique_ptr<grpc::ClientAsyncResponseReaderInterface<grpcinterface::InfoResponse>> reader;
    };

    void Endpoint::infoAsync(std::function<void(grpc::ClientContext*)> ctx, grpc::CompletionQueue* cq, std::function<void(Status, const grpcinterface::InfoResponse& response)> on_data) {
        grpcinterface::InfoParams params;

        InfoAsyncRequestImpl* reqdata = new InfoAsyncRequestImpl;
        ctx(&reqdata->context);
        reqdata->on_data = on_data;
        reqdata->reader = this->stub_->AsyncInfo(&reqdata->context, params, cq);
        reqdata->request_next();
    }

}
