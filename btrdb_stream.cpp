#include "btrdb_stream.h"

#include <memory>
#include <mutex>
#include <condition_variable>

#include "btrdb.h"

namespace btrdb {
    Stream::Stream(const std::shared_ptr<BTrDB>& b, const void* uuid)
        : b_(b), known_to_exist_(false), has_tags_(false),
          has_annotations_(false), has_collection_(false) {
        std::memcpy(uuid_, uuid, 16);
    }

    Stream::Stream(const std::shared_ptr<BTrDB>& b, const grpcinterface::StreamDescriptor& descriptor)
        : b_(b) {
        const std::string& uuid_string = descriptor.uuid();
        std::memcpy(uuid_, uuid_string.data(), 16);
        this->updateFromDescriptor(descriptor);
    }

    Status Stream::exists(std::function<void(grpc::ClientContext*)> ctx, bool* exists) {
        if (this->known_to_exist_) {
            *exists = true;
            return Status();
        }

        Status status = this->refreshMetadata(ctx);
        if (status.isError()) {
            if (status.code() == 404) {
                *exists = false;
                return Status();
            } else {
                return status;
            }
        }

        *exists = true;
        return Status();
    }

    Status Stream::collection(std::function<void(grpc::ClientContext*)> ctx, const std::string** collection_ptr) {
        if (this->has_collection_) {
            *collection_ptr = &this->collection_;
            return Status();
        }

        Status status = this->refreshMetadata(ctx);
        if (status.isError()) {
            return status;
        }

        *collection_ptr = &this->collection_;
        return status;
    }

    Status Stream::tags(std::function<void(grpc::ClientContext*)> ctx, const std::map<std::string, std::string>** tags_ptr) {
        if (this->has_tags_) {
            *tags_ptr = &this->tags_;
            return Status();
        }

        Status status = this->refreshMetadata(ctx);
        if (status.isError()) {
            return status;
        }

        *tags_ptr = &this->tags_;
        return status;
    }

    Status Stream::annotations(std::function<void(grpc::ClientContext*)> ctx, const std::map<std::string, std::string>** annotations_ptr, std::uint64_t* annotations_ver) {
        Status status = this->refreshMetadata(ctx);
        if (status.isError()) {
            return status;
        }

        *annotations_ptr = &this->annotations_;
        *annotations_ver = this->annotations_version_;
        return status;
    }

    Status Stream::cachedAnnotations(std::function<void(grpc::ClientContext*)> ctx, const std::map<std::string, std::string>** annotations_ptr, std::uint64_t* annotations_ver) {
        if (!this->has_annotations_) {
            Status status = this->refreshMetadata(ctx);
            if (status.isError()) {
                return status;
            }
        }

        *annotations_ptr = &this->annotations_;
        *annotations_ver = this->annotations_version_;
        return Status();
    }

    const void* Stream::UUID() {
        return this->uuid_;
    }

    Status Stream::version(std::function<void(grpc::ClientContext*)> ctx, std::uint64_t* version_ptr) {
        Status status;
        grpcinterface::StreamInfoResponse streamInfo;
        do {
            std::shared_ptr<Endpoint> ep;
            status = this->b_->endpointFor(ctx, this->uuid_, &ep);
            if (status.isError()) {
                continue;
            }
            status = ep->streamInfo(ctx, this->uuid_, &streamInfo, false, true);
        } while (this->b_->handleEndpointStatus(status));

        if (status.isError()) {
            return status;
        }

        *version_ptr = streamInfo.versionmajor();
        return status;
    }

    Status Stream::rawValuesAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct RawPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint64_t version) {
        this->b_->asyncEndpointFor(ctx, this->uuid_, [=](Status status, std::shared_ptr<Endpoint> ep) {
            if (status.isError()) {
                this->rawValuesAsync(ctx, std::move(on_data), start, end, version);
                return;
            }

            ep->rawValuesAsync(ctx, this->b_->completion_queue, [=](bool finished, Status status, std::vector<struct RawPoint>& data, std::uint64_t version) {
                if (this->b_->handleEndpointStatus(status)) {
                    this->rawValuesAsync(ctx, std::move(on_data), start, end, version);
                    return;
                }
                on_data(finished, status, data, version);
            }, this->uuid_, start, end, version);
        });
        return Status();
    }

    Status Stream::alignedWindowsAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint8_t pointwidth, std::uint64_t version) {
        this->b_->asyncEndpointFor(ctx, this->uuid_, [=](Status status, std::shared_ptr<Endpoint> ep) {
            if (status.isError()) {
                this->alignedWindowsAsync(ctx, std::move(on_data), start, end, pointwidth, version);
                return;
            }

            ep->alignedWindowsAsync(ctx, this->b_->completion_queue, [=](bool finished, Status status, std::vector<struct StatisticalPoint>& data, std::uint64_t version) {
                if (this->b_->handleEndpointStatus(status)) {
                    this->alignedWindowsAsync(ctx, std::move(on_data), start, end, pointwidth, version);
                    return;
                }
                on_data(finished, status, data, version);
            }, this->uuid_, start, end, pointwidth, version);
        });
        return Status();
    }

    Status Stream::windowsAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint64_t width, std::uint8_t depth, std::uint64_t version) {
        this->b_->asyncEndpointFor(ctx, this->uuid_, [=](Status status, std::shared_ptr<Endpoint> ep) {
            if (status.isError()) {
                this->windowsAsync(ctx, std::move(on_data), start, end, width, depth, version);
                return;
            }

            ep->windowsAsync(ctx, this->b_->completion_queue, [=](bool finished, Status status, std::vector<struct StatisticalPoint>& data, std::uint64_t version) {
                if (this->b_->handleEndpointStatus(status)) {
                    this->windowsAsync(ctx, std::move(on_data), start, end, width, depth, version);
                    return;
                }
                on_data(finished, status, data, version);
            }, this->uuid_, start, end, width, depth, version);
        });
        return Status();
    }

    Status Stream::changesAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct ChangedRange>&, std::uint64_t)> on_data, std::uint64_t from_version, std::uint64_t to_version, std::uint8_t resolution) {
        this->b_->asyncEndpointFor(ctx, this->uuid_, [=](Status status, std::shared_ptr<Endpoint> ep) {
            if (status.isError()) {
                this->changesAsync(ctx, std::move(on_data), from_version, to_version, resolution);
                return;
            }
            ep->changesAsync(ctx, this->b_->completion_queue, [=](bool finished, Status status, std::vector<struct ChangedRange>& data, std::uint64_t version) {
                if (this->b_->handleEndpointStatus(status)) {
                    this->changesAsync(ctx, std::move(on_data), from_version, to_version, resolution);
                    return;
                }
                on_data(finished, status, data, version);
            }, this->uuid_, from_version, to_version, resolution);
        });
        return Status();
    }

    Status Stream::nearestAsync(std::function<void(grpc::ClientContext*)> ctx, std::function<void(Status, const RawPoint&, std::uint64_t)> on_data, std::int64_t timestamp, bool backward, std::uint64_t version) {
        this->b_->asyncEndpointFor(ctx, this->uuid_, [=](Status status, std::shared_ptr<Endpoint> ep) {
            if (status.isError()) {
                this->nearestAsync(ctx, std::move(on_data), timestamp, backward, version);
                return;
            }
            ep->nearestAsync(ctx, this->b_->completion_queue, [=](Status status, const RawPoint& data, std::uint64_t version) {
                if (this->b_->handleEndpointStatus(status)) {
                    this->nearestAsync(ctx, std::move(on_data), timestamp, backward, version);
                    return;
                }
                on_data(status, data, version);
            }, this->uuid_, timestamp, backward, version);
        });
        return Status();
    }

    // TODO: chunk this up into 5000 point batches
    Status Stream::insert(std::function<void(grpc::ClientContext*)> ctx, std::uint64_t* version_ptr, std::vector<struct RawPoint>::const_iterator data_start, std::vector<struct RawPoint>::const_iterator data_end, bool sync) {
        Status status;
        do {
            std::shared_ptr<Endpoint> ep;
            status = this->b_->endpointFor(ctx, this->uuid_, &ep);
            if (status.isError()) {
                continue;
            }
            status = ep->insert(ctx, this->uuid_, data_start, data_end, sync, version_ptr);
        } while (this->b_->handleEndpointStatus(status));

        return status;
    }

    Status Stream::deleteRange(std::function<void(grpc::ClientContext*)> ctx, std::uint64_t* version_ptr, std::int64_t start, std::int64_t end) {
        Status status;
        do {
            std::shared_ptr<Endpoint> ep;
            status = this->b_->endpointFor(ctx, this->uuid_, &ep);
            if (status.isError()) {
                continue;
            }
            status = ep->deleteRange(ctx, this->uuid_, start, end, version_ptr);
        } while (this->b_->handleEndpointStatus(status));

        return status;
    }

    Status Stream::obliterate(std::function<void(grpc::ClientContext*)> ctx) {
        Status status;
        do {
            std::shared_ptr<Endpoint> ep;
            status = this->b_->endpointFor(ctx, this->uuid_, &ep);
            if (status.isError()) {
                continue;
            }
            status = ep->obliterate(ctx, this->uuid_);
        } while (this->b_->handleEndpointStatus(status));

        return status;
    }

    Status Stream::rawValues(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct RawPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint64_t version) {
        std::function<Status(std::function<void(bool, Status, std::vector<struct RawPoint>&, std::uint64_t)>)> callback = [=](std::function<void(bool, Status, std::vector<struct RawPoint>&, std::uint64_t)> callback) {
            return this->rawValuesAsync(ctx, callback, start, end, version);
        };
        return async_to_sync(std::move(callback), on_data);
    }

    Status Stream::alignedWindows(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint8_t pointwidth, std::uint64_t version) {
        std::function<Status(std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)>)> callback = [=](std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> callback) {
            return this->alignedWindowsAsync(ctx, callback, start, end, pointwidth, version);
        };
        return async_to_sync(std::move(callback), on_data);
    }

    Status Stream::windows(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> on_data, std::int64_t start, std::int64_t end, std::uint64_t width, std::uint8_t depth, std::uint64_t version) {
        std::function<Status(std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)>)> callback = [=](std::function<void(bool, Status, std::vector<struct StatisticalPoint>&, std::uint64_t)> callback) {
            return this->windowsAsync(ctx, callback, start, end, width, depth, version);
        };
        return async_to_sync(std::move(callback), on_data);
    }

    Status Stream::changes(std::function<void(grpc::ClientContext*)> ctx, std::function<void(bool, Status, std::vector<struct ChangedRange>&, std::uint64_t)> on_data, std::uint64_t from_version, std::uint64_t to_version, std::uint8_t resolution) {
        std::function<Status(std::function<void(bool, Status, std::vector<struct ChangedRange>&, std::uint64_t)>)> callback = [=](std::function<void(bool, Status, std::vector<struct ChangedRange>&, std::uint64_t)> callback) {
            return this->changesAsync(ctx, callback, from_version, to_version, resolution);
        };
        return async_to_sync(std::move(callback), on_data);
    }

    Status Stream::rawValues(std::function<void(grpc::ClientContext*)> ctx, std::vector<struct RawPoint>* result, std::uint64_t* version_ptr, std::int64_t start, std::int64_t end, std::uint64_t version) {
        return this->rawValues(ctx, collect_vernum_worker(result, version_ptr), start, end, version);
    }

    Status Stream::alignedWindows(std::function<void(grpc::ClientContext*)> ctx, std::vector<struct StatisticalPoint>* result, std::uint64_t* version_ptr, std::int64_t start, std::int64_t end, std::uint8_t pointwidth, std::uint64_t version) {
        return this->alignedWindows(ctx, collect_vernum_worker(result, version_ptr), start, end, pointwidth, version);
    }

    Status Stream::windows(std::function<void(grpc::ClientContext*)> ctx, std::vector<struct StatisticalPoint>* result, std::uint64_t* version_ptr, std::int64_t start, std::int64_t end, std::uint64_t width, std::uint8_t depth, std::uint64_t version) {
        return this->windows(ctx, collect_vernum_worker(result, version_ptr), start, end, width, depth, version);
    }

    Status Stream::changes(std::function<void(grpc::ClientContext*)> ctx, std::vector<struct ChangedRange>* result, std::uint64_t* version_ptr, std::uint64_t from_version, std::uint64_t to_version, std::uint8_t resolution) {
        return this->changes(ctx, collect_vernum_worker(result, version_ptr), from_version, to_version, resolution);
    }

    Status Stream::nearest(std::function<void(grpc::ClientContext*)> ctx, RawPoint* result, std::uint64_t* version_ptr, std::int64_t timestamp, bool backward, std::uint64_t version) {
        std::function<Status(std::function<void(bool, Status, const RawPoint&, std::uint64_t)>)> callback = [=](std::function<void(bool, Status, const RawPoint&, std::uint64_t)> callback) {
            return this->nearestAsync(ctx, [=](Status stat, const RawPoint& rawpoint, std::uint64_t version) {
                callback(true, stat, rawpoint, version);
            }, timestamp, backward, version);
        };
        std::function<void(bool, Status, const RawPoint&, std::uint64_t)> worker = [=](bool finished, Status status, const RawPoint& rawpoint, std::uint64_t version) {
            *result = rawpoint;
            *version_ptr = version;
        };
        return async_to_sync(std::move(callback), std::move(worker));
    }

    Status Stream::refreshMetadata(std::function<void(grpc::ClientContext*)> ctx) {
        Status status;
        grpcinterface::StreamInfoResponse streamInfo;
        do {
            std::shared_ptr<Endpoint> ep;
            status = this->b_->endpointFor(ctx, this->uuid_, &ep);
            if (status.isError()) {
                continue;
            }
            status = ep->streamInfo(ctx, this->uuid_, &streamInfo, true, false);
        } while (this->b_->handleEndpointStatus(status));

        if (!status.isError()) {
            const grpcinterface::StreamDescriptor& descriptor = streamInfo.streamdescriptor();
            this->updateFromDescriptor(descriptor);
        }

        return status;
    }

    void Stream::updateFromDescriptor(const grpcinterface::StreamDescriptor& descriptor) {
        this->known_to_exist_ = true;

        this->has_tags_ = true;
        this->tags_.clear();
        for (int i = 0; i != descriptor.tags_size(); i++) {
            const grpcinterface::KeyValue& kvpair = descriptor.tags(i);
            this->tags_[kvpair.key()] = kvpair.value();
        }

        this->has_annotations_ = true;
        this->annotations_.clear();
        for (int i = 0; i != descriptor.annotations_size(); i++) {
            const grpcinterface::KeyValue& kvpair = descriptor.annotations(i);
            this->annotations_[kvpair.key()] = kvpair.value();
        }
        this->annotations_version_ = descriptor.annotationversion();

        this->has_collection_ = true;
        this->collection_ = descriptor.collection();
    }
}
