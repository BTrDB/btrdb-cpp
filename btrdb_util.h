#ifndef BTRDB_UTIL_H_
#define BTRDB_UTIL_H_

#include <cstdlib>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <grpc++/grpc++.h>
#include "btrdb.grpc.pb.h"

namespace btrdb {
    /* Some useful constants. */
    const constexpr std::size_t UUID_NUM_BYTES = 16;

    /* Structures for BTrDB data. */
    struct RawPoint {
        std::int64_t time;
        double value;
    };

    struct StatisticalPoint {
        std::int64_t time;
        double min;
        double mean;
        double max;
        std::uint64_t count;
    };

    struct ChangedRange {
        std::int64_t start;
        std::int64_t end;
    };

    /* Interface to keep track of data for pending async requests. */
    class AsyncRequest {
    public:
        virtual ~AsyncRequest() {}
        virtual bool process_batch() = 0;
        virtual void end_request() = 0;
    };

    /* Status type. */
    class Status {
    public:
        Status();
        Status(const grpc::Status& grpcstatus);
        Status(std::uint32_t code, std::string message);
        Status(const grpcinterface::Status& btrdbstatus);

        bool isError() const;
        std::uint32_t code() const;
        std::string message() const;

        template <typename ResponseType>
        static Status fromResponse(grpc::Status& grpcstatus, ResponseType& response) {
            if (!grpcstatus.ok()) {
                return Status(grpcstatus);
            } else if (response.has_stat()) {
                const grpcinterface::Status& btrdbstatus = response.stat();
                if (btrdbstatus.code() != 0) {
                    return Status(btrdbstatus);
                }
                return Status(response.stat());
            }
            return Status();
        }

        /* Some useful constants. */
        static const Status Disconnected;
        static const Status ClusterDegraded;
        static const Status WrongArgs;
        static const Status NoSuchStream;

    private:
        enum Type {
            StatusOK,
            GRPCError,
            CodedError
        };
        Type type_;
        std::uint32_t code_;
        std::string message_;
    };

    /* Some useful functions. */
    std::vector<std::string> split_string(const std::string& str, char delimiter);

    template <typename V, typename... ExtraArgs>
    Status async_to_sync(std::function<Status(std::function<void(bool, Status, V, ExtraArgs...)>)> async_fn, std::function<void(bool, Status, V, ExtraArgs...)> worker) {
        bool done = false;
        std::mutex condvar_mutex;
        std::condition_variable condvar;

        Status status;
        auto callback = [&](bool finished, Status stat, V data, ExtraArgs... extra_args) {
            if (stat.isError()) {
                status = stat;
            }
            worker(finished, stat, data, extra_args...);
            if (finished) {
                std::lock_guard<std::mutex> lock(condvar_mutex);
                done = true;
                condvar.notify_one();
            }
        };

        status = async_fn(std::move(callback));
        if (status.isError()) {
            return status;
        }

        {
            std::unique_lock<std::mutex> lock(condvar_mutex);
            while (!done) {
                condvar.wait(lock);
            }
        }

        return status;
    }

    template <typename V>
    std::function<void(bool, Status, std::vector<V>&)> collect_worker(std::vector<V>* result) {
        return [=](bool finished, Status stat, std::vector<V>& data) {
            (void) finished;
            auto num_points = data.size();
            result->reserve(result->size() + num_points);
            for (typename std::vector<V>::size_type i = 0; i != num_points; i++) {
                result->push_back(std::move(data[i]));
            }
        };
    }

    template <typename V>
    std::function<void(bool, Status, std::vector<V>&, std::uint64_t)> collect_vernum_worker(std::vector<V>* result, std::uint64_t* version_ptr) {
        return [=](bool finished, Status stat, std::vector<V>& data, std::uint64_t version) {
            (void) finished;
            auto num_points = data.size();
            result->reserve(result->size() + num_points);
            for (typename std::vector<V>::size_type i = 0; i != num_points; i++) {
                result->push_back(std::move(data[i]));
            }
            *version_ptr = version;
        };
    }
}

#endif // BTRDB_UTIL_H_
