#include "btrdb_util.h"
#include <sstream>
#include <grpc++/grpc++.h>

namespace btrdb {
    std::vector<std::string> split_string(const std::string& str, char delimiter) {
        std::vector<std::string> parts;

        std::istringstream input(str);
        std::string part;
        while (std::getline(input, part, delimiter)) {
            parts.push_back(part);
        }

        return parts;
    }

    Status::Status() : type_(Status::Type::StatusOK), code_(0), message_() {}

    Status::Status(const grpc::Status& grpcstatus)
        : type_(Status::Type::GRPCError), code_(0),
          message_(grpcstatus.error_message()) {}

    Status::Status(std::uint32_t code, std::string message)
        : type_(Status::Type::CodedError), code_(code), message_(message) {
        if (code == 0) {
            type_ = Status::Type::StatusOK;
        }
    }

    Status::Status(const grpcinterface::Status& btrdbstatus)
        : Status(btrdbstatus.code(), btrdbstatus.msg()) {}

    bool Status::isError() const {
        return this->type_ != Status::Type::StatusOK;
    }

    std::uint32_t Status::code() const {
        return this->code_;
    }

    std::string Status::message() const {
        std::ostringstream output;

        switch (this->type_) {
        case Status::Type::StatusOK:
            output << "Success";
            break;
        case Status::Type::GRPCError:
            output << "grpc: " << this->message_;
            break;
        case Status::Type::CodedError:
            output << "[" << this->code_ << "] " << this->message_;
            break;
        }

        return output.str();
    }

    const Status Status::ClusterDegraded(419, "Cluster is degraded");
    const Status Status::NoSuchStream(404, "No such stream");
    const Status Status::WrongArgs(421, "Invalid arguments");
    const Status Status::Disconnected(421, "Driver is disconnected");
}
