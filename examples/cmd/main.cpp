#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>

#include <grpc++/grpc++.h>
#include <grpc/impl/codegen/compression_types.h>

#include <btrdb/btrdb.h>

using btrdb::split_string;

bool check_arguments(std::vector<std::string>& tokens, int required_args, int expected_args, bool exactly = true) {
    // Include the command
    int required_tokens = required_args + 1;
    int expected_tokens = expected_args + 1;

    if (tokens.size() < required_tokens) {
        return true;
    }

    if (exactly && tokens.size() > expected_tokens) {
        return true;
    }

    while (tokens.size() < expected_tokens) {
        tokens.emplace_back("");
    }
    return false;
}

char parse_char(char hexbyte) {
    switch (hexbyte) {
    case '0':
        return 0x0;
    case '1':
        return 0x1;
    case '2':
        return 0x2;
    case '3':
        return 0x3;
    case '4':
        return 0x4;
    case '5':
        return 0x5;
    case '6':
        return 0x6;
    case '7':
        return 0x7;
    case '8':
        return 0x8;
    case '9':
        return 0x9;
    case 'a':
        return 0xa;
    case 'b':
        return 0xb;
    case 'c':
        return 0xc;
    case 'd':
        return 0xd;
    case 'e':
        return 0xe;
    case 'f':
        return 0xf;
    default:
        std::cerr << "Error parsing UUID" << std::endl;
        return 0x0;
    }
}

char parse_byte(const char* data) {
    return (parse_char(data[0]) << 4) | parse_char(data[1]);
}

bool parse_uuid(const std::string& uuid_str, char* uuid_bytes) {
    if (uuid_str.size() != 36 || uuid_str[8] != '-' || uuid_str[13] != '-' ||
        uuid_str[18] != '-' || uuid_str[23] != '-') {
        return false;
    }
    const char* uuid_str_data = uuid_str.data();

    uuid_bytes[0] = parse_byte(&uuid_str_data[0]);
    uuid_bytes[1] = parse_byte(&uuid_str_data[2]);
    uuid_bytes[2] = parse_byte(&uuid_str_data[4]);
    uuid_bytes[3] = parse_byte(&uuid_str_data[6]);
    uuid_bytes[4] = parse_byte(&uuid_str_data[9]);
    uuid_bytes[5] = parse_byte(&uuid_str_data[11]);
    uuid_bytes[6] = parse_byte(&uuid_str_data[14]);
    uuid_bytes[7] = parse_byte(&uuid_str_data[16]);
    uuid_bytes[8] = parse_byte(&uuid_str_data[19]);
    uuid_bytes[9] = parse_byte(&uuid_str_data[21]);
    uuid_bytes[10] = parse_byte(&uuid_str_data[24]);
    uuid_bytes[11] = parse_byte(&uuid_str_data[26]);
    uuid_bytes[12] = parse_byte(&uuid_str_data[28]);
    uuid_bytes[13] = parse_byte(&uuid_str_data[30]);
    uuid_bytes[14] = parse_byte(&uuid_str_data[32]);
    uuid_bytes[15] = parse_byte(&uuid_str_data[34]);
    return true;
}

char stringify_half_byte(std::uint8_t half_byte) {
    switch (half_byte) {
    case 0:
        return '0';
    case 1:
        return '1';
    case 2:
        return '2';
    case 3:
        return '3';
    case 4:
        return '4';
    case 5:
        return '5';
    case 6:
        return '6';
    case 7:
        return '7';
    case 8:
        return '8';
    case 9:
        return '9';
    case 10:
        return 'a';
    case 11:
        return 'b';
    case 12:
        return 'c';
    case 13:
        return 'd';
    case 14:
        return 'e';
    case 15:
        return 'f';
    default:
        std::cerr << "Error stringifying UUID" << std::endl;
        return '\0';
    }
}

std::string stringify_byte(std::uint8_t byte) {
    char stringified[2];
    stringified[0] = stringify_half_byte(byte >> 4);
    stringified[1] = stringify_half_byte(byte & 0x0f);
    return std::string(stringified, sizeof(stringified));
}

std::string stringify_uuid(const void* uuid) {
    std::stringstream stream;
    const std::uint8_t* uuid_bytes = reinterpret_cast<const std::uint8_t*>(uuid);

    stream << stringify_byte(uuid_bytes[0]);
    stream << stringify_byte(uuid_bytes[1]);
    stream << stringify_byte(uuid_bytes[2]);
    stream << stringify_byte(uuid_bytes[3]);
    stream << "-";
    stream << stringify_byte(uuid_bytes[4]);
    stream << stringify_byte(uuid_bytes[5]);
    stream << "-";
    stream << stringify_byte(uuid_bytes[6]);
    stream << stringify_byte(uuid_bytes[7]);
    stream << "-";
    stream << stringify_byte(uuid_bytes[8]);
    stream << stringify_byte(uuid_bytes[9]);
    stream << "-";
    stream << stringify_byte(uuid_bytes[10]);
    stream << stringify_byte(uuid_bytes[11]);
    stream << stringify_byte(uuid_bytes[12]);
    stream << stringify_byte(uuid_bytes[13]);
    stream << stringify_byte(uuid_bytes[14]);
    stream << stringify_byte(uuid_bytes[15]);

    std::string uuid_str;
    stream >> uuid_str;
    return uuid_str;
}

bool parse_map(const std::string& s, std::map<std::string, std::string>* map) {
    std::vector<std::string> kvpairs = split_string(s, ',');
    for (int i = 0; i != kvpairs.size(); i++) {
        const std::string kvpair = kvpairs[i];
        std::vector<std::string> kvpairparsed = split_string(kvpair, '=');
        if (kvpairparsed.size() != 2) {
            return false;
        }
        const std::string& key = kvpairparsed[0];
        const std::string& value = kvpairparsed[1];
        map->operator[](key) = value;
    }
    return true;
}

bool parse_partial_map(const std::string& s, std::map<std::string, std::pair<std::string, bool>>* map) {
    std::vector<std::string> kvpairs = split_string(s, ',');
    for (int i = 0; i != kvpairs.size(); i++) {
        const std::string kvpair = kvpairs[i];
        std::vector<std::string> kvpairparsed = split_string(kvpair, '=');
        if (kvpairparsed.size() != 2) {
            const std::string& key = kvpairparsed[0];
            const std::pair<std::string, bool> value(kvpairparsed[1], true);
            map->operator[](key) = value;
        } else {
            map->operator[](kvpair) = std::make_pair<std::string, bool>("", false);
        }
    }
    return true;
}

std::string stringify_map(const std::map<std::string, std::string>& m) {
    std::stringstream stream;
    bool first = true;
    for (auto i = m.begin(); i != m.end(); i++) {
        if (first) {
            first = false;
        } else {
            stream << ",";
        }
        stream << i->first << "=" << i->second;
    }

    std::string s;
    stream >> s;
    return s;
}

template <typename T>
bool parse_number(const std::string& s, T* number) {
    std::stringstream parser(s);
    parser >> *number;
    return !parser.fail();
}

bool parse_time(const std::string& s, std::int64_t* number) {
    if (s == "min") {
        *number = btrdb::BTrDB::MIN_TIME;
        return true;
    } else if (s == "max") {
        *number = btrdb::BTrDB::MAX_TIME;
        return true;
    } else {
        return parse_number(s, number);
    }
}

bool parse_bool(const std::string& s, bool* boolean) {
    if (s == "true") {
        *boolean = true;
        return true;
    } else if (s == "false") {
        *boolean = false;
        return true;
    } else {
        return false;
    }
}

bool parse_point(const std::string& s, struct btrdb::RawPoint* point) {
    std::size_t comma_index = s.find(',');
    if (comma_index == std::string::npos) {
        return false;
    }

    std::string time_string(s, 0, comma_index);
    std::string value_string(s, comma_index + 1);

    bool time_valid = parse_time(time_string, &point->time);
    if (!time_valid) {
        return false;
    }

    bool value_valid = parse_number(value_string, &point->value);
    return value_valid;
}

void cmd_ctx(grpc::ClientContext* ctx) {
}

void evaluate(std::shared_ptr<btrdb::BTrDB> b, const std::string& command) {
    std::vector<std::string> tokens = split_string(command, ' ');
    if (tokens.size() == 0) {
        return;
    }

    const std::string& opcode = tokens[0];

    if (opcode == "collections") {
        if (check_arguments(tokens, 0, 1)) {
            std::cout << "Usage: collections [prefix]" << std::endl;
            return;
        }

        std::string prefix = "";
        if (tokens.size() == 2) {
            prefix = tokens[1];
        }

        std::vector<std::string> collections;
        btrdb::Status status = b->listCollections(cmd_ctx, &collections, prefix);
        if (status.isError()) {
            std::cout << status.message() << std::endl;
            return;
        }
        for (int i = 0; i != collections.size(); i++) {
            std::cout << collections[i] << std::endl;
        }
    } else if (opcode == "streams") {
        if (check_arguments(tokens, 0, 4)) {
            std::cout << "Usage: streams [collection] [is_prefix] [tags] "
                      << "[annotations]" << std::endl;
            return;
        }

        const std::string& collection = tokens[1];
        bool is_prefix;
        if (tokens[2] == "") {
            is_prefix = true;
        } else {
            bool is_prefix_valid = parse_bool(tokens[2], &is_prefix);
            if (!is_prefix_valid) {
                std::cout << "Bad is_prefix" << std::endl;
                return;
            }
        }

        std::map<std::string, std::pair<std::string, bool>> tags;
        std::map<std::string, std::pair<std::string, bool>> annotations;
        if (!parse_partial_map(tokens[3], &tags)) {
            std::cout << "Bad tags map" << std::endl;
            return;
        }
        if (!parse_partial_map(tokens[4], &annotations)) {
            std::cout << "Bad annotations map" << std::endl;
            return;
        }

        std::vector<std::unique_ptr<btrdb::Stream>> results;
        btrdb::Status stat = b->lookupStreams(cmd_ctx, &results, collection, is_prefix, tags, annotations);
        for (int i = 0; i != results.size(); i++) {
            std::unique_ptr<btrdb::Stream>& s = results[i];
            const std::string* stream_collection;
            const std::map<std::string, std::string>* stream_tags;
            const std::map<std::string, std::string>* stream_annotations;
            std::uint64_t stream_annotations_version;
            btrdb::Status stream_status = s->collection(cmd_ctx, &stream_collection);
            if (stream_status.isError()) {
                std::cout << stream_status.message() << std::endl;
                continue;
            }
            stream_status = s->tags(cmd_ctx, &stream_tags);
            if (stream_status.isError()) {
                std::cout << stream_status.message() << std::endl;
                continue;
            }
            stream_status = s->cachedAnnotations(cmd_ctx, &stream_annotations, &stream_annotations_version);
            if (stream_status.isError()) {
                std::cout << stream_status.message() << std::endl;
                continue;
            }
            std::cout << stringify_uuid(s->UUID())
                      << " " << *stream_collection
                      << " " << stringify_map(*stream_tags)
                      << " " << stringify_map(*stream_annotations)
                      << " " << stream_annotations_version
                      << std::endl;
        }
        if (stat.isError()) {
            std::cout << stat.message() << std::endl;
        }
    } else if (opcode == "create") {
        if (check_arguments(tokens, 2, 4)) {
            std::cout << "Usage: create UUID collection [tags] [annotations]"
                      << std::endl;
            return;
        }

        char uuid[16];
        if (!parse_uuid(tokens[1], uuid)) {
            std::cout << "Bad UUID" << std::endl;
            return;
        }

        const std::string& collection = tokens[2];

        std::map<std::string, std::string> tags;
        std::map<std::string, std::string> annotations;
        if (!parse_map(tokens[3], &tags)) {
            std::cout << "Bad tags map" << std::endl;
            return;
        }
        if (!parse_map(tokens[4], &annotations)) {
            std::cout << "Bad annotations map" << std::endl;
            return;
        }

        btrdb::Status status = b->create(cmd_ctx, uuid, collection, tags, annotations);
        std::cout << status.message() << std::endl;
    } else if (opcode == "insert") {
        if (check_arguments(tokens, 3, 3, false)) {
            std::cout << "Usage: insert UUID sync time1,value1 [time2,value2] ..."
                      << std::endl;
            return;
        }

        char uuid[16];
        if (!parse_uuid(tokens[1], uuid)) {
            std::cout << "Bad UUID" << std::endl;
            return;
        }

        bool sync = false;
        if (!tokens[2].empty() && !parse_bool(tokens[2], &sync)) {
            std::cout << "Bad sync indicator" << std::endl;
            return;
        }

        std::vector<std::string>::size_type num_points = tokens.size() - 3;
        std::vector<struct btrdb::RawPoint>::size_type num_points_casted = (std::vector<struct btrdb::RawPoint>::size_type) num_points;
        std::vector<struct btrdb::RawPoint> data(num_points_casted);
        for (std::vector<struct btrdb::RawPoint>::size_type i = 0; i != num_points_casted; i++) {
            bool parsed_successfully = parse_point(tokens[3 + i], &data[i]);
            if (!parsed_successfully) {
                std::cout << "Bad point #" << (i + 1) << ": " << tokens[3 + i]
                          << std::endl;
                return;
            }
        }

        std::unique_ptr<btrdb::Stream> s = b->streamFromUUID(uuid);

        bool exists;
        btrdb::Status stat = s->exists(cmd_ctx, &exists);
        if (stat.isError()) {
            std::cout << stat.message() << std::endl;
            return;
        }
        if (!exists) {
            std::cout << "Stream does not exist" << std::endl;
            return;
        }

        std::uint64_t version_resp = 0;
        btrdb::Status status = s->insert(cmd_ctx, &version_resp, data.begin(), data.end(), sync);
        std::cout << status.message() << std::endl;
        if (!status.isError()) {
            std::cout << "Version: " << version_resp << std::endl;
        }
    } else if (opcode == "delete") {
        if (check_arguments(tokens, 1, 3)) {
            std::cout << "Usage: delete UUID [start] [end]" << std::endl;
            return;
        }

        char uuid[16];
        if (!parse_uuid(tokens[1], uuid)) {
            std::cout << "Bad UUID" << std::endl;
            return;
        }

        std::int64_t start = btrdb::BTrDB::MIN_TIME;
        if (!tokens[2].empty() && !parse_time(tokens[2], &start)) {
            std::cout << "Bad start time" << std::endl;
            return;
        }

        std::int64_t end = btrdb::BTrDB::MAX_TIME;
        if (!tokens[3].empty() && !parse_time(tokens[3], &end)) {
            std::cout << "Bad end time" << std::endl;
            return;
        }

        std::unique_ptr<btrdb::Stream> s = b->streamFromUUID(uuid);

        bool exists;
        btrdb::Status stat = s->exists(cmd_ctx, &exists);
        if (stat.isError()) {
            std::cout << stat.message() << std::endl;
            return;
        }
        if (!exists) {
            std::cout << "Stream does not exist" << std::endl;
            return;
        }

        std::uint64_t version_resp = 0;
        btrdb::Status status = s->deleteRange(cmd_ctx, &version_resp, start, end);
        std::cout << status.message() << std::endl;
        if (!status.isError()) {
            std::cout << "Version: " << version_resp << std::endl;
        }
    } else if (opcode == "obliterate") {
        if (check_arguments(tokens, 1, 1)) {
            std::cout << "Usage: obliterate UUID" << std::endl;
            return;
        }

        char uuid[16];
        if (!parse_uuid(tokens[1], uuid)) {
            std::cout << "Bad UUID" << std::endl;
            return;
        }

        std::unique_ptr<btrdb::Stream> s = b->streamFromUUID(uuid);

        bool exists;
        btrdb::Status stat = s->exists(cmd_ctx, &exists);
        if (stat.isError()) {
            std::cout << stat.message() << std::endl;
            return;
        }
        if (!exists) {
            std::cout << "Stream does not exist" << std::endl;
            return;
        }

        btrdb::Status status = s->obliterate(cmd_ctx);
        std::cout << status.message() << std::endl;
    } else if (opcode == "raw") {
        if (check_arguments(tokens, 1, 4)) {
            std::cout << "Usage: raw UUID [start] [end] [version]" << std::endl;
            return;
        }

        char uuid[16];
        if (!parse_uuid(tokens[1], uuid)) {
            std::cout << "Bad UUID" << std::endl;
            return;
        }

        std::int64_t start = btrdb::BTrDB::MIN_TIME;
        if (!tokens[2].empty() && !parse_time(tokens[2], &start)) {
            std::cout << "Bad start time" << std::endl;
            return;
        }

        std::int64_t end = btrdb::BTrDB::MAX_TIME;
        if (!tokens[3].empty() && !parse_time(tokens[3], &end)) {
            std::cout << "Bad end time" << std::endl;
            return;
        }

        std::uint64_t version = 0;
        if (!tokens[4].empty() && !parse_number(tokens[4], &version)) {
            std::cout << "Bad version" << std::endl;
            return;
        }

        std::unique_ptr<btrdb::Stream> s = b->streamFromUUID(uuid);

        bool exists;
        btrdb::Status stat = s->exists(cmd_ctx, &exists);
        if (stat.isError()) {
            std::cout << stat.message() << std::endl;
            return;
        }
        if (!exists) {
            std::cout << "Stream does not exist" << std::endl;
            return;
        }

        std::uint64_t version_resp = 0;
        btrdb::Status status = s->rawValues(cmd_ctx, [&version_resp](bool finished, btrdb::Status status, const std::vector<struct btrdb::RawPoint>& data, std::uint64_t version)
        {
            (void) finished;
            (void) status;
            version_resp = version;
            for (int i = 0; i != data.size(); i++) {
                const struct btrdb::RawPoint& pt = data[i];
                std::cout << pt.time << "," << pt.value << std::endl;
            }
        }, start, end, version);
        std::cout << "Version: " << version_resp << std::endl;
        if (status.isError()) {
            std::cout << status.message() << std::endl;
        }
    } else if (opcode == "aligned") {
        if (check_arguments(tokens, 1, 5)) {
            std::cout << "Usage: aligned UUID [pwe] [start] [end] [version]"
                      << std::endl;
            return;
        }

        char uuid[16];
        if (!parse_uuid(tokens[1], uuid)) {
            std::cout << "Bad UUID" << std::endl;
            return;
        }

        std::uint32_t pwe_wide = btrdb::BTrDB::MAX_PWE;
        if (!tokens[2].empty() && !parse_number(tokens[2], &pwe_wide)) {
            std::cout << "Bad pwe" << std::endl;
            return;
        }
        std::uint8_t pwe = (std::uint8_t) pwe_wide;

        std::int64_t start = btrdb::BTrDB::MIN_TIME;
        if (!tokens[3].empty() && !parse_time(tokens[3], &start)) {
            std::cout << "Bad start time" << std::endl;
            return;
        }

        std::int64_t end = btrdb::BTrDB::MAX_TIME;
        if (!tokens[4].empty() && !parse_time(tokens[4], &end)) {
            std::cout << "Bad end time" << std::endl;
            return;
        }

        std::uint64_t version = 0;
        if (!tokens[5].empty() && !parse_number(tokens[5], &version)) {
            std::cout << "Bad version" << std::endl;
            return;
        }

        std::unique_ptr<btrdb::Stream> s = b->streamFromUUID(uuid);

        bool exists;
        btrdb::Status stat = s->exists(cmd_ctx, &exists);
        if (stat.isError()) {
            std::cout << stat.message() << std::endl;
            return;
        }
        if (!exists) {
            std::cout << "Stream does not exist" << std::endl;
            return;
        }

        std::uint64_t version_resp;
        btrdb::Status status = s->alignedWindows(cmd_ctx, [&version_resp](bool finished, btrdb::Status status, const std::vector<struct btrdb::StatisticalPoint> data, std::uint64_t version) {
            (void) finished;
            (void) status;
            version_resp = version;
            for (int i = 0; i != data.size(); i++) {
                const struct btrdb::StatisticalPoint& pt = data[i];
                std::cout << pt.time << "," << pt.min << "," << pt.mean << ","
                          << pt.max << "," << pt.count << std::endl;
            }
        }, start, end, pwe, version);
        std::cout << "Version: " << version_resp << std::endl;
        if (status.isError()) {
            std::cout << status.message() << std::endl;
        }
    } else if (opcode == "windows") {
        if (check_arguments(tokens, 1, 6)) {
            std::cout << "Usage: windows UUID [width] [start] [end] [depth] "
                      << "[version]" << std::endl;
            return;
        }

        char uuid[16];
        if (!parse_uuid(tokens[1], uuid)) {
            std::cout << "Bad UUID" << std::endl;
            return;
        }

        std::uint64_t width = btrdb::BTrDB::MAX_TIME - btrdb::BTrDB::MIN_TIME;
        if (!tokens[2].empty() && !parse_number(tokens[2], &width)) {
            std::cout << "Bad width" << std::endl;
            return;
        }

        std::int64_t start = btrdb::BTrDB::MIN_TIME;
        if (!tokens[3].empty() && !parse_time(tokens[3], &start)) {
            std::cout << "Bad start time" << std::endl;
            return;
        }

        std::int64_t end = btrdb::BTrDB::MAX_TIME;
        if (!tokens[4].empty() && !parse_time(tokens[4], &end)) {
            std::cout << "Bad end time" << std::endl;
            return;
        }

        std::uint32_t depth_wide = 0;
        if (!tokens[5].empty() && !parse_number(tokens[5], &depth_wide)) {
            std::cout << "Bad depth" << std::endl;
            return;
        }
        std::uint8_t depth = (std::uint8_t) depth_wide;

        std::uint64_t version = 0;
        if (!tokens[6].empty() && !parse_number(tokens[6], &version)) {
            std::cout << "Bad version" << std::endl;
            return;
        }

        std::unique_ptr<btrdb::Stream> s = b->streamFromUUID(uuid);

        bool exists;
        btrdb::Status stat = s->exists(cmd_ctx, &exists);
        if (stat.isError()) {
            std::cout << stat.message() << std::endl;
            return;
        }
        if (!exists) {
            std::cout << "Stream does not exist" << std::endl;
            return;
        }

        std::vector<struct btrdb::StatisticalPoint> data;
        std::uint64_t version_resp;
        btrdb::Status status = s->windows(cmd_ctx, [&version_resp](bool finished, btrdb::Status status, const std::vector<struct btrdb::StatisticalPoint> data, std::uint64_t version) {
            (void) finished;
            (void) status;
            version_resp = version;
            for (int i = 0; i != data.size(); i++) {
                const struct btrdb::StatisticalPoint& pt = data[i];
                std::cout << pt.time << "," << pt.min << "," << pt.mean << ","
                          << pt.max << "," << pt.count << std::endl;
            }
        }, start, end, width, depth, version);
        std::cout << "Version: " << version_resp << std::endl;
        if (status.isError()) {
            std::cout << status.message() << std::endl;
        }
    } else if (opcode == "changes") {
        if (check_arguments(tokens, 1, 4)) {
            std::cout << "Usage: changes UUID from_version [to_version] "
                      << "[depth]" << std::endl;
            return;
        }

        char uuid[16];
        if (!parse_uuid(tokens[1], uuid)) {
            std::cout << "Bad UUID" << std::endl;
            return;
        }

        std::uint64_t from_version = 1;
        if (!tokens[2].empty() && !parse_number(tokens[2], &from_version)) {
            std::cout << "Bad from_version" << std::endl;
            return;
        }

        std::uint64_t to_version = 0;
        if (!tokens[3].empty() && !parse_number(tokens[3], &to_version)) {
            std::cout << "Bad to_version" << std::endl;
            return;
        }

        std::uint32_t depth_wide = 0;
        if (!tokens[4].empty() && !parse_number(tokens[4], &depth_wide)) {
            std::cout << "Bad depth" << std::endl;
            return;
        }
        std::uint8_t depth = (std::uint8_t) depth_wide;

        std::unique_ptr<btrdb::Stream> s = b->streamFromUUID(uuid);

        bool exists;
        btrdb::Status stat = s->exists(cmd_ctx, &exists);
        if (stat.isError()) {
            std::cout << stat.message() << std::endl;
            return;
        }
        if (!exists) {
            std::cout << "Stream does not exist" << std::endl;
            return;
        }

        std::vector<struct btrdb::StatisticalPoint> data;
        std::uint64_t version_resp;
        btrdb::Status status = s->changes(cmd_ctx, [&version_resp](bool finished, btrdb::Status status, const std::vector<struct btrdb::ChangedRange> data, std::uint64_t version) {
            (void) finished;
            (void) status;
            version_resp = version;
            for (int i = 0; i != data.size(); i++) {
                const struct btrdb::ChangedRange& range = data[i];
                std::cout << range.start << " - " << range.end << std::endl;
            }
        }, from_version, to_version, depth);
        std::cout << "Version: " << version_resp << std::endl;
        if (status.isError()) {
            std::cout << status.message() << std::endl;
        }
    } else if (opcode == "nearest") {
        if (check_arguments(tokens, 1, 4)) {
            std::cout << "Usage: nearest UUID [backward] [timestamp] [version]"
                      << std::endl;
            return;
        }

        char uuid[16];
        if (!parse_uuid(tokens[1], uuid)) {
            std::cout << "Bad UUID" << std::endl;
            return;
        }

        bool backward = false;
        if (!tokens[2].empty() && !parse_bool(tokens[2], &backward)) {
            std::cout << "Bad backwards indicator" << std::endl;
            return;
        }

        std::int64_t timestamp = backward ? btrdb::BTrDB::MAX_TIME : btrdb::BTrDB::MIN_TIME;
        if (!tokens[3].empty() && !parse_time(tokens[3], &timestamp)) {
            std::cout << "Bad timestamp" << std::endl;
            return;
        }

        std::uint64_t version = 0;
        if (!tokens[4].empty() && !parse_number(tokens[4], &version)) {
            std::cout << "Bad version" << std::endl;
            return;
        }

        std::unique_ptr<btrdb::Stream> s = b->streamFromUUID(uuid);

        bool exists;
        btrdb::Status stat = s->exists(cmd_ctx, &exists);
        if (stat.isError()) {
            std::cout << stat.message() << std::endl;
            return;
        }
        if (!exists) {
            std::cout << "Stream does not exist" << std::endl;
            return;
        }

        struct btrdb::RawPoint pt;
        std::uint64_t version_resp;
        btrdb::Status status = s->nearest(cmd_ctx, &pt, &version_resp, timestamp, backward, version);
        std::cout << pt.time << "," << pt.value << std::endl;
        std::cout << "Version: " << version_resp << std::endl;
        if (status.isError()) {
            std::cout << status.message() << std::endl;
        }
    } else if (opcode == "help") {
        std::cout << "collections" << std::endl
                  << "streams" << std::endl
                  << "create" << std::endl
                  << "insert" << std::endl
                  << "delete" << std::endl
                  << "obliterate" << std::endl
                  << "raw" << std::endl
                  << "aligned" << std::endl
                  << "windows" << std::endl
                  << "changes" << std::endl
                  << "nearest" << std::endl
                  << "help" << std::endl;
    } else {
        std::cout << "Unknown operation \"" << opcode << "\"." << std::endl
                  << "Use \"help\" for a list of valid operations. " << std::endl;
    }
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cout << "Usage: " << argv[0] << " address:port" << std::endl;
        return 1;
    }
    std::shared_ptr<btrdb::BTrDB> b = btrdb::BTrDB::connect(cmd_ctx, { argv[1] });
    if (b.get() == nullptr) {
        std::cout << "Error: could not connect" << std::endl;
        return 2;
    }

    for (;;) {
        std::cout << argv[1] << "> ";
        std::string cmd;
        std::getline(std::cin, cmd);
        if (!std::cin.good()) {
            break;
        }
        if (cmd.empty()) {
            continue;
        }

        /* Evaluate the statement. */
        evaluate(b, cmd);
    }

    std::cout << std::endl;
    return 0;
}
