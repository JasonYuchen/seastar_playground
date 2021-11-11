//
// Created by jason on 2021/10/27.
//

#pragma once

#include <seastar/core/future.hh>

#include "protocol/raft.hh"
#include "util/fragmented_temporary_buffer.hh"

namespace rafter::protocol {

// TODO: design pipeline-style writer for checksum, compression, and so on

uint64_t serialize(
    const struct group_id& obj,
    util::fragmented_temporary_buffer::ostream& o);

uint64_t deserialize(
    struct group_id& obj,
    util::fragmented_temporary_buffer::istream& i);

uint64_t serialize(
    const struct bootstrap& obj,
    util::fragmented_temporary_buffer::ostream& o);

uint64_t deserialize(
    struct bootstrap& obj,
    util::fragmented_temporary_buffer::istream& i);

uint64_t serialize(
    const struct membership& obj,
    util::fragmented_temporary_buffer::ostream& o);

uint64_t deserialize(
    struct membership& obj,
    util::fragmented_temporary_buffer::istream& i);

uint64_t serialize(
    const struct log_entry& obj,
    util::fragmented_temporary_buffer::ostream& o);

uint64_t deserialize(
    struct log_entry& obj,
    util::fragmented_temporary_buffer::istream& i);

uint64_t serialize(
    const struct hard_state& obj,
    util::fragmented_temporary_buffer::ostream& o);

uint64_t deserialize(
    struct hard_state& obj,
    util::fragmented_temporary_buffer::istream& i);

uint64_t serialize(
    const struct snapshot_file& obj,
    util::fragmented_temporary_buffer::ostream& o);

uint64_t deserialize(
    struct snapshot_file& obj,
    util::fragmented_temporary_buffer::istream& i);

uint64_t serialize(
    const struct snapshot& obj,
    util::fragmented_temporary_buffer::ostream& o);

uint64_t deserialize(
    struct snapshot& obj,
    util::fragmented_temporary_buffer::istream& i);

uint64_t serialize(
    const struct hint& obj,
    util::fragmented_temporary_buffer::ostream& o);

uint64_t deserialize(
    struct hint& obj,
    util::fragmented_temporary_buffer::istream& i);

uint64_t serialize(
    const struct message& obj,
    util::fragmented_temporary_buffer::ostream& o);

uint64_t deserialize(
    struct message& obj,
    util::fragmented_temporary_buffer::istream& i);

uint64_t serialize(
    const struct config_change& obj,
    util::fragmented_temporary_buffer::ostream& o);

uint64_t deserialize(
    struct config_change& obj,
    util::fragmented_temporary_buffer::istream& i);

uint64_t serialize(
    const struct update& obj,
    util::fragmented_temporary_buffer::ostream& o);

uint64_t deserialize(
    struct update& obj,
    util::fragmented_temporary_buffer::istream& i);

seastar::future<uint64_t> deserialize_meta(
    struct update& obj,
    seastar::input_stream<char>& i);

}  // namespace rafter::protocol
