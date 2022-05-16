//
// Created by jason on 2022/4/13.
//

#pragma once

#include <any>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <string>

#include "protocol/raft.hh"
#include "rsm/files.hh"
#include "util/seastarx.hh"

namespace rafter {

class statemachine {
 public:
  enum class snapshot_status {
    // the snapshot operation is successfully completed
    done,
    // the snapshot operation is stopped due to the node is being closed
    stopped,
    // the snapshot operation is aborted instructed by the statemachine
    aborted,
  };

  using result = protocol::rsm_result;
  using factory =
      std::function<future<std::unique_ptr<statemachine>>(protocol::group_id)>;

  virtual ~statemachine() = default;
  virtual future<result> open() = 0;
  // TODO(jyc): support batch
  virtual future<result> update(uint64_t index, std::string_view cmd) = 0;
  virtual future<result> lookup(std::string_view cmd) = 0;
  virtual future<> sync() = 0;
  virtual future<std::any> prepare() = 0;
  virtual future<snapshot_status> save_snapshot(
      std::any ctx,
      output_stream<char>& writer,
      rsm::files& fs,
      bool& abort) = 0;
  virtual future<snapshot_status> recover_from_snapshot(
      input_stream<char>& reader,
      const protocol::snapshot_files& fs,
      bool& abort) = 0;
  virtual future<> close() = 0;
  virtual protocol::state_machine_type type() const = 0;
};

class kv_statemachine final : public statemachine {
 public:
  future<result> open() override {
    // TODO(jyc): protocol
    return make_ready_future<result>();
  }
  // TODO(jyc): support batch lookup and batch update
  future<result> update(uint64_t index, std::string_view cmd) override {
    // TODO(jyc): protocol
    return make_ready_future<result>();
  }
  future<result> lookup(std::string_view cmd) override {
    // TODO(jyc): protocol
    return make_ready_future<result>();
  }
  future<> sync() override {
    // TODO(jyc): protocol
    return make_ready_future<>();
  }
  future<std::any> prepare() override {
    // TODO(jyc): protocol
    return make_ready_future<std::any>();
  }
  future<snapshot_status> save_snapshot(
      std::any ctx,
      output_stream<char>& writer,
      rsm::files& fs,
      bool& abort) override {
    // TODO(jyc): protocol
    return make_ready_future<snapshot_status>(snapshot_status::done);
  }
  future<snapshot_status> recover_from_snapshot(
      input_stream<char>& reader,
      const protocol::snapshot_files& fs,
      bool& abort) override {
    // TODO(jyc): protocol
    return make_ready_future<snapshot_status>(snapshot_status::done);
  }
  future<> close() override {
    // TODO(jyc): protocol
    return make_ready_future<>();
  }
  protocol::state_machine_type type() const override {
    return protocol::state_machine_type::regular;
  }

 private:
  std::unordered_map<std::string, std::string> _data;
};

}  // namespace rafter
