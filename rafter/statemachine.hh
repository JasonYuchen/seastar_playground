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

/// \defgroup rsm Replicated State Machine

/// \addtogroup rsm
/// @{

/// \class statemachine
///
/// \brief the interface of a rafter-managed replicated state machine.
///
/// user should provide a \code statemachine::factory \endcode to generate the
/// actual statemachine that implements this interface.
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

  virtual ~statemachine() = default;

  /// open opens the statemachine returned from \code statemachine::factory
  /// \endcode. This method will only be called once immediately after the
  /// creation.
  ///
  /// \return Future that becomes ready once the statemachine is opened. The
  /// \ref result should contain the on-disk index in its value.
  virtual future<result> open() = 0;

  // TODO(jyc): support batch

  /// update updates the statemachine with given \param cmd. The update method
  /// must be deterministic. This method is like a CAS and can be used as a read
  /// operation but not recommended.
  ///
  /// \param index The corresponding raft log's index of this given \param cmd.
  /// \param cmd The user's proposal to apply the statemachine.
  /// \return Future that becomes ready when the update is finished. The \ref
  /// result should contain the outcome of this update. When an exception is
  /// thrown if there is unrecoverable error, it will panic the program.
  virtual future<result> update(uint64_t index, std::string_view cmd) = 0;

  /// lookup queries the statemachine with given \param cmd. This method should
  /// not change the statemachine.
  ///
  /// \param cmd The user's query.
  /// \return Future that becomes ready when the lookup is finished. The \ref
  /// result should contain the outcome of this lookup. When an exception is
  /// thrown if there is error, it will be passed to the client.
  virtual future<result> lookup(std::string_view cmd) = 0;

  /// sync synchronizes the state of the statemachine so that are core data are
  /// persisted.
  ///
  /// \return Future that becomes ready when the sync is finished. When an
  /// exception is thrown if there is unrecoverable error, it will panic the
  /// program.
  virtual future<> sync() = 0;

  /// prepare prepares the snapshot to be captured, it is always invoked before
  /// the corresponding \ref save_snapshot.
  ///
  /// \return Future that becomes ready when the preparation is finished. The
  /// returned \code any \endcode should contain all required information and
  /// will be passed to the subsequent \code save_snapshot \endcode. When an
  /// exception is thrown if there is error, it will abort this snapshot.
  virtual future<std::any> prepare() = 0;

  /// save_snapshot saves the statemachine's state with given \param ctx
  /// returned by the \code prepare \endcode method.
  ///
  /// \param ctx The required information returned by the previous \code prepare
  /// \endcode method.
  /// \param writer A writer for writing all statemachine's data in a
  /// deterministic manner.
  /// \param fs A file collection for statemachine's disk files. These files are
  /// considered a part of this snapshot and will also be provided in \code
  /// recover_from_snapshot \endcode.
  /// \param abort An indicator to notify the statemachine that this save
  /// operation is aborted or not. It is recommended to check for abort
  /// periodically in a long saving operation and return \code
  /// snapshot_status::aborted \endcode as soon as notified.
  /// \return Future that becomes ready when the saving is finished. The
  /// returned \code snapshot_status \endcode should contain the result of this
  /// saving. If the returned value is not done, this snapshot will be
  /// considered failed and discarded.
  virtual future<snapshot_status> save_snapshot(
      std::any ctx,
      output_stream<char>& writer,
      rsm::files& fs,
      bool& abort) = 0;

  /// recover_from_snapshot recovers the statemachine's state with latest
  /// snapshot.
  ///
  /// \param reader A reader for reading all statemachine's data.
  /// \param fs A file collection for statemachine's disk files.
  /// \param abort An indicator to notify the statemachine that this recover
  /// operation is aborted or not.
  /// \return Future that becomes ready when the recovery is finished. The
  /// returned \code snapshot_status \endcode should contain the result of this
  /// recovery.
  virtual future<snapshot_status> recover_from_snapshot(
      input_stream<char>& reader,
      const protocol::snapshot_files& fs,
      bool& abort) = 0;

  /// close closes the statemachine. It is recommended to release any resources
  /// owned by this statemachine and prepare for re-opening in the future.
  ///
  /// \return Future that becomes ready when close is finished.
  virtual future<> close() = 0;

  /// type returns the type of the underlying statemachine. Currently we only
  /// support in-memory statemachine as noted in the original Raft thesis.
  ///
  /// \return Type of the underlying statemachine.
  virtual protocol::state_machine_type type() const = 0;
};

class statemachine_factory {
 public:
  virtual future<std::unique_ptr<statemachine>> make(
      protocol::group_id gid) = 0;
  virtual ~statemachine_factory() = default;
};

class kv_statemachine final : public statemachine {
 public:
  explicit kv_statemachine(protocol::group_id gid);
  future<result> open() override;
  future<result> update(uint64_t index, std::string_view cmd) override;
  future<result> lookup(std::string_view cmd) override;
  future<> sync() override;
  future<std::any> prepare() override;
  future<snapshot_status> save_snapshot(
      std::any ctx,
      output_stream<char>& writer,
      rsm::files& fs,
      bool& abort) override;
  future<snapshot_status> recover_from_snapshot(
      input_stream<char>& reader,
      const protocol::snapshot_files& fs,
      bool& abort) override;
  future<> close() override;
  protocol::state_machine_type type() const override;

 private:
  struct string_hash {
    using is_transparent = void;
    [[nodiscard]] size_t operator()(const char* key) const {
      return std::hash<std::string_view>{}(key);
    }
    [[nodiscard]] size_t operator()(std::string_view key) const {
      return std::hash<std::string_view>{}(key);
    }
    [[nodiscard]] size_t operator()(const std::string& key) const {
      return std::hash<std::string>{}(key);
    }
  };

  protocol::group_id _gid;
  std::unordered_map<std::string, std::string, string_hash, std::equal_to<>>
      _data;
};

class kv_statemachine_factory : public statemachine_factory {
 public:
  future<std::unique_ptr<statemachine>> make(protocol::group_id gid) override {
    return make_ready_future<std::unique_ptr<statemachine>>(
        new kv_statemachine(gid));
  }
};

/// @}

}  // namespace rafter
