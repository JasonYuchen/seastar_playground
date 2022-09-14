//
// Created by jason on 2022/9/4.
//

#pragma once

#include "test/base.hh"
#include "test/core/helper.hh"
#include "test/test_logdb.hh"
#include "test/util.hh"
#include "util/seastarx.hh"

namespace rafter::test {

using namespace rafter::protocol;
using enum raft_role;
using enum message_type;
using helper = core_helper;

inline std::unique_ptr<raft_config> new_test_config(
    uint64_t node_id,
    uint64_t election,
    uint64_t heartbeat,
    uint64_t log_size = UINT64_MAX) {
  // cluster is hard coded here as we only test against a single cluster
  // unlimited log size by default
  return std::make_unique<raft_config>(raft_config{
      .cluster_id = 1,
      .node_id = node_id,
      .election_rtt = election,
      .heartbeat_rtt = heartbeat,
      .max_in_memory_log_bytes = log_size});
}

inline bool check_leader_transfer_state(
    core::raft& r,
    raft_role role,
    uint64_t leader,
    uint64_t target = group_id::INVALID_NODE) {
  bool success = (helper::_role(r) == role) &&
                 (helper::_leader_id(r) == leader) &&
                 (helper::_leader_transfer_target(r) == target);
  EXPECT_EQ(helper::_role(r), role);
  EXPECT_EQ(helper::_leader_id(r), leader);
  EXPECT_EQ(helper::_leader_transfer_target(r), target);
  return success;
}

class sm {
 public:
  virtual ~sm() = default;
  virtual bool is_raft() const = 0;
  virtual future<> handle(message m) = 0;
  virtual message_vector read_messages() = 0;
};

// a standalone logdb backed by an in-memory entries vector, should not be
// shared among multiple raft nodes
class db {
 public:
  explicit db() : _db(new test::test_logdb()), _reader({1, 1}, *_db) {}
  core::log_reader& lr() { return _reader; }
  future<> append(log_entry_vector entries) {
    update up{.gid = {1, 1}, .entries_to_save = std::move(entries)};
    storage::update_pack pack(up);
    // FIXME(jyc): due to continuation threshold, not always ready here
    EXPECT_TRUE(_db->save({&pack, 1}).available());
    EXPECT_TRUE(pack.done.get_future().available());
    _reader.apply_entries(up.entries_to_save);
    return make_ready_future<>();
  }

  void set_state(hard_state state) { _reader.set_state(state); }

 private:
  std::unique_ptr<storage::logdb> _db;
  core::log_reader _reader;
};

class raft_sm final : public sm {
 public:
  explicit raft_sm(std::unique_ptr<raft_config> cfg, std::unique_ptr<db> logdb)
    : _cfg(std::move(cfg))
    , _db(std::move(logdb))
    , _r(std::make_unique<core::raft>(*_cfg, _db->lr())) {}
  ~raft_sm() override = default;

  static raft_sm* make(
      uint64_t id,
      const std::vector<uint64_t>& peers,
      uint64_t election,
      uint64_t heartbeat) {
    return make(id, peers, election, heartbeat, std::make_unique<db>());
  }

  static raft_sm* make(
      uint64_t id,
      const std::vector<uint64_t>& peers,
      uint64_t election,
      uint64_t heartbeat,
      std::unique_ptr<db> logdb,
      uint64_t limit_size = UINT64_MAX) {
    auto config = new_test_config(id, election, heartbeat, limit_size);
    auto r = std::make_unique<raft_sm>(std::move(config), std::move(logdb));
    for (uint64_t peer : peers) {
      helper::_remotes(r->raft()).emplace(peer, core::remote{.next = 1});
    }
    return r.release();
  }

  static raft_sm* make_with_entries(const std::vector<log_id>& log_ids) {
    auto config = new_test_config(1, 5, 1);
    auto logdb = std::make_unique<db>();
    // test log db will not perform disk io
    EXPECT_TRUE(logdb->append(test::util::new_entries(log_ids)).available());
    auto r = std::make_unique<raft_sm>(std::move(config), std::move(logdb));
    helper::reset(r->raft(), log_ids.back().term, true);
    return r.release();
  }

  static raft_sm* make_with_vote(uint64_t term, uint64_t vote) {
    auto config = new_test_config(1, 5, 1);
    auto logdb = std::make_unique<db>();
    logdb->set_state({.term = term, .vote = vote});
    auto r = std::make_unique<raft_sm>(std::move(config), std::move(logdb));
    helper::reset(r->raft(), term, true);
    return r.release();
  }
  bool is_raft() const override { return true; }
  future<> handle(message m) override { return _r->handle(m); }
  message_vector read_messages() override {
    return std::exchange(_r->messages(), {});
  }
  raft_config& cfg() { return *_cfg; }
  core::raft& raft() { return *_r; }

  db* logdb0() { return _db.get(); }
  std::unique_ptr<db> logdb() { return std::move(_db); }

 private:
  std::unique_ptr<raft_config> _cfg;
  std::unique_ptr<db> _db;
  std::unique_ptr<core::raft> _r;
};

class black_hole final : public sm {
 public:
  ~black_hole() override = default;
  bool is_raft() const override { return false; }
  future<> handle(message) override { return make_ready_future<>(); }
  message_vector read_messages() override { return {}; }
};

inline sm* null() { return nullptr; }
inline sm* hole() { return new black_hole; }
inline core::raft& raft_cast(sm* s) {
  if (auto* rs = dynamic_cast<raft_sm*>(s); rs != nullptr) {
    return rs->raft();
  }
  throw rafter::util::panic("not a raft_sm");
}

struct node_pair {
  uint64_t from = 0;
  uint64_t to = 0;
  std::strong_ordering operator<=>(const node_pair&) const = default;
};

struct network {
  explicit network(std::vector<sm*>&& peers) {
    for (size_t i = 0; i < peers.size(); ++i) {
      uint64_t node_id = i + 1;
      if (peers[i] == nullptr) {
        auto test_cfg = new_test_config(node_id, 10, 1);
        auto logdb = std::make_unique<db>();
        auto sm =
            std::make_unique<raft_sm>(std::move(test_cfg), std::move(logdb));
        for (size_t j = 0; j < peers.size(); ++j) {
          // set test peers
          helper::_remotes(sm->raft()).emplace(j + 1, core::remote{.next = 1});
        }
        dbs[node_id] = sm->logdb();
        sms[node_id] = std::move(sm);
      } else if (auto& rs = *peers[i]; typeid(rs) == typeid(raft_sm)) {
        auto& p = dynamic_cast<raft_sm&>(*peers[i]);
        std::unordered_set<uint64_t> observers;
        std::unordered_set<uint64_t> witnesses;
        for (auto& node : helper::_observers(p.raft())) {
          observers.insert(node.first);
        }
        for (auto& node : helper::_witnesses(p.raft())) {
          witnesses.insert(node.first);
        }
        // manually overwrite the node id
        p.cfg().node_id = node_id;
        helper::_gid(p.raft()) = {1, node_id};
        for (size_t j = 0; j < peers.size(); ++j) {
          if (observers.contains(j + 1)) {
            helper::_observers(p.raft()).emplace(j + 1, core::remote{});
          } else if (witnesses.contains(j + 1)) {
            helper::_witnesses(p.raft()).emplace(j + 1, core::remote{});
          } else {
            helper::_remotes(p.raft()).emplace(j + 1, core::remote{});
          }
        }
        helper::reset(p.raft(), helper::_term(p.raft()), true);
        dbs[node_id] = p.logdb();
        sms[node_id] = std::unique_ptr<sm>(peers[i]);
      } else if (auto& bh = *peers[i]; typeid(bh) == typeid(black_hole)) {
        sms[node_id] = std::unique_ptr<sm>(peers[i]);
      } else {
        throw rafter::util::panic("unexpected sm");
      }
    }
  }

  future<> send(message msg) {
    std::queue<message> msgs;
    msgs.emplace(std::move(msg));
    while (!msgs.empty()) {
      auto m = std::move(msgs.front());
      msgs.pop();
      if (!sms.contains(m.to)) {
        continue;
      }
      auto& p = sms[m.to];
      co_await p->handle(std::move(m));
      filter_to(msgs, p->read_messages());
    }
  }

  void drop(uint64_t from, uint64_t to, double rate) {
    drop_msg[{from, to}] = rate;
  }

  void cut(uint64_t one, uint64_t other) {
    drop(one, other, 1.0);
    drop(other, one, 1.0);
  }

  void isolate(uint64_t node_id) {
    for (uint64_t id = 1; id <= sms.size(); ++id) {
      if (id != node_id) {
        cut(id, node_id);
      }
    }
  }

  void ignore(message_type type) { ignore_msg[type] = true; }

  void recover() {
    drop_msg.clear();
    ignore_msg.clear();
  }

  void filter_to(std::queue<message>& q, message_vector msgs) {
    static std::mt19937_64 rnd(
        std::chrono::system_clock::now().time_since_epoch().count());
    static std::uniform_real_distribution<double> dist(0.0, 1.0);
    for (auto& m : msgs) {
      if (ignore_msg.contains(m.type)) {
        continue;
      }
      if (m.type == message_type::election) {
        throw rafter::util::panic("unexpected election");
      }
      if (dist(rnd) < drop_msg[{m.from, m.to}]) {
        continue;
      }
      q.push(std::move(m));
    }
  }

  std::unordered_map<uint64_t, std::unique_ptr<sm>> sms;
  std::unordered_map<uint64_t, std::unique_ptr<db>> dbs;
  std::unordered_map<node_pair, double, rafter::util::pair_hasher> drop_msg;
  std::unordered_map<message_type, bool> ignore_msg;
};

inline future<log_entry_vector> next_entries(sm* s, db* logdb) {
  auto& r = raft_cast(s);
  log_entry_vector to_save;
  helper::_log(r).get_entries_to_save(to_save);
  co_await logdb->append(std::move(to_save));
  log_id stable{
      .term = co_await helper::_log(r).last_term(),
      .index = helper::_log(r).last_index()};
  helper::_log(r).commit_update(update_commit{.stable_log_id = stable});
  log_entry_vector to_apply;
  co_await helper::_log(r).get_entries_to_apply(to_apply);
  helper::_log(r).commit_update(
      update_commit{.processed = helper::_log(r).committed()});
  co_return std::move(to_apply);
}

inline future<log_entry_vector> get_all_entries(core::raft_log& rl) {
  log_entry_vector entries;
  co_await rl.query(rl.first_index(), entries, UINT64_MAX);
  co_return std::move(entries);
}

inline future<log_entry_vector> get_all_entries(sm* s) {
  if (auto* rs = dynamic_cast<raft_sm*>(s); rs != nullptr) {
    return get_all_entries(helper::_log(rs->raft()));
  }
  return make_exception_future<log_entry_vector>(
      rafter::util::panic("not a raft_sm"));
}

inline std::unordered_set<uint64_t> get_all_nodes(core::raft& r) {
  std::unordered_set<uint64_t> nodes;
  auto collector = [&nodes](auto&& m) {
    for (auto&& [id, _] : m) {
      if (nodes.contains(id)) {
        ADD_FAILURE() << "duplicate node " << id << " found";
      }
      nodes.insert(id);
    }
  };
  collector(helper::_remotes(r));
  collector(helper::_observers(r));
  collector(helper::_witnesses(r));
  return nodes;
}

inline membership_ptr test_membership(sm* s) {
  auto members = make_lw_shared<membership>();
  auto& r = raft_cast(s);
  for (auto& [id, _] : helper::_remotes(r)) {
    members->addresses.emplace(id, "");
  }
  for (auto& [id, _] : helper::_observers(r)) {
    members->observers.emplace(id, "");
  }
  for (auto& [id, _] : helper::_witnesses(r)) {
    members->witnesses.emplace(id, "");
  }
  return members;
}

inline membership_ptr test_membership(const std::vector<uint64_t>& nodes) {
  auto members = make_lw_shared<membership>();
  for (auto id : nodes) {
    members->addresses[id] = "";
  }
  return members;
}

inline snapshot_ptr test_snapshot(const std::vector<uint64_t>& nodes) {
  auto snap = make_lw_shared<snapshot>();
  snap->log_id = {11, 11};
  snap->membership = test_membership(nodes);
  return snap;
}

inline future<snapshot_ptr> test_snapshot(
    db* logdb, uint64_t index, membership_ptr members) {
  auto snap = logdb->lr().get_snapshot();
  if (snap && snap->log_id.index >= index) {
    co_await coroutine::return_exception(rafter::util::snapshot_out_of_date());
  }
  // the range is [marker + 1, marker + len - 1]
  auto range = logdb->lr().get_range();
  if (index > range.high) {
    l.error("snapshot index out of bound {} > {}", index, range.high);
    co_await coroutine::return_exception(
        rafter::util::panic("snapshot index out of bound"));
  }
  auto s = make_lw_shared<snapshot>();
  s->log_id = {.term = co_await logdb->lr().get_term(index), .index = index};

  s->membership = std::move(members);
  co_return std::move(s);
}

inline message naive_proposal(
    uint64_t from, uint64_t to, std::string_view payload = "") {
  message m{.type = message_type::propose, .from = from, .to = to};
  m.entries.emplace_back().copy_of(payload);
  return m;
}

inline std::vector<uint64_t> test_peers(uint64_t size) {
  std::vector<uint64_t> peers(size);
  std::iota(peers.begin(), peers.end(), 1);
  return peers;
}

}  // namespace rafter::test
