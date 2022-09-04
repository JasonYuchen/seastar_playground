//
// Created by jason on 2022/5/28.
//

#include "core/raft.hh"

#include <fmt/format.h>

#include <seastar/core/coroutine.hh>
#include <typeinfo>

#include "core/raft_log.hh"
#include "helper.hh"
#include "protocol/serializer.hh"
#include "test/base.hh"
#include "test/test_logdb.hh"
#include "test/util.hh"
#include "util/error.hh"
#include "util/seastarx.hh"

namespace {

using namespace rafter;
using namespace rafter::protocol;

using helper = rafter::test::core_helper;
using rafter::test::l;

std::unique_ptr<raft_config> new_test_config(
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

class raft_test : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  static sm* null() { return nullptr; }
  static sm* noop() { return new black_hole; }
  static core::raft& raft_cast(sm* s) {
    if (auto* rs = dynamic_cast<raft_sm*>(s); rs != nullptr) {
      return rs->raft();
    }
    throw rafter::util::panic("not a raft_sm");
  }

  static bool check_leader_transfer_state(
      core::raft& r,
      raft_role role,
      uint64_t leader,
      uint64_t target = group_id::INVALID_NODE) {
    EXPECT_EQ(helper::_role(r), role);
    EXPECT_EQ(helper::_leader_id(r), leader);
    EXPECT_EQ(helper::_leader_transfer_target(r), target);
    return !HasFailure();
  }

  static future<log_entry_vector> next_entries(sm* s, db* logdb) {
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

  static future<log_entry_vector> get_all_entries(sm* s) {
    if (auto* rs = dynamic_cast<raft_sm*>(s); rs != nullptr) {
      return get_all_entries(helper::_log(rs->raft()));
    }
    return make_exception_future<log_entry_vector>(
        rafter::util::panic("not a raft_sm"));
  }

  static future<log_entry_vector> get_all_entries(core::raft_log& rl) {
    log_entry_vector entries;
    co_await rl.query(rl.first_index(), entries, UINT64_MAX);
    co_return std::move(entries);
  }

  static std::unordered_set<uint64_t> get_all_nodes(core::raft& r) {
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

  static membership_ptr test_membership(sm* s) {
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

  static membership_ptr test_membership(const std::vector<uint64_t>& nodes) {
    auto members = make_lw_shared<membership>();
    for (auto id : nodes) {
      members->addresses[id] = "";
    }
    return members;
  }

  static snapshot_ptr test_snapshot(const std::vector<uint64_t>& nodes) {
    auto snap = make_lw_shared<snapshot>();
    snap->log_id = {11, 11};
    snap->membership = test_membership(nodes);
    return snap;
  }

  static future<snapshot_ptr> test_snapshot(
      db* logdb, uint64_t index, membership_ptr members) {
    auto snap = logdb->lr().get_snapshot();
    if (snap && snap->log_id.index >= index) {
      co_await coroutine::return_exception(
          rafter::util::snapshot_out_of_date());
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

  static message naive_proposal(
      uint64_t from, uint64_t to, std::string_view payload = "") {
    message m{.type = message_type::propose, .from = from, .to = to};
    m.entries.emplace_back().copy_of(payload);
    return m;
  }
};

RAFTER_TEST_F(raft_test, leader_transfer_to_up_to_date_node) {
  // transferring should succeed if the transferee has the most up-to-date log
  // entries when transfer starts.
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  auto& lead = raft_cast(nt.sms[1].get());
  ASSERT_EQ(helper::_leader_id(lead), 1) << "unexpected leader after election";
  // transfer leadership to 2, this message is sent to leader
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 2,
       .to = 1,
       .hint = {.low = 2}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::follower, 2));
  co_await nt.send(naive_proposal(1, 1));
  // transfer leadership back to 1 after replication
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 1,
       .to = 2,
       .hint = {.low = 1}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::leader, 1));
}

RAFTER_TEST_F(raft_test, leader_transfer_to_up_to_date_node_from_follower) {
  // transferring should succeed even the transfer message is sent to the
  // follower.
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  auto& lead = raft_cast(nt.sms[1].get());
  ASSERT_EQ(helper::_leader_id(lead), 1) << "unexpected leader after election";
  // transfer leadership to 2, this message is sent to follower
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 2,
       .to = 2,
       .hint = {.low = 2}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::follower, 2));
  co_await nt.send(naive_proposal(1, 1));
  // transfer leadership back to 1 after replication
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 1,
       .to = 1,
       .hint = {.low = 1}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::leader, 1));
}

RAFTER_TEST_F(raft_test, DISABLED_leader_transfer_with_check_quorum) {
  // FIXME: prevote is enabled by default and cannot pass this test
  // transferring leader still works even the current leader is still under its
  // leader lease
  auto nt = network({null(), null(), null()});
  for (uint64_t i = 1; i < 4; ++i) {
    auto& r = raft_cast(nt.sms[i].get());
    helper::_check_quorum(r) = true;
    helper::_randomized_election_timeout(r) = helper::_election_timeout(r) + i;
  }
  auto& r2 = raft_cast(nt.sms[2].get());
  // let r2 timeout
  for (uint64_t i = 0; i < helper::_election_timeout(r2); ++i) {
    co_await helper::tick(r2);
  }
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  auto& lead = raft_cast(nt.sms[1].get());
  ASSERT_EQ(helper::_leader_id(lead), 1) << "unexpected leader after election";
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 2,
       .to = 1,
       .hint = {.low = 2}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::follower, 2));
  co_await nt.send(naive_proposal(1, 1));
  // transfer leadership back to 1 after replication
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 1,
       .to = 2,
       .hint = {.low = 1}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::leader, 1));
}

RAFTER_TEST_F(raft_test, leader_transfer_to_slow_follower) {
  // transferring leadership to a follower without sufficient log will fail.
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  nt.isolate(3);
  co_await nt.send(naive_proposal(1, 1));
  nt.recover();
  auto& lead = raft_cast(nt.sms[1].get());
  ASSERT_EQ(helper::_remotes(lead)[3].match, 1);
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 3,
       .to = 1,
       .hint = {.low = 3}});
  // leader is still under transferring state with target = 3
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::leader, 1, 3));
  helper::abort_leader_transfer(lead);
  co_await nt.send(naive_proposal(1, 1));
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 3,
       .to = 1,
       .hint = {.low = 3}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::follower, 3));
}

RAFTER_TEST_F(raft_test, leader_transfer_after_snapshot) {
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  nt.isolate(3);
  co_await nt.send(naive_proposal(1, 1));
  auto& lead = raft_cast(nt.sms[1].get());
  co_await next_entries(nt.sms[1].get(), nt.dbs[1].get());
  auto m = test_membership(nt.sms[1].get());
  auto s = co_await test_snapshot(
      nt.dbs[1].get(), helper::_log(lead).processed(), m);
  nt.dbs[1]->lr().create_snapshot(s);
  co_await nt.dbs[1]->lr().apply_compaction(helper::_log(lead).processed());
  nt.recover();
  ASSERT_EQ(helper::_remotes(lead)[3].match, 1);
  // transfer leadership to 3 when node 3 is lack of snapshot.
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 3,
       .to = 1,
       .hint = {.low = 3}});
  // trigger a snapshot for node 3.
  co_await nt.send({.type = message_type::heartbeat_resp, .from = 3, .to = 1});
  // now leader is 3.
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::follower, 3));
}

RAFTER_TEST_F(raft_test, leader_transfer_to_self) {
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  auto& lead = raft_cast(nt.sms[1].get());
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 1,
       .to = 1,
       .hint = {.low = 1}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::leader, 1));
}

RAFTER_TEST_F(raft_test, leader_transfer_to_non_existing_node) {
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  auto& lead = raft_cast(nt.sms[1].get());
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 4,
       .to = 1,
       .hint = {.low = 4}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::leader, 1));
}

RAFTER_TEST_F(raft_test, leader_transfer_timeout) {
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  nt.isolate(3);
  auto& lead = raft_cast(nt.sms[1].get());
  // Transfer leadership to isolated node, wait for timeout.
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 3,
       .to = 1,
       .hint = {.low = 3}});
  ASSERT_EQ(helper::_leader_transfer_target(lead), 3);
  for (uint64_t i = 0; i < helper::_heartbeat_timeout(lead); ++i) {
    co_await helper::tick(lead);
  }
  ASSERT_EQ(helper::_leader_transfer_target(lead), 3);
  for (uint64_t i = 0; i < helper::_election_timeout(lead); ++i) {
    co_await helper::tick(lead);
  }
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::leader, 1));
}

RAFTER_TEST_F(raft_test, leader_transfer_ignore_proposal) {
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  nt.isolate(3);
  auto& lead = raft_cast(nt.sms[1].get());
  // Transfer leadership to isolated node to let transfer pending, then propose.
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 3,
       .to = 1,
       .hint = {.low = 3}});
  ASSERT_EQ(helper::_leader_transfer_target(lead), 3);
  co_await nt.send(naive_proposal(1, 1));
  uint64_t matched = helper::_remotes(lead)[2].match;
  co_await nt.send(naive_proposal(1, 1));
  ASSERT_EQ(helper::_remotes(lead)[2].match, matched);
}

RAFTER_TEST_F(raft_test, leader_transfer_receive_higher_term_vote) {
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  nt.isolate(3);
  auto& lead = raft_cast(nt.sms[1].get());
  // Transfer leadership to isolated node to let transfer pending.
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 3,
       .to = 1,
       .hint = {.low = 3}});
  ASSERT_EQ(helper::_leader_transfer_target(lead), 3);
  co_await nt.send(
      {.type = message_type::election,
       .from = 2,
       .to = 2,
       .term = 2,
       .lid = {.index = 1}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::follower, 2));
}

RAFTER_TEST_F(raft_test, leader_transfer_remove_node) {
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  nt.ignore(message_type::timeout_now);
  auto& lead = raft_cast(nt.sms[1].get());
  // The leadTransferee is removed when leadership transferring.
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 3,
       .to = 1,
       .hint = {.low = 3}});
  ASSERT_EQ(helper::_leader_transfer_target(lead), 3);
  co_await helper::remove_node(lead, 3);
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::leader, 1));
}

RAFTER_TEST_F(raft_test, leader_transfer_cannot_override_existing_one) {
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  nt.isolate(3);
  auto& lead = raft_cast(nt.sms[1].get());
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 3,
       .to = 1,
       .hint = {.low = 3}});
  ASSERT_EQ(helper::_leader_transfer_target(lead), 3);
  uint64_t election_tick = helper::_election_tick(lead);
  // should be ignored due to ongoing leader transfer
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 1,
       .to = 1,
       .hint = {.low = 1}});
  ASSERT_EQ(helper::_leader_transfer_target(lead), 3);
  ASSERT_EQ(helper::_election_tick(lead), election_tick);
}

RAFTER_TEST_F(raft_test, leader_transfer_second_transfer_to_same_node) {
  auto nt = network({null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  nt.isolate(3);
  auto& lead = raft_cast(nt.sms[1].get());
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 3,
       .to = 1,
       .hint = {.low = 3}});
  ASSERT_EQ(helper::_leader_transfer_target(lead), 3);
  for (uint64_t i = 0; i < helper::_heartbeat_timeout(lead); ++i) {
    co_await helper::tick(lead);
  }
  // second leader transfer request targeting same node
  co_await nt.send(
      {.type = message_type::leader_transfer,
       .from = 3,
       .to = 1,
       .hint = {.low = 3}});
  // should not extend the timeout while the first one is pending.
  auto t = helper::_election_timeout(lead) - helper::_heartbeat_timeout(lead);
  for (uint64_t i = 0; i < t; ++i) {
    co_await helper::tick(lead);
  }
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::leader, 1));
}

RAFTER_TEST_F(raft_test, remote_resume_by_heartbeat_resp) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 5, 1)};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  helper::_remotes(r->raft())[2].retry_to_wait();
  co_await r->raft().handle(
      {.type = message_type::leader_heartbeat, .from = 1, .to = 1});
  ASSERT_EQ(helper::_remotes(r->raft())[2].state, core::remote::state::wait);
  helper::_remotes(r->raft())[2].become_replicate();
  co_await r->raft().handle(
      {.type = message_type::heartbeat_resp, .from = 2, .to = 1});
  ASSERT_NE(helper::_remotes(r->raft())[2].state, core::remote::state::wait);
}

RAFTER_TEST_F(raft_test, remote_paused) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 5, 1)};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  co_await r->raft().handle(naive_proposal(1, 1));
  co_await r->raft().handle(naive_proposal(1, 1));
  co_await r->raft().handle(naive_proposal(1, 1));
  ASSERT_EQ(r->read_messages().size(), 1);
}

RAFTER_TEST_F(raft_test, leader_election) {
  struct {
    network nt;
    raft_role exp_role;
    uint64_t exp_term;
  } tests[] = {
      {network{{null(), null(), null()}}, raft_role::leader, 1},
      {network{{null(), null(), noop()}}, raft_role::leader, 1},

      // FIXME(jyc): with prevote enabled, these tests will fail
      // {network{{null(), noop(), noop()}}, raft_role::candidate, 1},
      // {network{{null(), noop(), noop(), null()}}, raft_role::candidate, 1},

      // with prevote disabled, these tests will fail
      {network{{null(), noop(), noop()}}, raft_role::pre_candidate, 0},
      {network{{null(), noop(), noop(), null()}}, raft_role::pre_candidate, 0},

      {network{{null(), noop(), noop(), null(), null()}}, raft_role::leader, 1},

      // logs not up to date, receive 3 rejections and become follower
      {network{
           {raft_sm::make_with_entries({{1, 1}}),
            raft_sm::make_with_entries({{1, 1}, {1, 2}}),
            raft_sm::make_with_entries({{1, 1}, {1, 2}}),
            raft_sm::make_with_entries({{1, 1}, {1, 2}}),
            null()}},
       raft_role::follower,
       1},

      // receive higher term from peers, become follower
      {network{
           {null(),
            raft_sm::make_with_entries({{1, 1}}),
            raft_sm::make_with_entries({{1, 1}}),
            raft_sm::make_with_entries({{1, 1}, {1, 2}}),
            null()}},
       raft_role::follower,
       1},
  };
  for (auto& t : tests) {
    co_await t.nt.send({.type = message_type::election, .from = 1, .to = 1});
    auto& sm = raft_cast(t.nt.sms[1].get());
    EXPECT_EQ(helper::_role(sm), t.exp_role) << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_term(sm), t.exp_term) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, leader_cycle) {
  // each node can campaign and be elected in turn. the elections work when not
  // starting from a clean state as they do in leader_election tests
  auto nt = network({null(), null(), null()});
  for (uint64_t campaigner = 1; campaigner <= 3; campaigner++) {
    co_await nt.send(
        {.type = message_type::election, .from = campaigner, .to = campaigner});
    for (auto& [id, peer] : nt.sms) {
      auto& r = raft_cast(peer.get());
      if (helper::_gid(r).node == campaigner) {
        EXPECT_EQ(helper::_role(r), raft_role::leader) << campaigner;
      } else {
        EXPECT_EQ(helper::_role(r), raft_role::follower) << campaigner;
      }
    }
  }
  co_return;
}

RAFTER_TEST_F(raft_test, election_overwrite_newer_logs) {
  auto nt = network({
      raft_sm::make_with_entries({{1, 1}}),
      raft_sm::make_with_entries({{1, 1}}),
      raft_sm::make_with_entries({{1, 1}, {2, 2}}),
      raft_sm::make_with_vote(2, 3),
      raft_sm::make_with_vote(2, 3),
  });
  // node 1 campaigns but fails due to a quorum of nodes knows about the
  // election that already happened at term 2
  // node 1 is pushed to term 2
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  auto& r1 = raft_cast(nt.sms[1].get());
  ASSERT_EQ(helper::_role(r1), raft_role::follower);
  ASSERT_EQ(helper::_term(r1), 2);
  // node 1 campaigns again with higher term, succeeds
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  ASSERT_EQ(helper::_role(r1), raft_role::leader);
  ASSERT_EQ(helper::_term(r1), 3);
  // all nodes agree on a raft log with ids: [{1, 1}, {3, 2}]
  for (auto& [id, peer] : nt.sms) {
    auto entries = co_await get_all_entries(peer.get());
    EXPECT_EQ(entries.size(), 2) << id;
    EXPECT_EQ(entries[0].lid.term, 1);
    EXPECT_EQ(entries[1].lid.term, 3);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, vote_from_any_state) {
  // TODO(jyc): support observer/witness
  std::vector<raft_role> roles{
      raft_role::follower,
      raft_role::pre_candidate,
      raft_role::candidate,
      raft_role::leader,
      /* raft_role::observer, */
      /* raft_role::witness, */
  };
  std::vector<message_type> vote_types{
      message_type::request_prevote,
      message_type::request_vote,
  };
  for (auto vote_type : vote_types) {
    for (auto role : roles) {
      auto err = fmt::format("{}:{}", vote_type, role);
      std::unique_ptr<raft_sm> rsm{raft_sm::make(1, {1, 2, 3}, 10, 1)};
      auto& r = rsm->raft();
      helper::_term(r) = 1;
      if (role == raft_role::follower) {
        helper::become_follower(r, helper::_term(r), 3, true);
      } else if (role == raft_role::pre_candidate) {
        helper::become_pre_candidate(r);
      } else if (role == raft_role::candidate) {
        helper::become_candidate(r);
      } else if (role == raft_role::leader) {
        helper::become_candidate(r);
        co_await helper::become_leader(r);
      } else if (role == raft_role::observer) {
        /* helper::become_observer(r, helper::_term(r), 3); */
      } else if (role == raft_role::witness) {
        /* helper::become_witness(r, helper::_term(r), 3); */
      }
      auto orig_term = helper::_term(r);
      auto new_term = orig_term + 1;
      message msg{
          .type = vote_type,
          .from = 2,
          .to = 1,
          .term = new_term,
          .lid = {new_term, 42},
      };
      co_await r.handle(msg);
      EXPECT_EQ(r.messages().size(), 1) << err;
      auto& resp = r.messages().front();
      if (vote_type == message_type::request_vote) {
        EXPECT_EQ(resp.type, message_type::request_vote_resp) << err;
      } else {
        EXPECT_EQ(resp.type, message_type::request_prevote_resp) << err;
      }
      EXPECT_FALSE(resp.reject) << err;
      if (vote_type == message_type::request_vote) {
        EXPECT_EQ(helper::_role(r), raft_role::follower) << err;
        EXPECT_EQ(helper::_term(r), new_term) << err;
        EXPECT_EQ(helper::_vote(r), 2) << err;
      } else {  // request_prevote, nothing change
        EXPECT_EQ(helper::_role(r), role) << err;
        EXPECT_EQ(helper::_term(r), orig_term) << err;
        // if role is follower or pre_candidate, raft hasn't voted yet.
        // if role is candidate or leader, it's voted for itself.
        if (helper::_vote(r) != group_id::INVALID_NODE) {
          EXPECT_EQ(helper::_vote(r), 1) << err;
        }
      }
    }
  }
  co_return;
}

RAFTER_TEST_F(raft_test, single_node_commit) {
  auto nt = network({null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  co_await nt.send(naive_proposal(1, 1));
  co_await nt.send(naive_proposal(1, 1));
  ASSERT_EQ(helper::_log(raft_cast(nt.sms[1].get())).committed(), 3);
}

RAFTER_TEST_F(raft_test, cannot_commit_without_new_term_entry) {
  // entries cannot be committed when leader changes, no new proposal comes in
  // and ChangeTerm proposal is filtered.
  auto nt = network({null(), null(), null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  nt.cut(1, 3);
  nt.cut(1, 4);
  nt.cut(1, 5);
  co_await nt.send(naive_proposal(1, 1));
  co_await nt.send(naive_proposal(1, 1));
  auto& r1 = raft_cast(nt.sms[1].get());
  ASSERT_EQ(helper::_log(r1).committed(), 1);
  nt.recover();
  // avoid committing change term proposal
  nt.ignore(message_type::replicate);
  co_await nt.send({.type = message_type::election, .from = 2, .to = 2});
  auto& r2 = raft_cast(nt.sms[2].get());
  // no log entries from previous term should be committed
  ASSERT_EQ(helper::_log(r2).committed(), 1);
  nt.recover();
  // send heartbeat to reset wait
  co_await nt.send(
      {.type = message_type::leader_heartbeat, .from = 2, .to = 2});
  // append an entry at current term
  co_await nt.send(naive_proposal(2, 2));
  // committed index should be advanced
  ASSERT_EQ(helper::_log(r2).committed(), 5);
}

RAFTER_TEST_F(raft_test, commit_without_new_term_entry) {
  //  entries could be committed when leader changes, no new proposal comes in.
  auto nt = network({null(), null(), null(), null(), null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  nt.cut(1, 3);
  nt.cut(1, 4);
  nt.cut(1, 5);
  co_await nt.send(naive_proposal(1, 1));
  co_await nt.send(naive_proposal(1, 1));
  auto& r1 = raft_cast(nt.sms[1].get());
  ASSERT_EQ(helper::_log(r1).committed(), 1);
  nt.recover();
  // node 1 will be elected as a new leader with term 2, after append a change
  // term entry from the current term, all entries should be committed
  co_await nt.send({.type = message_type::election, .from = 2, .to = 2});
  ASSERT_EQ(helper::_log(r1).committed(), 4);
}

RAFTER_TEST_F(raft_test, DISABLED_dueling_candidates) {
  // disable this test since prevote is hard-coded enabled
  auto* a = raft_sm::make(1, {1, 2, 3}, 10, 1);
  auto* b = raft_sm::make(2, {1, 2, 3}, 10, 1);
  auto* c = raft_sm::make(3, {1, 2, 3}, 10, 1);
  auto nt = network({a, b, c});
  nt.cut(1, 3);
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  co_await nt.send({.type = message_type::election, .from = 3, .to = 3});
  // 1 becomes leader since it receives votes from 1 and 2
  ASSERT_EQ(helper::_role(a->raft()), raft_role::leader);
  // 3 stays as candidate since it receives a vote from 3 and a rejection from 2
  // FIXME(jyc): in prevote mode, 3 should be reverted to follower
  ASSERT_EQ(helper::_role(c->raft()), raft_role::candidate);
  nt.recover();
  // candidate 3 now increases its term and tries to vote again
  // we expect it to disrupt the leader 1 since it has a higher term
  // 3 will be follower again since both 1 and 2 rejects its vote request since
  // 3 does not have a long enough log
  co_await nt.send({.type = message_type::election, .from = 3, .to = 3});
  struct {
    raft_sm* sm;
    raft_role role;
    uint64_t term;
    uint64_t exp_committed;
    uint64_t exp_processed;
    std::vector<log_id> exp_entries;
  } tests[] = {
      {a, raft_role::follower, 2, 1, 0, {{1, 1}}},
      {b, raft_role::follower, 2, 1, 0, {{1, 1}}},
      {c, raft_role::follower, 2, 0, 0, {}},
  };
  for (auto& t : tests) {
    auto& r = t.sm->raft();
    EXPECT_EQ(helper::_role(r), t.role) << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_term(r), t.term) << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_log(r).committed(), t.exp_committed)
        << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_log(r).processed(), t.exp_processed)
        << CASE_INDEX(t, tests);
    EXPECT_EQ(
        co_await get_all_entries(helper::_log(r)),
        rafter::test::util::new_entries(t.exp_entries))
        << CASE_INDEX(t, tests);
  }
}

RAFTER_TEST_F(raft_test, dueling_pre_candidates) {
  auto* a = raft_sm::make(1, {1, 2, 3}, 10, 1);
  auto* b = raft_sm::make(2, {1, 2, 3}, 10, 1);
  auto* c = raft_sm::make(3, {1, 2, 3}, 10, 1);
  auto nt = network({a, b, c});
  nt.cut(1, 3);
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  co_await nt.send({.type = message_type::election, .from = 3, .to = 3});
  // 1 becomes leader since it receives votes from 1 and 2
  ASSERT_EQ(helper::_role(a->raft()), raft_role::leader);
  // 3 is reverted to follower
  ASSERT_EQ(helper::_role(c->raft()), raft_role::follower);
  nt.recover();
  // 3 now increases its term and tries to vote again.
  // It does not disrupt the leader.
  co_await nt.send({.type = message_type::election, .from = 3, .to = 3});
  struct {
    raft_sm* sm;
    raft_role role;
    uint64_t term;
    uint64_t exp_committed;
    uint64_t exp_processed;
    std::vector<log_id> exp_entries;
  } tests[] = {
      {a, raft_role::leader, 1, 1, 0, {{1, 1}}},
      {b, raft_role::follower, 1, 1, 0, {{1, 1}}},
      {c, raft_role::follower, 1, 0, 0, {}},
  };
  for (auto& t : tests) {
    auto& r = t.sm->raft();
    EXPECT_EQ(helper::_role(r), t.role) << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_term(r), t.term) << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_log(r).committed(), t.exp_committed)
        << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_log(r).processed(), t.exp_processed)
        << CASE_INDEX(t, tests);
    EXPECT_EQ(
        co_await get_all_entries(helper::_log(r)),
        rafter::test::util::new_entries(t.exp_entries))
        << CASE_INDEX(t, tests);
  }
}

RAFTER_TEST_F(raft_test, candidate_concede) {
  auto nt = network({null(), null(), null()});
  nt.isolate(1);
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  co_await nt.send({.type = message_type::election, .from = 3, .to = 3});
  nt.recover();
  // send heartbeat to reset wait
  co_await nt.send(
      {.type = message_type::leader_heartbeat, .from = 3, .to = 3});
  // send a proposal to 3 to flush out a msg append to 1
  co_await nt.send(naive_proposal(3, 3));
  // send heartbeat to flush out commit
  co_await nt.send(
      {.type = message_type::leader_heartbeat, .from = 3, .to = 3});
  auto& r1 = raft_cast(nt.sms[1].get());
  ASSERT_EQ(helper::_role(r1), raft_role::follower);
  ASSERT_EQ(helper::_term(r1), 1);
  for (auto& [id, p] : nt.sms) {
    auto& r = raft_cast(p.get());
    EXPECT_EQ(helper::_log(r).committed(), 2);
    EXPECT_EQ(helper::_log(r).processed(), 0);
    EXPECT_EQ(
        co_await get_all_entries(helper::_log(r)),
        rafter::test::util::new_entries({{1, 1}, {1, 2}}));
  }
}

RAFTER_TEST_F(raft_test, single_node_candidate) {
  auto nt = network({null()});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  auto& r = raft_cast(nt.sms[1].get());
  ASSERT_EQ(helper::_role(r), raft_role::leader);
}

RAFTER_TEST_F(raft_test, old_messages) {
  auto nt = network({null(), null(), null()});
  // make 0 leader @ term 3
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  co_await nt.send({.type = message_type::election, .from = 2, .to = 2});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  // pretend we're an old leader trying to make progress
  // this entry is expected to be ignored.
  co_await nt.send(
      {.type = message_type::replicate,
       .from = 2,
       .to = 1,
       .term = 2,
       .entries = rafter::test::util::new_entries({{2, 3}})});
  // commit a new entry
  co_await nt.send(naive_proposal(1, 1, "some data"));
  auto ents = rafter::test::util::new_entries({{1, 1}, {2, 2}, {3, 3}, {3, 4}});
  ents.back().copy_of("some data");
  for (auto& [id, p] : nt.sms) {
    auto& r = raft_cast(p.get());
    EXPECT_EQ(helper::_log(r).committed(), 4);
    EXPECT_EQ(helper::_log(r).processed(), 0);
    EXPECT_EQ(co_await get_all_entries(helper::_log(r)), ents);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, proposal) {
  struct {
    network nt;
    bool success;
  } tests[] = {
      {network{{null(), null(), null()}}, true},
      {network{{null(), null(), noop()}}, true},
      {network{{null(), noop(), noop()}}, false},
      {network{{null(), noop(), noop(), null()}}, false},
      {network{{null(), noop(), noop(), null(), null()}}, true},
  };
  for (auto& t : tests) {
    co_await t.nt.send({.type = message_type::election, .from = 1, .to = 1});
    co_await t.nt.send(naive_proposal(1, 1, "some data"));
    log_entry_vector ents;
    if (t.success) {
      ents = rafter::test::util::new_entries({{1, 1}, {1, 2}});
      ents.back().copy_of("some data");
    }
    for (auto& [id, p] : t.nt.sms) {
      if (!p->is_raft()) {
        continue;
      }
      auto& r = raft_cast(p.get());
      EXPECT_EQ(helper::_log(r).committed(), t.success ? 2 : 0);
      EXPECT_EQ(helper::_log(r).processed(), 0);
      EXPECT_EQ(co_await get_all_entries(helper::_log(r)), ents);
    }
    auto& r1 = raft_cast(t.nt.sms[1].get());
    // since we enable prevote by default, case #3,#4 will not increase term as
    // they cannot become candidate
    if (helper::_role(r1) == raft_role::leader) {
      EXPECT_EQ(helper::_term(r1), 1) << CASE_INDEX(t, tests);
    } else {
      EXPECT_EQ(helper::_term(r1), 0) << CASE_INDEX(t, tests);
    }
  }
  co_return;
}

RAFTER_TEST_F(raft_test, proposal_by_proxy) {
  struct {
    network nt;
  } tests[] = {
      {network{{null(), null(), null()}}},
      {network{{null(), null(), noop()}}},
  };
  auto ents = rafter::test::util::new_entries({{1, 1}, {1, 2}});
  ents.back().copy_of("some data");
  for (auto& t : tests) {
    co_await t.nt.send({.type = message_type::election, .from = 1, .to = 1});
    co_await t.nt.send(naive_proposal(2, 2, "some data"));
    for (auto& [id, p] : t.nt.sms) {
      if (!p->is_raft()) {
        continue;
      }
      auto& r = raft_cast(p.get());
      EXPECT_EQ(helper::_log(r).committed(), 2);
      EXPECT_EQ(helper::_log(r).processed(), 0);
      EXPECT_EQ(co_await get_all_entries(helper::_log(r)), ents);
    }
    auto& r1 = raft_cast(t.nt.sms[1].get());
    EXPECT_EQ(helper::_term(r1), 1) << CASE_INDEX(t, tests);
  }
}

RAFTER_TEST_F(raft_test, commit) {
  struct {
    std::vector<uint64_t> matches;
    std::vector<log_id> logs;
    uint64_t term;
    uint64_t exp_committed;
  } tests[] = {
      // single
      {{1}, {{1, 1}}, 1, 1},
      {{1}, {{1, 1}}, 2, 0},
      {{2}, {{1, 1}, {2, 2}}, 2, 2},
      {{1}, {{2, 1}}, 2, 1},

      // odd
      // quorum 1, term according to log is 1, node's term is 1, committed -> 1
      {{2, 1, 1}, {{1, 1}, {2, 2}}, 1, 1},
      // quorum 1, term according to log is 1, node's term is 2, committed = 0
      {{2, 1, 1}, {{1, 1}, {1, 2}}, 2, 0},
      {{2, 1, 2}, {{1, 1}, {2, 2}}, 2, 2},
      {{2, 1, 2}, {{1, 1}, {1, 2}}, 2, 0},

      // even
      {{2, 1, 1, 1}, {{1, 1}, {2, 2}}, 1, 1},
      {{2, 1, 1, 1}, {{1, 1}, {1, 2}}, 2, 0},
      {{2, 1, 1, 2}, {{1, 1}, {2, 2}}, 1, 1},
      {{2, 1, 1, 2}, {{1, 1}, {1, 2}}, 2, 0},
      {{2, 1, 2, 2}, {{1, 1}, {2, 2}}, 2, 2},
      {{2, 1, 2, 2}, {{1, 1}, {1, 2}}, 2, 0},
  };
  for (auto& t : tests) {
    auto logdb = std::make_unique<db>();
    co_await logdb->append(test::util::new_entries(t.logs));
    logdb->set_state({.term = t.term});
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1}, 5, 1, std::move(logdb))};
    for (uint64_t j = 0; j < t.matches.size(); ++j) {
      helper::_remotes(r->raft())[j + 1] =
          core::remote{.match = t.matches[j], .next = t.matches[j] + 1};
    }
    helper::_role(r->raft()) = raft_role::leader;
    co_await helper::try_commit(r->raft());
    EXPECT_EQ(helper::_log(r->raft()).committed(), t.exp_committed)
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, election_timeout) {
  struct {
    uint64_t elapse;
    double probability;
    bool round;
  } tests[] = {
      {5, 0.0, false},
      {10, 0.1, true},
      {13, 0.4, true},
      {15, 0.6, true},
      {18, 0.9, true},
      {20, 1.0, false},
  };
  for (auto& t : tests) {
    auto logdb = std::make_unique<db>();
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1}, 10, 1, std::move(logdb))};
    helper::_election_tick(r->raft()) = t.elapse;
    uint64_t c = 0;
    for (uint64_t j = 0; j < 10000; ++j) {
      helper::set_randomized_election_timeout(r->raft());
      if (helper::time_to_elect(r->raft())) {
        c++;
      }
    }
    auto got = static_cast<double>(c) / 10000.0;
    if (t.round) {
      got = std::floor(got * 10 + 0.5) / 10.0;
    }
    EXPECT_EQ(got, t.probability) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, step_ignore_old_term_msg) {
  auto logdb = std::make_unique<db>();
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1}, 10, 1, std::move(logdb))};
  helper::_term(r->raft()) = 2;
  r->read_messages();  // drain the msg
  auto f = r->handle({.type = message_type::replicate, .term = 1});
  ASSERT_TRUE(f.available());
  auto msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 1);
  // higher term node will reply a noop to step down the old leader
  ASSERT_EQ(msgs[0].type, message_type::noop);
}

RAFTER_TEST_F(raft_test, replicate) {
  struct {
    uint64_t term;
    uint64_t commit;
    log_id lid;
    std::vector<log_id> entries;
    uint64_t exp_index;
    uint64_t exp_commit;
    bool exp_reject;
  } tests[] = {
      // the node initially has log entries {1, 1}, {2, 2}
      // previous log mismatch
      {2, 3, {3, 2}, {}, 2, 0, true},
      // previous log not exist
      {2, 3, {3, 3}, {}, 2, 0, true},
      // accept and advance commit index
      {2, 1, {1, 1}, {}, 2, 1, false},
      // conflict found, delete all existing logs, append the new one
      {2, 1, {0, 0}, {{2, 1}}, 1, 1, false},
      // no conflict, both new logs appended, leader's commit determines the
      // commit value, last index in log is 4
      {2, 3, {2, 2}, {{2, 3}, {2, 4}}, 4, 3, false},
      // no conflict, one log appended, last index in log determines the commit
      // value 3, last index in log is 3
      {2, 4, {2, 2}, {{2, 3}}, 3, 3, false},
      // no conflict, no new entry. last index in log determines the commit
      // value 2, last index in log is 2.
      {2, 4, {1, 1}, {{2, 2}}, 2, 2, false},
      // match entry 1, commit up to last new entry 1
      {1, 3, {1, 1}, {}, 2, 1, false},
      // match entry 1, commit up to last new entry 2
      {1, 3, {1, 1}, {{2, 2}}, 2, 2, false},
      // match entry 2, commit up to last new entry 2
      {2, 3, {2, 2}, {}, 2, 2, false},
      // commit up to log.last()
      {2, 4, {2, 2}, {}, 2, 2, false},
  };
  for (auto& t : tests) {
    auto logdb = std::make_unique<db>();
    co_await logdb->append(test::util::new_entries({{1, 1}, {2, 2}}));
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1}, 10, 1, std::move(logdb))};
    helper::become_follower(r->raft(), 2, group_id::INVALID_NODE, true);
    message m{
        .type = message_type::replicate,
        .term = t.term,
        .lid = t.lid,
        .commit = t.commit,
        .entries = test::util::new_entries(t.entries),
    };
    co_await helper::node_replicate(r->raft(), m);
    EXPECT_EQ(helper::_log(r->raft()).last_index(), t.exp_index)
        << CASE_INDEX(t, tests);
    EXPECT_EQ(helper::_log(r->raft()).committed(), t.exp_commit)
        << CASE_INDEX(t, tests);
    auto msgs = r->read_messages();
    EXPECT_EQ(msgs.size(), 1) << CASE_INDEX(t, tests);
    EXPECT_EQ(msgs[0].reject, t.exp_reject) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, heartbeat) {
  uint64_t commit = 2;
  struct {
    message m;
    uint64_t exp_commit;
  } tests[] = {
      // advance commit by heartbeat
      {{.type = message_type::heartbeat,
        .from = 2,
        .to = 1,
        .term = 2,
        .commit = commit + 1},
       commit + 1},
      // commit cannot be decreased
      {{.type = message_type::heartbeat,
        .from = 2,
        .to = 1,
        .term = 2,
        .commit = commit - 1},
       commit},
  };
  for (auto& t : tests) {
    auto logdb = std::make_unique<db>();
    co_await logdb->append(test::util::new_entries({{1, 1}, {2, 2}, {3, 3}}));
    std::unique_ptr<raft_sm> r{
        raft_sm::make(1, {1, 2}, 5, 1, std::move(logdb))};
    helper::become_follower(r->raft(), 2, 2, true);
    helper::_log(r->raft()).commit(commit);
    co_await helper::node_heartbeat(r->raft(), t.m);
    EXPECT_EQ(helper::_log(r->raft()).committed(), t.exp_commit)
        << CASE_INDEX(t, tests);
    auto msgs = r->read_messages();
    EXPECT_EQ(msgs.size(), 1) << CASE_INDEX(t, tests);
    EXPECT_EQ(msgs[0].type, message_type::heartbeat_resp)
        << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, heartbeat_resp) {
  auto logdb = std::make_unique<db>();
  co_await logdb->append(test::util::new_entries({{1, 1}, {2, 2}, {3, 3}}));
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 5, 1, std::move(logdb))};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  helper::_log(r->raft()).commit(helper::_log(r->raft()).last_index());
  // A heartbeat response from a node that is behind; re-send replicate
  co_await r->handle({.type = message_type::heartbeat_resp, .from = 2});
  auto msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0].type, message_type::replicate);
  // A second heartbeat response generates another replicate re-send
  co_await r->handle({.type = message_type::heartbeat_resp, .from = 2});
  msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0].type, message_type::replicate);
  // Once we have an replicate_resp, heartbeats no longer send replicate.
  co_await r->handle(
      {.type = message_type::replicate_resp,
       .from = 2,
       .lid = {0, msgs[0].lid.index + msgs[0].entries.size()}});
  (void)r->read_messages();
  co_await r->handle({.type = message_type::heartbeat_resp, .from = 2});
  msgs = r->read_messages();
  ASSERT_TRUE(msgs.empty());
}

RAFTER_TEST_F(raft_test, replicate_resp_wait_reset) {
  auto logdb = std::make_unique<db>();
  std::unique_ptr<raft_sm> r{
      raft_sm::make(1, {1, 2, 3}, 5, 1, std::move(logdb))};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  co_await helper::broadcast_replicate(r->raft());
  (void)r->read_messages();
  // Node 2 acks the first entry, making it committed.
  co_await r->handle(
      {.type = message_type::replicate_resp, .from = 2, .lid = {0, 1}});
  ASSERT_EQ(helper::_log(r->raft()).committed(), 1);
  (void)r->read_messages();
  co_await r->handle(
      {.type = message_type::propose,
       .from = 1,
       .entries = test::util::new_entries({{1, 1}})});
  auto msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0].type, message_type::replicate);
  ASSERT_EQ(msgs[0].to, 2);
  ASSERT_EQ(msgs[0].entries.size(), 1);
  ASSERT_EQ(msgs[0].entries[0].lid.index, 2);
  // initially node 3 is in the probe state, the received replicate_resp message
  // makes the node to enter replicate state.
  ASSERT_EQ(helper::_remotes(r->raft())[3].state, core::remote::state::wait);
  co_await r->handle(
      {.type = message_type::replicate_resp, .from = 3, .lid = {0, 1}});
  // Node 3 acks the first entry. This releases the wait and entry 2 is sent.
  ASSERT_EQ(
      helper::_remotes(r->raft())[3].state, core::remote::state::replicate);
  msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0].type, message_type::replicate);
  ASSERT_EQ(msgs[0].to, 3);
  ASSERT_EQ(msgs[0].entries.size(), 1);
  ASSERT_EQ(msgs[0].entries[0].lid.index, 2);
}

RAFTER_TEST_F(raft_test, recv_msg_vote) {
  struct {
    raft_role role;
    log_id lid;
    uint64_t vote_for;
    bool exp_reject;
  } tests[] = {
      {raft_role::follower, {0, 0}, group_id::INVALID_NODE, true},
      {raft_role::follower, {1, 0}, group_id::INVALID_NODE, true},
      {raft_role::follower, {2, 0}, group_id::INVALID_NODE, true},
      {raft_role::follower, {3, 0}, group_id::INVALID_NODE, false},
      {raft_role::follower, {0, 1}, group_id::INVALID_NODE, true},
      {raft_role::follower, {1, 1}, group_id::INVALID_NODE, true},
      {raft_role::follower, {2, 1}, group_id::INVALID_NODE, true},
      {raft_role::follower, {3, 1}, group_id::INVALID_NODE, false},
      {raft_role::follower, {0, 2}, group_id::INVALID_NODE, true},
      {raft_role::follower, {1, 2}, group_id::INVALID_NODE, true},
      {raft_role::follower, {2, 2}, group_id::INVALID_NODE, false},
      {raft_role::follower, {3, 2}, group_id::INVALID_NODE, false},
      {raft_role::follower, {0, 3}, group_id::INVALID_NODE, true},
      {raft_role::follower, {1, 3}, group_id::INVALID_NODE, true},
      {raft_role::follower, {2, 3}, group_id::INVALID_NODE, false},
      {raft_role::follower, {3, 3}, group_id::INVALID_NODE, false},
      {raft_role::follower, {2, 3}, 2, false},
      {raft_role::follower, {2, 3}, 1, true},
      {raft_role::leader, {3, 3}, 1, true},
      {raft_role::pre_candidate, {3, 3}, 1, true},
      {raft_role::candidate, {3, 3}, 1, true},
  };
  for (auto& t : tests) {
    auto logdb = std::make_unique<db>();
    co_await logdb->append(test::util::new_entries({{2, 1}, {2, 2}}));
    std::unique_ptr<raft_sm> r{
        raft_sm::make(1, {1, 2}, 10, 1, std::move(logdb))};
    helper::_role(r->raft()) = t.role;
    helper::_vote(r->raft()) = t.vote_for;
    co_await r->handle(
        {.type = message_type::request_vote, .from = 2, .lid = t.lid});
    auto msgs = r->read_messages();
    EXPECT_EQ(msgs.size(), 1) << CASE_INDEX(t, tests);
    EXPECT_EQ(msgs[0].reject, t.exp_reject) << CASE_INDEX(t, tests);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, role_transition) {
  auto NO_LEADER = group_id::INVALID_NODE;
  struct {
    raft_role from;
    raft_role to;
    bool exp_allow;
    uint64_t exp_term;
    uint64_t exp_lead;
  } tests[] = {
      {raft_role::follower, raft_role::follower, true, 1, NO_LEADER},
      {raft_role::follower, raft_role::pre_candidate, true, 0, NO_LEADER},
      {raft_role::follower, raft_role::candidate, true, 1, NO_LEADER},
      {raft_role::pre_candidate, raft_role::follower, true, 0, NO_LEADER},
      {raft_role::pre_candidate, raft_role::pre_candidate, true, 0, NO_LEADER},
      {raft_role::pre_candidate, raft_role::candidate, true, 1, NO_LEADER},
      {raft_role::pre_candidate, raft_role::leader, false, 0, 1},
      {raft_role::candidate, raft_role::follower, true, 0, NO_LEADER},
      {raft_role::candidate, raft_role::pre_candidate, true, 0, NO_LEADER},
      {raft_role::candidate, raft_role::candidate, true, 1, NO_LEADER},
      {raft_role::candidate, raft_role::leader, true, 0, 1},
      {raft_role::leader, raft_role::follower, true, 1, NO_LEADER},
      {raft_role::leader, raft_role::pre_candidate, false, 0, NO_LEADER},
      {raft_role::leader, raft_role::candidate, false, 1, NO_LEADER},
      {raft_role::leader, raft_role::leader, true, 0, 1},
  };
  for (auto& t : tests) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1}, 10, 1)};
    helper::_role(r->raft()) = t.from;
    try {
      switch (t.to) {
        case raft_role::follower:
          helper::become_follower(r->raft(), t.exp_term, t.exp_lead, true);
          break;
        case raft_role::pre_candidate:
          helper::become_pre_candidate(r->raft());
          break;
        case raft_role::candidate:
          helper::become_candidate(r->raft());
          break;
        case raft_role::leader:
          co_await helper::become_leader(r->raft());
          break;
        default:
          ADD_FAILURE();
          break;
      }
      EXPECT_TRUE(t.exp_allow) << CASE_INDEX(t, tests);
      EXPECT_EQ(helper::_term(r->raft()), t.exp_term) << CASE_INDEX(t, tests);
      EXPECT_EQ(helper::_leader_id(r->raft()), t.exp_lead)
          << CASE_INDEX(t, tests);
    } catch (const rafter::util::invalid_raft_state& ex) {
      EXPECT_FALSE(t.exp_allow) << CASE_INDEX(t, tests);
    }
  }
  co_return;
}

RAFTER_TEST_F(raft_test, all_server_step_down) {
  struct {
    raft_role role;
    raft_role exp_role;
    uint64_t exp_term;
    uint64_t exp_index;
  } tests[] = {
      {raft_role::follower, raft_role::follower, 3, 0},
      {raft_role::pre_candidate, raft_role::follower, 3, 0},
      {raft_role::candidate, raft_role::follower, 3, 0},
      {raft_role::leader, raft_role::follower, 3, 1},
  };
  for (auto& t : tests) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 10, 1)};
    switch (t.role) {
      case raft_role::follower:
        helper::become_follower(r->raft(), 1, group_id::INVALID_NODE, true);
        break;
      case raft_role::pre_candidate:
        helper::become_pre_candidate(r->raft());
        break;
      case raft_role::candidate:
        helper::become_candidate(r->raft());
        break;
      case raft_role::leader:
        helper::become_candidate(r->raft());
        co_await helper::become_leader(r->raft());
        break;
      default:
        ADD_FAILURE();
        break;
    }
    for (auto msg : {message_type::request_vote, message_type::replicate}) {
      co_await r->handle({.type = msg, .from = 2, .term = 3, .lid = {3, 0}});
      EXPECT_EQ(helper::_role(r->raft()), t.exp_role)
          << CASE_INDEX(t, tests) << msg;
      EXPECT_EQ(helper::_term(r->raft()), t.exp_term)
          << CASE_INDEX(t, tests) << msg;
      EXPECT_EQ(helper::_log(r->raft()).last_index(), t.exp_index)
          << CASE_INDEX(t, tests) << msg;
      auto entries = co_await get_all_entries(r.get());
      EXPECT_EQ(entries.size(), t.exp_index) << CASE_INDEX(t, tests) << msg;
      if (msg == message_type::request_vote) {
        EXPECT_EQ(helper::_leader_id(r->raft()), group_id::INVALID_NODE)
            << CASE_INDEX(t, tests) << msg;
      } else {
        EXPECT_EQ(helper::_leader_id(r->raft()), 2)
            << CASE_INDEX(t, tests) << msg;
      }
    }
  }
  co_return;
}

RAFTER_TEST_F(raft_test, leader_step_down_when_quorum_active) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 5, 1)};
  helper::_check_quorum(r->raft()) = true;
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  for (uint64_t i = 0; i <= helper::_election_timeout(r->raft()); ++i) {
    co_await r->handle(
        {.type = message_type::heartbeat_resp,
         .from = 2,
         .term = helper::_term(r->raft())});
    co_await helper::tick(r->raft());
  }
  ASSERT_EQ(helper::_role(r->raft()), raft_role::leader);
}

RAFTER_TEST_F(raft_test, leader_step_down_when_quorum_lost) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2, 3}, 5, 1)};
  helper::_check_quorum(r->raft()) = true;
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  for (uint64_t i = 0; i <= helper::_election_timeout(r->raft()); ++i) {
    co_await helper::tick(r->raft());
  }
  ASSERT_EQ(helper::_role(r->raft()), raft_role::follower);
}

RAFTER_TEST_F(raft_test, leader_superseding_with_check_quorum) {
  auto* a = raft_sm::make(1, {1, 2, 3}, 10, 1);
  auto* b = raft_sm::make(2, {1, 2, 3}, 10, 1);
  auto* c = raft_sm::make(3, {1, 2, 3}, 10, 1);
  helper::_check_quorum(a->raft()) = true;
  helper::_check_quorum(b->raft()) = true;
  helper::_check_quorum(c->raft()) = true;
  auto nt = network({a, b, c});
  helper::_randomized_election_timeout(b->raft()) =
      helper::_election_timeout(b->raft()) + 1;
  for (uint64_t i = 0; i < helper::_election_timeout(b->raft()); ++i) {
    co_await helper::tick(b->raft());
  }
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  ASSERT_EQ(helper::_role(a->raft()), raft_role::leader);
  ASSERT_EQ(helper::_role(c->raft()), raft_role::follower);
  co_await nt.send({.type = message_type::election, .from = 3, .to = 3});
  // Peer b rejected c's vote since its election tick had not reached timeout
  ASSERT_EQ(helper::_role(c->raft()), raft_role::pre_candidate);
  // Letting b reach election timeout
  for (uint64_t i = 0; i < helper::_election_timeout(b->raft()); ++i) {
    co_await helper::tick(b->raft());
  }
  co_await nt.send({.type = message_type::election, .from = 3, .to = 3});
  ASSERT_EQ(helper::_role(c->raft()), raft_role::leader);
}

RAFTER_TEST_F(raft_test, leader_election_with_check_quorum) {
  auto* a = raft_sm::make(1, {1, 2, 3}, 10, 1);
  auto* b = raft_sm::make(2, {1, 2, 3}, 10, 1);
  auto* c = raft_sm::make(3, {1, 2, 3}, 10, 1);
  helper::_check_quorum(a->raft()) = true;
  helper::_check_quorum(b->raft()) = true;
  helper::_check_quorum(c->raft()) = true;
  auto nt = network({a, b, c});
  helper::_randomized_election_timeout(a->raft()) =
      helper::_election_timeout(a->raft()) + 1;
  helper::_randomized_election_timeout(b->raft()) =
      helper::_election_timeout(b->raft()) + 2;
  // Immediately after creation, votes are cast regardless of the election
  // timeout.
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  ASSERT_EQ(helper::_role(a->raft()), raft_role::leader);
  ASSERT_EQ(helper::_role(c->raft()), raft_role::follower);
  helper::_randomized_election_timeout(a->raft()) =
      helper::_election_timeout(a->raft()) + 1;
  helper::_randomized_election_timeout(b->raft()) =
      helper::_election_timeout(b->raft()) + 2;
  for (uint64_t i = 0; i < helper::_election_timeout(a->raft()); ++i) {
    co_await helper::tick(a->raft());
  }
  for (uint64_t i = 0; i < helper::_election_timeout(b->raft()); ++i) {
    co_await helper::tick(b->raft());
  }
  co_await nt.send({.type = message_type::election, .from = 3, .to = 3});
  // node 3 will receive (order is important):
  //  - 1's heartbeat + rejection and then 2's vote, in which case the node 3
  //    will re-enter follower state due to heartbeat
  //  - 2's vote and then 1's heartbeat + rejection, in which case the node 3
  //    will enter candidate state but cannot proceed as node 2 will not cast
  //    vote due to leader is available
  if (helper::_role(a->raft()) == raft_role::leader) {
    // case 1
    ASSERT_EQ(helper::_role(c->raft()), raft_role::follower);
  } else if (helper::_role(a->raft()) == raft_role::follower) {
    // case 2
    ASSERT_EQ(helper::_role(c->raft()), raft_role::candidate);
  } else {
    ADD_FAILURE();
  }
}

RAFTER_TEST_F(raft_test, SKIP_free_stuck_candidate_with_check_quorum) {
  // FIXME(jyc): TestFreeStuckCandidateWithCheckQuorum ensures that a candidate
  //  with a higher term can disrupt the leader even if the leader still
  //  "officially" holds the lease, The leader is expected to step down and
  //  adopt the candidate's term.
  //  However, since prevote is enabled by default, this test is invalid in
  //  prevote mode.
  co_return;
}

RAFTER_TEST_F(raft_test, non_promotable_voter_with_check_quorum) {
  auto* a = raft_sm::make(1, {1, 2}, 10, 1);
  auto* b = raft_sm::make(2, {1}, 10, 1);
  helper::_check_quorum(a->raft()) = true;
  helper::_check_quorum(b->raft()) = true;
  auto nt = network({a, b});
  helper::_randomized_election_timeout(b->raft()) =
      helper::_election_timeout(b->raft()) + 1;
  helper::_remotes(b->raft()).erase(2);
  ASSERT_TRUE(helper::is_self_removed(b->raft()));
  for (uint64_t i = 0; i < helper::_election_timeout(b->raft()); ++i) {
    co_await helper::tick(b->raft());
  }
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  ASSERT_EQ(helper::_role(a->raft()), raft_role::leader);
  ASSERT_EQ(helper::_role(b->raft()), raft_role::follower);
  ASSERT_EQ(helper::_leader_id(b->raft()), 1);
}

RAFTER_TEST_F(raft_test, readonly_option_safe) {
  auto* a = raft_sm::make(1, {1, 2, 3}, 10, 1);
  auto* b = raft_sm::make(2, {1, 2, 3}, 10, 1);
  auto* c = raft_sm::make(3, {1, 2, 3}, 10, 1);
  auto nt = network({a, b, c});
  helper::_randomized_election_timeout(b->raft()) =
      helper::_election_timeout(b->raft()) + 1;
  for (uint64_t i = 0; i < helper::_election_timeout(b->raft()); ++i) {
    co_await helper::tick(b->raft());
  }
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  ASSERT_EQ(helper::_role(a->raft()), raft_role::leader);
  struct {
    raft_sm* r;
    uint64_t proposals;
    uint64_t exp_read_index;
    hint exp_context;
  } tests[] = {
      {a, 10, 11, {10001, 10002}},
      {b, 10, 21, {10002, 10003}},
      {c, 10, 31, {10003, 10004}},
      {a, 10, 41, {10004, 10005}},
      {b, 10, 51, {10005, 10006}},
      {c, 10, 61, {10006, 10007}},
  };
  for (auto& t : tests) {
    for (uint64_t j = 0; j < t.proposals; ++j) {
      co_await nt.send(naive_proposal(1, 1));
    }
    co_await nt.send(
        {.type = message_type::read_index,
         .from = t.r->cfg().node_id,
         .to = t.r->cfg().node_id,
         .hint = t.exp_context});
    auto& reads = t.r->raft().ready_to_reads();
    EXPECT_FALSE(reads.empty()) << CASE_INDEX(t, tests);
    if (!reads.empty()) {
      EXPECT_EQ(reads[0].index, t.exp_read_index) << CASE_INDEX(t, tests);
      EXPECT_EQ(reads[0].context, t.exp_context) << CASE_INDEX(t, tests);
    }
    reads.clear();
  }
}

RAFTER_TEST_F(raft_test, leader_append_resp) {
  // initial progress: match = 0, next = 3
  struct {
    uint64_t index;
    bool reject;
    // progress
    uint64_t exp_match;
    uint64_t exp_next;
    // message
    uint64_t exp_msg_num;
    uint64_t exp_index;
    uint64_t exp_committed;
  } tests[] = {
      // stale resp, no replies
      {3, true, 0, 3, 0, 0, 0},
      // denied resp, leader does not commit, decrease next and send probe
      {2, true, 0, 2, 1, 1, 0},
      // accept resp, leader commits, broadcast with commit
      {2, false, 2, 4, 2, 2, 2},
      // ignore heartbeat replies
      {0, false, 0, 3, 0, 0, 0},
  };
  for (auto& t : tests) {
    auto logdb = std::make_unique<db>();
    co_await logdb->append(test::util::new_entries({{0, 1}, {1, 2}}));
    std::unique_ptr<raft_sm> r{
        raft_sm::make(1, {1, 2, 3}, 10, 1, std::move(logdb))};
    helper::become_candidate(r->raft());
    co_await helper::become_leader(r->raft());
    (void)r->read_messages();
    co_await r->handle(
        {.type = message_type::replicate_resp,
         .from = 2,
         .term = helper::_term(r->raft()),
         .lid = {0, t.index},
         .reject = t.reject,
         .hint = {t.index, 0}});
    auto& p = helper::_remotes(r->raft())[2];
    EXPECT_EQ(p.match, t.exp_match) << CASE_INDEX(t, tests);
    EXPECT_EQ(p.next, t.exp_next) << CASE_INDEX(t, tests);
    auto msgs = r->read_messages();
    EXPECT_EQ(msgs.size(), t.exp_msg_num) << CASE_INDEX(t, tests);
    for (auto& msg : msgs) {
      EXPECT_EQ(msg.lid.index, t.exp_index);
      EXPECT_EQ(msg.commit, t.exp_committed);
    }
  }
  co_return;
}

RAFTER_TEST_F(raft_test, broadcast_heartbeat) {
  uint64_t offset = 1000;
  auto snap = test::util::new_snapshot({1, offset});
  snap->membership = test_membership({1, 2, 3});
  auto logdb = std::make_unique<db>();
  logdb->lr().apply_snapshot(snap);
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {}, 10, 1, std::move(logdb))};
  helper::_term(r->raft()) = 1;
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  for (uint64_t i = 0; i < 10; ++i) {
    auto entries = test::util::new_entries({{0, i + 1}});
    co_await helper::append_entries(r->raft(), entries);
  }
  // slow follower
  helper::_remotes(r->raft())[2].match = 5;
  helper::_remotes(r->raft())[2].next = 6;
  // normal follower
  auto index = helper::_log(r->raft()).last_index();
  helper::_remotes(r->raft())[3].match = index;
  helper::_remotes(r->raft())[3].next = index + 1;
  co_await r->handle({.type = message_type::leader_heartbeat});
  auto msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 2);
  auto commit = helper::_log(r->raft()).committed();
  std::unordered_map<uint64_t, uint64_t> exp_commit{
      {2, std::min(commit, helper::_remotes(r->raft())[2].match)},
      {3, std::min(commit, helper::_remotes(r->raft())[3].match)},
  };
  for (auto& msg : msgs) {
    EXPECT_EQ(msg.type, message_type::heartbeat);
    EXPECT_EQ(msg.lid, log_id{});
    EXPECT_EQ(msg.commit, exp_commit[msg.to]);
    exp_commit.erase(msg.to);
    EXPECT_TRUE(msg.entries.empty());
  }
}

RAFTER_TEST_F(raft_test, receive_leader_heartbeat) {
  struct {
    raft_role role;
    uint64_t exp_msg_num;
  } tests[] = {
      {raft_role::leader, 2},
      {raft_role::pre_candidate, 0},
      {raft_role::candidate, 0},
      {raft_role::follower, 0},
  };
  for (auto& t : tests) {
    auto logdb = std::make_unique<db>();
    co_await logdb->append(test::util::new_entries({{0, 1}, {1, 2}}));
    std::unique_ptr<raft_sm> r{
        raft_sm::make(1, {1, 2, 3}, 10, 1, std::move(logdb))};
    helper::_term(r->raft()) = 1;
    helper::_role(r->raft()) = t.role;
    co_await r->handle(
        {.type = message_type::leader_heartbeat, .from = 1, .to = 1});
    auto msgs = r->read_messages();
    EXPECT_EQ(msgs.size(), t.exp_msg_num) << CASE_INDEX(t, tests);
    for (auto& msg : msgs) {
      EXPECT_EQ(msg.type, message_type::heartbeat) << CASE_INDEX(t, tests);
    }
  }
  co_return;
}

RAFTER_TEST_F(raft_test, leader_increase_next) {
  auto previous_entries = test::util::new_entries({{1, 1}, {1, 2}, {1, 3}});
  struct {
    enum core::remote::state state;
    uint64_t next;
    uint64_t exp_next;
  } tests[] = {
      // state replicate, optimistically increase next
      // previous entries + noop entry + propose + 1
      {core::remote::state::replicate, 2, previous_entries.size() + 1 + 1 + 1},
      // state probe, not optimistically increase next
      {core::remote::state::retry, 2, 2},
  };
  for (auto& t : tests) {
    auto logdb = std::make_unique<db>();
    co_await logdb->append(test::util::new_entries({{1, 1}, {1, 2}, {1, 3}}));
    std::unique_ptr<raft_sm> r{
        raft_sm::make(1, {1, 2}, 10, 1, std::move(logdb))};
    helper::become_candidate(r->raft());
    co_await helper::become_leader(r->raft());
    helper::_remotes(r->raft())[2].state = t.state;
    helper::_remotes(r->raft())[2].next = t.next;
    co_await r->handle(naive_proposal(1, 1, "some data"));
    EXPECT_EQ(helper::_remotes(r->raft())[2].next, t.exp_next);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, send_append_for_remote_retry) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1)};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  (void)r->read_messages();
  helper::_remotes(r->raft())[2].become_retry();
  for (uint64_t i = 0; i < 3; ++i) {
    if (i == 0) {
      // we expect that raft will only send out one append on the first
      // loop. After that, the follower is paused until a heartbeat response is
      // received.
      auto to_append = test::util::new_entries({{0, 0}});
      co_await helper::append_entries(r->raft(), to_append);
      co_await helper::send_replicate(
          r->raft(), 2, helper::_remotes(r->raft())[2]);
      auto msgs = r->read_messages();
      ASSERT_EQ(msgs.size(), 1);
      ASSERT_EQ(msgs[0].lid.index, 0);
    }
    ASSERT_EQ(helper::_remotes(r->raft())[2].state, core::remote::state::wait);
    for (uint64_t j = 0; j < 10; ++j) {
      auto to_append = test::util::new_entries({{0, 0}});
      co_await helper::append_entries(r->raft(), to_append);
      co_await helper::send_replicate(
          r->raft(), 2, helper::_remotes(r->raft())[2]);
      ASSERT_TRUE(r->read_messages().empty());
    }
    // do a heartbeat
    for (uint64_t j = 0; j < helper::_heartbeat_timeout(r->raft()); ++j) {
      co_await r->handle(
          {.type = message_type::leader_heartbeat, .from = 1, .to = 1});
    }
    ASSERT_EQ(helper::_remotes(r->raft())[2].state, core::remote::state::wait);
    auto msgs = r->read_messages();
    ASSERT_EQ(msgs.size(), 1);
    ASSERT_EQ(msgs[0].type, message_type::heartbeat);
  }
  // a heartbeat response will allow another message to be sent
  co_await r->handle(
      {.type = message_type::heartbeat_resp, .from = 2, .to = 1});
  auto msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0].lid.index, 0);
  ASSERT_EQ(helper::_remotes(r->raft())[2].state, core::remote::state::wait);
}

RAFTER_TEST_F(raft_test, send_append_for_remote_replicate) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1)};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  (void)r->read_messages();
  // optimistically send entries out
  helper::_remotes(r->raft())[2].become_replicate();
  for (uint64_t i = 0; i < 10; ++i) {
    auto to_append = test::util::new_entries({{0, 0}});
    co_await helper::send_replicate(
        r->raft(), 2, helper::_remotes(r->raft())[2]);
    ASSERT_EQ(r->read_messages().size(), 1);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, send_append_for_remote_snapshot) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1)};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  (void)r->read_messages();
  // paused, no append when entries are locally appended
  helper::_remotes(r->raft())[2].become_snapshot(10);
  for (uint64_t i = 0; i < 10; ++i) {
    auto to_append = test::util::new_entries({{0, 0}});
    co_await helper::send_replicate(
        r->raft(), 2, helper::_remotes(r->raft())[2]);
    ASSERT_TRUE(r->read_messages().empty());
  }
  co_return;
}

RAFTER_TEST_F(raft_test, recv_unreachable) {
  // unreachable puts remote peer into probe state.
  auto logdb = std::make_unique<db>();
  co_await logdb->append(test::util::new_entries({{1, 1}, {1, 2}, {1, 3}}));
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1, std::move(logdb))};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  (void)r->read_messages();
  auto& p = helper::_remotes(r->raft())[2];
  p.match = 3;
  p.become_replicate();
  p.try_update(5);
  co_await r->handle({.type = message_type::unreachable, .from = 2, .to = 1});
  ASSERT_EQ(p.state, core::remote::state::retry);
  ASSERT_EQ(p.match + 1, p.next);
}

RAFTER_TEST_F(raft_test, restore) {
  auto snap = test::util::new_snapshot({11, 11});
  snap->membership = test_membership({1, 2, 3});
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1)};
  ASSERT_TRUE(co_await helper::restore(r->raft(), snap));
  ASSERT_EQ(helper::_log(r->raft()).last_index(), snap->log_id.index);
  ASSERT_TRUE(co_await helper::_log(r->raft()).term_index_match(snap->log_id));
  std::unordered_set<uint64_t> exp_nodes{1, 2, 3};
  ASSERT_NE(get_all_nodes(r->raft()), exp_nodes) << "nodes restored too early";
  message m{.snapshot = snap};
  co_await helper::node_restore_remote(r->raft(), m);
  ASSERT_EQ(get_all_nodes(r->raft()), exp_nodes);
  ASSERT_FALSE(co_await helper::restore(r->raft(), snap));
}

RAFTER_TEST_F(raft_test, restore_ignore_snapshot) {
  auto logdb = std::make_unique<db>();
  co_await logdb->append(test::util::new_entries({{1, 1}, {1, 2}, {1, 3}}));
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1, std::move(logdb))};
  uint64_t commit = 1;
  helper::_log(r->raft()).commit(commit);
  auto snap = test::util::new_snapshot({1, commit});
  snap->membership = test_membership({1, 2});
  ASSERT_FALSE(co_await helper::restore(r->raft(), snap));
  ASSERT_EQ(helper::_log(r->raft()).committed(), commit);
  // snap.index, snap.term match the log. restore is not required.
  // but it will fast forward the committed value.
  // ignore snapshot and fast forward commit
  snap = test::util::new_snapshot({1, commit + 1});
  snap->membership = test_membership({1, 2});
  ASSERT_FALSE(co_await helper::restore(r->raft(), snap));
  ASSERT_EQ(helper::_log(r->raft()).committed(), commit + 1);
}

RAFTER_TEST_F(raft_test, provide_snapshot) {
  // restore the state machine from a snapshot so it has a compacted log and a
  // snapshot
  auto snap = test::util::new_snapshot({11, 11});
  snap->membership = test_membership({1, 2});
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1}, 10, 1)};
  co_await helper::restore(r->raft(), snap);
  message m{.snapshot = snap};
  co_await helper::node_restore_remote(r->raft(), m);
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  // force set the next of node 2, so that node 2 needs a snapshot
  helper::_remotes(r->raft())[2].next = helper::_log(r->raft()).first_index();
  co_await r->handle(
      {.type = message_type::replicate_resp,
       .from = 2,
       .to = 1,
       .lid = {0, helper::_remotes(r->raft())[2].next - 1},
       .reject = true});
  auto msgs = r->read_messages();
  ASSERT_EQ(msgs.size(), 1);
  ASSERT_EQ(msgs[0].type, message_type::install_snapshot);
}

RAFTER_TEST_F(raft_test, ignore_providing_snapshot) {
  // restore the state machine from a snapshot so it has a compacted log and a
  // snapshot
  auto snap = test::util::new_snapshot({11, 11});
  snap->membership = test_membership({1, 2});
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1}, 10, 1)};
  co_await helper::restore(r->raft(), snap);
  message m{.snapshot = snap};
  co_await helper::node_restore_remote(r->raft(), m);
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  // force set the next of node 2, so that node 2 needs a snapshot
  // change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
  helper::_remotes(r->raft())[2].next =
      helper::_log(r->raft()).first_index() - 1;
  helper::_remotes(r->raft())[2].active = false;
  co_await r->handle(naive_proposal(1, 1, "some data"));
  auto msgs = r->read_messages();
  ASSERT_TRUE(msgs.empty());
}

RAFTER_TEST_F(raft_test, restore_from_snapshot) {
  auto snap = test::util::new_snapshot({11, 11});
  snap->membership = test_membership({1, 2});
  std::unique_ptr<raft_sm> r{raft_sm::make(2, {1, 2}, 10, 1)};
  co_await r->handle(
      {.type = message_type::install_snapshot,
       .from = 1,
       .term = 2,
       .snapshot = snap});
  ASSERT_EQ(helper::_leader_id(r->raft()), 1);
}

RAFTER_TEST_F(raft_test, slow_node_restore) {
  auto nt = network({{null(), null(), null()}});
  co_await nt.send({.type = message_type::election, .from = 1, .to = 1});
  nt.isolate(3);
  for (uint64_t i = 0; i <= 100; ++i) {
    co_await nt.send(naive_proposal(1, 1, "some data"));
  }
  auto& lead = raft_cast(nt.sms[1].get());
  co_await next_entries(nt.sms[1].get(), nt.dbs[1].get());
  auto m = test_membership(nt.sms[1].get());
  auto s = co_await test_snapshot(
      nt.dbs[1].get(), helper::_log(lead).processed(), m);
  nt.dbs[1]->lr().create_snapshot(s);
  co_await nt.dbs[1]->lr().apply_compaction(helper::_log(lead).processed());
  auto& follower = raft_cast(nt.sms[3].get());
  nt.recover();
  // send heartbeats so that the leader can learn everyone is active.
  // node 3 will only be considered as active when node 1 receives a reply from
  // it.
  while (true) {
    co_await nt.send(
        {.type = message_type::leader_heartbeat, .from = 1, .to = 1});
    if (helper::_remotes(lead)[3].active) {
      break;
    }
  }
  // trigger a snapshot
  co_await nt.send(naive_proposal(1, 1, "some data"));
  // trigger a commit
  co_await nt.send(naive_proposal(1, 1, "some data"));
  ASSERT_EQ(helper::_log(lead).committed(), helper::_log(follower).committed());
}

RAFTER_TEST_F(raft_test, step_config) {
  // when raft handle proposal with config_change type, it appends the entry to
  // log and sets pending_config_change to be true.
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1)};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  auto index = helper::_log(r->raft()).last_index();
  auto cc = naive_proposal(1, 1);
  cc.entries[0].type = entry_type::config_change;
  co_await r->handle(std::move(cc));
  ASSERT_EQ(helper::_log(r->raft()).last_index(), index + 1);
  ASSERT_TRUE(helper::_pending_config_change(r->raft()));
}

RAFTER_TEST_F(raft_test, step_ignore_config) {
  // if raft step the second proposal with config_change type when the first one
  // is uncommitted, the node will set the proposal to noop and keep its
  // original state.
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1)};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  auto index = helper::_log(r->raft()).last_index();
  auto cc = naive_proposal(1, 1);
  cc.entries[0].type = entry_type::config_change;
  co_await r->handle(std::move(cc));
  ASSERT_EQ(helper::_log(r->raft()).last_index(), index + 1);
  ASSERT_TRUE(helper::_pending_config_change(r->raft()));
  // the second config change
  cc = naive_proposal(1, 1);
  cc.entries[0].type = entry_type::config_change;
  co_await r->handle(std::move(cc));
  ASSERT_EQ(helper::_log(r->raft()).last_index(), index + 2);
  ASSERT_EQ(r->raft().dropped_entries().size(), 1);
  ASSERT_EQ(r->raft().dropped_entries()[0].type, entry_type::config_change);
  log_entry_vector queried;
  co_await helper::_log(r->raft()).query(index + 2, queried, UINT64_MAX);
  log_entry exp_entry{{1, 3}};
  exp_entry.type = entry_type::application;
  ASSERT_EQ(queried[0], exp_entry);
  ASSERT_TRUE(helper::_pending_config_change(r->raft()));
}

RAFTER_TEST_F(raft_test, recover_pending_config) {
  // new leader recovers its pending_config_change based on uncommitted entries.
  struct {
    entry_type type;
    bool exp_pending;
  } tests[] = {
      {entry_type::application, false},
      {entry_type::config_change, true},
  };
  for (auto& t : tests) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1)};
    auto entry = test::util::new_entries({{0, 0}});
    entry[0].type = t.type;
    co_await helper::append_entries(r->raft(), entry);
    helper::become_candidate(r->raft());
    co_await helper::become_leader(r->raft());
    EXPECT_EQ(helper::_pending_config_change(r->raft()), t.exp_pending);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, recover_double_pending_config) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1)};
  auto entry = test::util::new_entries({{0, 0}});
  entry[0].type = entry_type::config_change;
  co_await helper::append_entries(r->raft(), entry);
  co_await helper::append_entries(r->raft(), entry);
  helper::become_candidate(r->raft());
  ASSERT_THROW(
      co_await helper::become_leader(r->raft()),
      rafter::util::invalid_raft_state);
}

RAFTER_TEST_F(raft_test, add_node) {
  // add_node could update pending_config_change and nodes correctly.
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1)};
  helper::_pending_config_change(r->raft()) = true;
  helper::add_node(r->raft(), 2);
  ASSERT_FALSE(helper::_pending_config_change(r->raft()));
  std::unordered_set<uint64_t> exp_nodes{1, 2};
  ASSERT_EQ(get_all_nodes(r->raft()), exp_nodes);
}

RAFTER_TEST_F(raft_test, remove_node) {
  // remove_node could update pending_config_change, nodes and removed list
  // correctly.
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 10, 1)};
  helper::_pending_config_change(r->raft()) = true;
  co_await helper::remove_node(r->raft(), 2);
  ASSERT_FALSE(helper::_pending_config_change(r->raft()));
  std::unordered_set<uint64_t> exp_nodes{1};
  ASSERT_EQ(get_all_nodes(r->raft()), exp_nodes);
  co_await helper::remove_node(r->raft(), 1);
  exp_nodes.erase(1);
  ASSERT_EQ(get_all_nodes(r->raft()), exp_nodes);
}

RAFTER_TEST_F(raft_test, promotable) {
  struct {
    std::vector<uint64_t> peers;
    bool exp;
  } tests[] = {
      {{1}, true},
      {{1, 2, 3}, true},
      {{}, false},
      {{2, 3}, false},
  };
  for (auto& t : tests) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, t.peers, 10, 1)};
    EXPECT_EQ(!helper::is_self_removed(r->raft()), t.exp);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, nodes) {
  struct {
    std::vector<uint64_t> peers;
    std::unordered_set<uint64_t> exp_peers;
  } tests[] = {
      {{1, 2, 3}, {1, 2, 3}},
      {{3, 2, 1}, {1, 2, 3}},
  };
  for (auto& t : tests) {
    std::unique_ptr<raft_sm> r{raft_sm::make(1, t.peers, 10, 1)};
    EXPECT_EQ(get_all_nodes(r->raft()), t.exp_peers);
  }
  co_return;
}

RAFTER_TEST_F(raft_test, compaign_while_leader) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1}, 5, 1)};
  ASSERT_EQ(helper::_role(r->raft()), raft_role::follower);
  co_await r->handle({.type = message_type::election, .from = 1, .to = 1});
  ASSERT_EQ(helper::_role(r->raft()), raft_role::leader);
  auto term = helper::_term(r->raft());
  co_await r->handle({.type = message_type::election, .from = 1, .to = 1});
  ASSERT_EQ(helper::_role(r->raft()), raft_role::leader);
  ASSERT_EQ(helper::_term(r->raft()), term);
}

RAFTER_TEST_F(raft_test, commit_after_remove_node) {
  // pending commands can become committed when a config change reduces the
  // quorum requirements.
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 5, 1)};
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  config_change cc{.type = config_change_type::remove_node, .node = 2};
  auto m = naive_proposal(1, 1);
  m.entries[0].type = entry_type::config_change;
  m.entries[0].payload = write_to_tmpbuf(cc);
  co_await r->handle(std::move(m));
  // Stabilize the log and make sure nothing is committed yet.
  auto ents = co_await next_entries(r.get(), r->logdb0());
  ASSERT_TRUE(ents.empty());
  auto cc_index = helper::_log(r->raft()).last_index();
  // While the config change is pending, make another proposal.
  co_await r->handle(naive_proposal(1, 1, "hello"));
  // Node 2 acknowledges the config change, committing it.
  co_await r->handle(
      {.type = message_type::replicate_resp,
       .from = 2,
       .to = 1,
       .lid = {0, cc_index}});
  ents = co_await next_entries(r.get(), r->logdb0());
  ASSERT_EQ(ents.size(), 2);
  ASSERT_EQ(ents[0].type, entry_type::application);
  ASSERT_TRUE(ents[0].payload.empty());
  ASSERT_EQ(ents[1].type, entry_type::config_change);
  // Apply the config change. This reduces quorum requirements so the pending
  // command can now be committed.
  co_await helper::remove_node(r->raft(), 2);
  ents = co_await next_entries(r.get(), r->logdb0());
  ASSERT_EQ(ents.size(), 1);
  ASSERT_EQ(ents[0].type, entry_type::application);
  ASSERT_EQ(ents[0].payload, temporary_buffer<char>::copy_of("hello"));
}

RAFTER_TEST_F(raft_test, sending_snapshot_set_pending_snapshot) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 5, 1)};
  auto snap = test_snapshot({1, 2});
  co_await helper::restore(r->raft(), snap);
  message m{.snapshot = snap};
  co_await helper::node_restore_remote(r->raft(), m);
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  // force set the next of node 1 so that it needs a snapshot
  auto index = helper::_log(r->raft()).first_index();
  helper::_remotes(r->raft())[2].next = index;
  co_await r->handle(
      {.type = message_type::replicate_resp,
       .from = 2,
       .to = 1,
       .lid = {0, index - 1},
       .reject = true});
  ASSERT_EQ(helper::_remotes(r->raft())[2].snapshot_index, 11);
}

RAFTER_TEST_F(raft_test, pending_snapshot_pause_replication) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 5, 1)};
  auto snap = test_snapshot({1, 2});
  co_await helper::restore(r->raft(), snap);
  message m{.snapshot = snap};
  co_await helper::node_restore_remote(r->raft(), m);
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  helper::_remotes(r->raft())[2].become_snapshot(11);
  co_await r->handle(naive_proposal(1, 1));
  ASSERT_TRUE(r->read_messages().empty());
}

RAFTER_TEST_F(raft_test, snapshot_failure) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 5, 1)};
  auto snap = test_snapshot({1, 2});
  co_await helper::restore(r->raft(), snap);
  message m{.snapshot = snap};
  co_await helper::node_restore_remote(r->raft(), m);
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  helper::_remotes(r->raft())[2].next = 1;
  helper::_remotes(r->raft())[2].become_snapshot(11);
  co_await r->handle(
      {.type = message_type::snapshot_status,
       .from = 2,
       .to = 1,
       .reject = true});
  ASSERT_EQ(helper::_remotes(r->raft())[2].snapshot_index, 0);
  ASSERT_EQ(helper::_remotes(r->raft())[2].next, 1);
  ASSERT_EQ(helper::_remotes(r->raft())[2].state, core::remote::state::wait);
}

RAFTER_TEST_F(raft_test, snapshot_succeed) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 5, 1)};
  auto snap = test_snapshot({1, 2});
  co_await helper::restore(r->raft(), snap);
  message m{.snapshot = snap};
  co_await helper::node_restore_remote(r->raft(), m);
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  helper::_remotes(r->raft())[2].next = 1;
  helper::_remotes(r->raft())[2].become_snapshot(11);
  co_await r->handle(
      {.type = message_type::snapshot_status,
       .from = 2,
       .to = 1,
       .reject = false});
  ASSERT_EQ(helper::_remotes(r->raft())[2].snapshot_index, 0);
  ASSERT_EQ(helper::_remotes(r->raft())[2].next, 12);
  ASSERT_EQ(helper::_remotes(r->raft())[2].state, core::remote::state::wait);
}

RAFTER_TEST_F(raft_test, snapshot_abort) {
  std::unique_ptr<raft_sm> r{raft_sm::make(1, {1, 2}, 5, 1)};
  auto snap = test_snapshot({1, 2});
  co_await helper::restore(r->raft(), snap);
  message m{.snapshot = snap};
  co_await helper::node_restore_remote(r->raft(), m);
  helper::become_candidate(r->raft());
  co_await helper::become_leader(r->raft());
  helper::_remotes(r->raft())[2].next = 1;
  helper::_remotes(r->raft())[2].become_snapshot(11);
  // A successful replicate_resp that has a higher/equal index than the pending
  // snapshot should abort the pending snapshot.
  co_await r->handle(
      {.type = message_type::replicate_resp,
       .from = 2,
       .to = 1,
       .lid = {0, 11}});
  ASSERT_EQ(helper::_remotes(r->raft())[2].snapshot_index, 0);
  ASSERT_EQ(helper::_remotes(r->raft())[2].next, 12);
}

}  // namespace
