//
// Created by jason on 2022/5/28.
//

#include "core/raft.hh"

#include <fmt/format.h>

#include <seastar/core/coroutine.hh>
#include <typeinfo>

#include "core/raft_log.hh"
#include "helper.hh"
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

  future<> handle(message m) override { return _r->handle(m); }
  message_vector read_messages() override {
    return std::exchange(_r->messages(), {});
  }
  raft_config& cfg() { return *_cfg; }
  core::raft& raft() { return *_r; }

  std::unique_ptr<db> logdb() { return std::move(_db); }

 private:
  std::unique_ptr<raft_config> _cfg;
  std::unique_ptr<db> _db;
  std::unique_ptr<core::raft> _r;
};

class black_hole final : public sm {
 public:
  ~black_hole() override = default;
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
      if (!peers[i]) {
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
      log_entry_vector entries;
      auto fi = helper::_log(rs->raft()).first_index();
      co_await helper::_log(rs->raft()).query(fi, entries, UINT64_MAX);
      co_return entries;
    }
    co_await coroutine::return_exception(rafter::util::panic("not a raft_sm"));
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

    s->membership = members;
    co_return std::move(s);
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
  co_await nt.send(
      {.type = message_type::propose, .from = 1, .to = 1, .entries = {{}}});
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
  co_await nt.send(
      {.type = message_type::propose, .from = 1, .to = 1, .entries = {{}}});
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
  co_await nt.send(
      {.type = message_type::propose, .from = 1, .to = 1, .entries = {{}}});
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
  co_await nt.send(
      {.type = message_type::propose, .from = 1, .to = 1, .entries = {{}}});
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
  co_await nt.send(
      {.type = message_type::propose, .from = 1, .to = 1, .entries = {{}}});
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
  co_await nt.send(
      {.type = message_type::propose, .from = 1, .to = 1, .entries = {{}}});
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
  co_await nt.send(
      {.type = message_type::propose, .from = 1, .to = 1, .entries = {{}}});
  uint64_t matched = helper::_remotes(lead)[2].match;
  co_await nt.send(
      {.type = message_type::propose, .from = 1, .to = 1, .entries = {{}}});
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
  co_await r->raft().handle(
      {.type = message_type::propose, .from = 1, .to = 1, .entries = {{}}});
  co_await r->raft().handle(
      {.type = message_type::propose, .from = 1, .to = 1, .entries = {{}}});
  co_await r->raft().handle(
      {.type = message_type::propose, .from = 1, .to = 1, .entries = {{}}});
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

}  // namespace
