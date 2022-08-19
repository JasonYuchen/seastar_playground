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

}  // namespace
