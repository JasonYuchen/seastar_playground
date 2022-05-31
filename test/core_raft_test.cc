//
// Created by jason on 2022/5/28.
//

#include <fmt/format.h>

#include <seastar/core/coroutine.hh>
#include <typeinfo>

#include "core/raft.hh"
#include "core/raft_log.hh"
#include "test/base.hh"
#include "test/test_helper.hh"
#include "test/test_logdb.hh"
#include "util/error.hh"
#include "util/seastarx.hh"

namespace {

using namespace rafter;
using namespace rafter::protocol;

using rafter::test::helper;
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

class raft_sm final : public sm {
 public:
  raft_sm(std::unique_ptr<raft_config> cfg, core::log_reader& lr)
    : _cfg(std::move(cfg)), _r(std::make_unique<core::raft>(*_cfg, lr)) {}
  ~raft_sm() override = default;
  future<> handle(message m) override { return _r->handle(m); }
  message_vector read_messages() override {
    auto msgs = _r->messages();
    _r->messages().clear();
    return msgs;
  }
  raft_config& cfg() { return *_cfg; }
  core::raft& raft() { return *_r; }

 private:
  std::unique_ptr<raft_config> _cfg;
  std::unique_ptr<core::raft> _r;
};

std::unique_ptr<raft_sm> new_test_raft(
    uint64_t id,
    const std::vector<uint64_t>& peers,
    uint64_t election,
    uint64_t heartbeat,
    core::log_reader& lr,
    uint64_t limit_size = UINT64_MAX) {
  auto config = new_test_config(id, election, heartbeat, limit_size);
  auto r = std::make_unique<raft_sm>(std::move(config), lr);
  for (uint64_t peer : peers) {
    helper::_remotes(r->raft()).emplace(peer, core::remote{.next = 1});
  }
  return r;
}

class black_hole final : public sm {
 public:
  ~black_hole() override = default;
  future<> handle(message m) override { return make_ready_future<>(); }
  protocol::message_vector read_messages() override { return {}; }
};

class db {
 public:
  db() : _db(new test::test_logdb()), _reader({1, 1}, *_db) {}
  core::log_reader& lr() { return _reader; }
  void append(log_entry_vector entries);

 private:
  std::unique_ptr<storage::logdb> _db;
  core::log_reader _reader;
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
        dbs[node_id] = std::make_unique<db>();
        auto sm =
            std::make_unique<raft_sm>(std::move(test_cfg), dbs[node_id]->lr());
        for (size_t j = 0; j < peers.size(); ++j) {
          // set test peers
          helper::_remotes(sm->raft()).emplace(j + 1, core::remote{.next = 1});
        }
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
        p.cfg().node_id = node_id;
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
        sms[node_id] = std::unique_ptr<sm>(peers[i]);
      } else if (auto& bh = *peers[i]; typeid(bh) == typeid(black_hole)) {
        sms[node_id] = std::unique_ptr<sm>(peers[i]);
      } else {
        throw rafter::util::panic("unexpected sm");
      }
    }
  }

  future<> send(message_vector msgs) {
    while (!msgs.empty()) {
      auto& m = msgs.front();
      if (!sms.contains(m.to)) {
        continue;
      }
      auto& p = sms[m.to];
      co_await p->handle(std::move(m));
      msgs.erase(msgs.begin());
      auto next = filter(p->read_messages());
      msgs.insert(
          msgs.end(),
          std::make_move_iterator(next.begin()),
          std::make_move_iterator(next.end()));
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

  message_vector filter(message_vector msgs) {
    static std::mt19937_64 rnd(
        std::chrono::system_clock::now().time_since_epoch().count());
    static std::uniform_real_distribution<double> dist(0.0, 1.0);
    message_vector ret;
    auto should_filter = [this, &ret](message& m) {
      if (ignore_msg.contains(m.type)) {
        return;
      }
      if (m.type == message_type::election) {
        throw rafter::util::panic("unexpected election");
      }
      if (dist(rnd) < drop_msg[{m.from, m.to}]) {
        return;
      }
      ret.emplace_back(std::move(m));
    };
    std::for_each(msgs.begin(), msgs.end(), should_filter);
    return ret;
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
      core::raft& r, raft_role role, uint64_t leader) {
    EXPECT_EQ(helper::_role(r), role);
    EXPECT_EQ(helper::_leader_id(r), leader);
    EXPECT_EQ(helper::_leader_transfer_target(r), group_id::INVALID_NODE);
    return !HasFailure();
  }
};

RAFTER_TEST_F(raft_test, leader_transfer_to_up_to_date_node) {
  auto nt = network({null(), null(), null()});
  co_await nt.send(
      {message{.type = message_type::election, .from = 1, .to = 1}});
  auto& lead = raft_cast(nt.sms[1].get());
  ASSERT_EQ(helper::_leader_id(lead), 1) << "unexpected leader after election";
  // transfer leadership to 2
  co_await nt.send({message{
      .type = message_type::leader_transfer,
      .from = 2,
      .to = 1,
      .hint = {.low = 2}}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::follower, 2));
  co_await nt.send({message{
      .type = message_type::propose,
      .from = 1,
      .to = 1,
      .entries = {make_lw_shared<log_entry>()}}});
  // transfer leadership back to 1 after replication
  co_await nt.send({message{
      .type = message_type::leader_transfer,
      .from = 1,
      .to = 2,
      .hint = {.low = 1}}});
  ASSERT_TRUE(check_leader_transfer_state(lead, raft_role::leader, 1));
}

}  // namespace
