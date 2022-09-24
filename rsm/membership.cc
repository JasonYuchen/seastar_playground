//
// Created by jason on 2022/5/9.
//

#include "membership.hh"

#include "rsm/logger.hh"
#include "util/error.hh"

namespace rafter::rsm {

using enum protocol::config_change_type;
using protocol::config_change;

bool membership::handle(const config_change& cc, uint64_t index) {
  if (static_cast<uint8_t>(cc.type) >= static_cast<uint8_t>(num_of_type)) {
    util::panic::panic_with_backtrace("unknown config change type");
  }
  auto up_to_date = is_up_to_date(cc);
  auto adding_removed_node = is_adding_removed_node(cc);
  auto adding_existing_node = is_adding_existing_node(cc);
  auto adding_node_as_observer = is_adding_node_as_observer(cc);
  auto adding_node_as_witness = is_adding_node_as_witness(cc);
  auto adding_witness_as_node = is_adding_witness_as_node(cc);
  auto adding_witness_as_observer = is_adding_witness_as_observer(cc);
  auto adding_observer_as_witness = is_adding_observer_as_witness(cc);
  auto deleting_last_node = is_deleting_last_node(cc);
  auto invalid_observer_promotion = is_invalid_observer_promotion(cc);
  bool accepted = up_to_date && !adding_removed_node && !adding_existing_node &&
                  !adding_node_as_observer && !adding_node_as_witness &&
                  !adding_witness_as_node && !adding_witness_as_observer &&
                  !adding_observer_as_witness && !deleting_last_node &&
                  !invalid_observer_promotion;
  auto description = fmt::format(
      "type:{}, ccid:{}, index:{}, node_id:{}, address:{}",
      cc.type,
      cc.config_change_id,
      index,
      cc.node,
      cc.address);
  if (accepted) {
    apply(cc, index);
    l.info("{} applied: {}", _gid, description);
  } else {
    auto warn_if = [&](bool cond, std::string_view desc) {
      if (cond) {
        l.warn("{} rejected {}: {}", _gid, desc, description);
      }
    };
    warn_if(!up_to_date, "out-of-date");
    warn_if(adding_removed_node, "add-removed-node");
    warn_if(adding_existing_node, "add-existing-node");
    warn_if(adding_node_as_observer, "add-node-as-observer");
    warn_if(adding_node_as_witness, "add-node-as-witness");
    warn_if(adding_witness_as_node, "add-witness-as-node");
    warn_if(adding_witness_as_observer, "add-witness-as-observer");
    warn_if(adding_observer_as_witness, "add-observer-as-witness");
    warn_if(deleting_last_node, "delete-last-node");
    warn_if(invalid_observer_promotion, "invalid-observer-promotion");
  }
  return accepted;
}

void membership::apply(const config_change& cc, uint64_t index) {
  _membership.config_change_id = index;
  switch (cc.type) {
    case add_node: {
      _membership.observers.erase(cc.node);
      if (_membership.witnesses.contains(cc.node)) [[unlikely]] {
        util::panic::panic_with_backtrace("add witness as node");
      }
      _membership.addresses[cc.node] = cc.address;
      break;
    }
    case add_observer: {
      if (_membership.addresses.contains(cc.node)) [[unlikely]] {
        util::panic::panic_with_backtrace("add node as observer");
      }
      _membership.observers[cc.node] = cc.address;
      break;
    }
    case add_witness: {
      if (_membership.addresses.contains(cc.node)) [[unlikely]] {
        util::panic::panic_with_backtrace("add node as witness");
      }
      if (_membership.observers.contains(cc.node)) [[unlikely]] {
        util::panic::panic_with_backtrace("add observer as witness");
      }
      _membership.witnesses[cc.node] = cc.address;
      break;
    }
    case remove_node: {
      _membership.addresses.erase(cc.node);
      _membership.observers.erase(cc.node);
      _membership.witnesses.erase(cc.node);
      _membership.removed[cc.node] = true;
      break;
    }
    default:
      util::panic::panic_with_backtrace("unknown config change type");
  }
}

bool membership::is_up_to_date(const config_change& cc) const {
  if (!_ordered || cc.initialize) {
    return true;
  }
  if (_membership.config_change_id == cc.config_change_id) {
    return true;
  }
  return false;
}

bool membership::is_adding_removed_node(const config_change& cc) const {
  switch (cc.type) {
    case add_node:
    case add_observer:
    case add_witness:
      return _membership.removed.contains(cc.node);
    default:
      return false;
  }
}

bool membership::is_promoting_observer(const config_change& cc) const {
  if (cc.type == add_node && _membership.observers.contains(cc.node)) {
    return _membership.observers.at(cc.node) == cc.address;
  }
  return false;
}

bool membership::is_invalid_observer_promotion(const config_change& cc) const {
  if (cc.type == add_node && _membership.observers.contains(cc.node)) {
    return _membership.observers.at(cc.node) != cc.address;
  }
  return false;
}

bool membership::is_adding_existing_node(const config_change& cc) const {
  if (cc.type == add_node && _membership.addresses.contains(cc.node)) {
    return true;
  }
  if (cc.type == add_observer && _membership.observers.contains(cc.node)) {
    return true;
  }
  if (cc.type == add_witness && _membership.witnesses.contains(cc.node)) {
    return true;
  }
  if (is_promoting_observer(cc)) {
    return false;
  }
  if (cc.type == add_node || cc.type == add_observer ||
      cc.type == add_witness) {
    auto pred = [&](const protocol::member_map& m) {
      return std::any_of(m.begin(), m.end(), [&](const auto& peer) {
        return peer.second == cc.address;
      });
    };
    return pred(_membership.addresses) || pred(_membership.observers) ||
           pred(_membership.witnesses);
  }
  return false;
}

bool membership::is_adding_node_as_observer(const config_change& cc) const {
  if (cc.type == add_observer) {
    return _membership.addresses.contains(cc.node);
  }
  return false;
}

bool membership::is_adding_node_as_witness(const config_change& cc) const {
  if (cc.type == add_witness) {
    return _membership.addresses.contains(cc.node);
  }
  return false;
}

bool membership::is_adding_witness_as_observer(const config_change& cc) const {
  if (cc.type == add_observer) {
    return _membership.witnesses.contains(cc.node);
  }
  return false;
}

bool membership::is_adding_witness_as_node(const config_change& cc) const {
  if (cc.type == add_node) {
    return _membership.witnesses.contains(cc.node);
  }
  return false;
}

bool membership::is_adding_observer_as_witness(const config_change& cc) const {
  if (cc.type == add_witness) {
    return _membership.observers.contains(cc.node);
  }
  return false;
}

bool membership::is_deleting_last_node(const config_change& cc) const {
  if (cc.type == remove_node && _membership.addresses.size() == 1) {
    return _membership.addresses.contains(cc.node);
  }
  return false;
}

}  // namespace rafter::rsm
