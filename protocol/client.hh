//
// Created by jason on 2021/10/8.
//

#pragma once

#include <stdint.h>

#include "util/types.hh"

namespace rafter::protocol {

class session {
 public:
  inline static constexpr uint64_t not_managed_id = 0;
  inline static constexpr uint64_t noop_series_id = 0;
  inline static constexpr uint64_t initial_series_id = 1;
  inline static constexpr uint64_t registration_series_id = UINT64_MAX - 1;
  inline static constexpr uint64_t unregistration_series_id = UINT64_MAX;

  uint64_t cluster_id = 0;
  uint64_t client_id = 0;
  uint64_t series_id = 0;
  uint64_t responded_to = 0;

  session(uint64_t cluster_id, uint64_t client_id, bool noop = false);
  DEFAULT_COPY_MOVE_AND_ASSIGN(session);

  bool is_noop() const noexcept;
  void prepare_for_register();
  void prepare_for_unregister();
  void prepare_for_propose();
  void proposal_completed();
  bool is_valid_for_proposal(uint64_t cluster_id) const noexcept;
  bool is_valid_for_session_operation(uint64_t cluster_id) const noexcept;

 private:
  void assert_regular() const;
};

}  // namespace rafter::protocol
