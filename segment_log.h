//
// Created by jason on 2021/3/12.
//

#ifndef SEASTAR_PLAYGROUND__SEGMENT_LOG_H_
#define SEASTAR_PLAYGROUND__SEGMENT_LOG_H_

#include <seastar/core/file.hh>

class segment {
 public:
  struct header {
    uint64_t term;        // latest
    uint64_t vote;        // latest
    uint64_t commit;      // latest
    uint64_t start_index; // inclusive
    uint64_t end_index;   // inclusive
    uint8_t checksum_type

  };
  struct log_meta {
    uint64_t term;
    uint64_t index;
    uint8_t type;
    uint8_t checksum_type;
    uint32_t data_size;
    uint32_t data_checksum;
    uint32_t meta_checksum;
  };
 private:
  // {start_index:020d}.log
  const std::string name_;
  const uint64_t cluster_id_;
  const uint64_t node_id_;
  seastar::shared_ptr<seastar::file> file_;
};

#endif //SEASTAR_PLAYGROUND__SEGMENT_LOG_H_
