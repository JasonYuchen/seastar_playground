//
// Created by jason on 2021/10/13.
//

#include "segment_manager.h"

namespace rafter::storage {

using namespace protocol;
using namespace seastar;
using namespace std;

segment_manager::segment_manager(group_id id, filesystem::path data_dir)
    : _group_id(id), _path(std::move(data_dir)) {
  // TODO: setup WAL module
  //  1. validate data_dir
  //  2. parse existing segments
  //  3. create active segment
}

future<bool> segment_manager::append(const protocol::update& update) {
  // TODO:
  //  1. estimate written size to allocate fragmented_temporary_buffer
  //  2. do serialize
}

}  // namespace rafter::storage
