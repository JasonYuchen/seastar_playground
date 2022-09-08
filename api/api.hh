//
// Created by jason on 2022/9/6.
//

#pragma once

#include <iostream>
#include <seastar/core/sstring.hh>
#include <seastar/http/json_path.hh>
#include <seastar/json/json_elements.hh>

// FIXME(jyc): This file should be generated according to api.json.

namespace seastar {
namespace httpd {
namespace api_json {

// TODO(jyc): model

static const sstring name = "api";

extern const path_description listClusters;
extern const path_description startCluster;
extern const path_description singleGet;
extern const path_description singlePut;
extern const path_description singleDelete;

}  // namespace api_json
}  // namespace httpd
}  // namespace seastar
