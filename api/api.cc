//
// Created by jason on 2022/9/6.
//

#include "api.hh"

namespace seastar {
namespace httpd {
namespace api_json {

const path_description listClusters("/clusters", GET, "listClusters", {}, {});

const path_description startCluster("/clusters", POST, "startCluster", {}, {});

const path_description singleGet(
    "/cluster",
    GET,
    "singleGet",
    {{"clusterId", path_description::url_component_type::PARAM}},
    {"key"});

const path_description singlePut(
    "/cluster",
    PUT,
    "singlePut",
    {{"clusterId", path_description::url_component_type::PARAM}},
    {"key", "value"});

const path_description singleDelete(
    "/cluster",
    DELETE,
    "singleDelete",
    {{"clusterId", path_description::url_component_type::PARAM}},
    {"key"});

}  // namespace api_json
}  // namespace httpd
}  // namespace seastar
