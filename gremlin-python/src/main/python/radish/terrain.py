#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import os

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver import serializer
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from radish import before, after, world

outV = __.out_v
label = __.label
inV = __.in_v
project = __.project
tail = __.tail
gremlin_server_url = os.environ.get('GREMLIN_SERVER_URL', 'http://localhost:{}/gremlin')
test_no_auth_url = gremlin_server_url.format(45940)

@before.all
def prepare_static_traversal_source(features, marker):
    # as the various traversal sources for testing do not change their data, there is no need to re-create remotes
    # and client side lookup data over and over. it can be created once for all tests and be reused.
    cache = {}
    for graph_name in (("modern", "gmodern"), ("classic", "gclassic"), ("crew", "gcrew"), ("grateful", "ggrateful"), ("sink", "gsink")):
        cache[graph_name[0]] = {}
        remote = __create_remote(graph_name[1])
        cache[graph_name[0]]["remote_conn"] = __create_remote(graph_name[1])
        cache[graph_name[0]]["lookup_v"] = world.create_lookup_v(remote)
        cache[graph_name[0]]["lookup_e"] = world.create_lookup_e(remote)
        cache[graph_name[0]]["lookup_vp"] = world.create_lookup_vp(remote)

    # store the cache on the global context so that remotes can be shutdown cleanly at the end of the tests
    world.cache = cache

    # iterate each feature and apply the cached remotes/lookups to each scenario context so that they are
    # accessible to the feature steps for test logic
    for feature in features:
        for scenario in feature.all_scenarios:
            scenario.context.remote_conn = {}
            scenario.context.lookup_v = {}
            scenario.context.lookup_e = {}
            scenario.context.lookup_vp = {}

            for graph_name in ("modern", "classic", "crew", "grateful", "sink"):
                scenario.context.remote_conn[graph_name] = cache[graph_name]["remote_conn"]
                scenario.context.lookup_v[graph_name] = cache[graph_name]["lookup_v"]
                scenario.context.lookup_e[graph_name] = cache[graph_name]["lookup_e"]
                scenario.context.lookup_vp[graph_name] = cache[graph_name]["lookup_vp"]

            # setup the "empty" lookups as needed
            scenario.context.lookup_v["empty"] = {}
            scenario.context.lookup_e["empty"] = {}
            scenario.context.lookup_vp["empty"] = {}


@before.each_scenario
def prepare_traversal_source(scenario):
    # some tests create data - create a fresh remote to the empty graph and clear that graph prior to each test
    remote = __create_remote("ggraph")
    scenario.context.remote_conn["empty"] = remote
    scenario.context.traversals = world.gremlins.get(scenario.sentence, None)
    g = traversal().with_(remote)
    g.V().drop().iterate()


@after.each_scenario
def close_traversal_source(scenario):
    scenario.context.remote_conn["empty"].close()


@after.all
def close_static_traversal_source(features, marker):
    for key, value in world.cache.items():
        value["remote_conn"].close()


def __create_remote(server_graph_name):
    if not("serializer" in world.config.user_data):
        raise ValueError('test configuration requires setting of --user-data="serializer={mime-type}"')

    if world.config.user_data["serializer"] == "application/vnd.graphbinary-v4.0":
        s = serializer.GraphBinarySerializersV4()
    elif world.config.user_data["serializer"] == "application/vnd.gremlin-v4.0+json":
        s = serializer.GraphSONSerializersV4()
    else:
        raise ValueError('serializer not found - ' + world.config.user_data["serializer"])

    bulking = world.config.user_data["bulking"] == "true" if "bulking" in world.config.user_data else False

    return DriverRemoteConnection(test_no_auth_url, server_graph_name,
                                  request_serializer=s, response_serializer=s,
                                  bulk_results=bulking)
