'''
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
'''

import re
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from radish import before, after, world

outV = __.outV
label = __.label
inV = __.inV
project = __.project
tail = __.tail


@before.all
def prepare_static_traversal_source(features, marker):
    # as the various traversal sources for testing do not change their data, there is no need to re-create remotes
    # and client side lookup data over and over. it can be created once for all tests and be reused.
    cache = {}
    for graph_name in (("modern", "gmodern"), ("classic", "gclassic"), ("crew", "gcrew"), ("grateful", "ggrateful")):
        cache[graph_name[0]] = {}
        remote = __create_remote(graph_name[1])
        cache[graph_name[0]]["remote_conn"] = __create_remote(graph_name[1])
        cache[graph_name[0]]["lookup_v"] = __create_lookup_v(remote)
        cache[graph_name[0]]["lookup_e"] = __create_lookup_e(remote)

    # store the cache on the global context so that remotes can be shutdown cleanly at the end of the tests
    world.cache = cache

    # iterate each feature and apply the cached remotes/lookups to each scenario context so that they are
    # accessible to the feature steps for test logic
    for feature in features:
        for scenario in feature.all_scenarios:
            scenario.context.remote_conn = {}
            scenario.context.lookup_v = {}
            scenario.context.lookup_e = {}

            for graph_name in ("modern", "classic", "crew", "grateful"):
                scenario.context.remote_conn[graph_name] = cache[graph_name]["remote_conn"]
                scenario.context.lookup_v[graph_name] = cache[graph_name]["lookup_v"]
                scenario.context.lookup_e[graph_name] = cache[graph_name]["lookup_e"]


@before.each_scenario
def prepare_traversal_source(scenario):
    # some tests create data - create a fresh remote to the empty graph and clear that graph prior to each test
    remote = DriverRemoteConnection('ws://localhost:45940/gremlin', "ggraph")
    scenario.context.remote_conn["empty"] = remote
    g = Graph().traversal().withRemote(remote)
    g.V().drop().iterate()


@after.each_scenario
def close_traversal_source(scenario):
    scenario.context.remote_conn["empty"].close()


@after.all
def close_static_traversal_source(features, marker):
    for key, value in world.cache.iteritems():
        value["remote_conn"].close()


def __create_remote(server_graph_name):
    return DriverRemoteConnection('ws://localhost:45940/gremlin', server_graph_name)


def __create_lookup_v(remote):
    g = Graph().traversal().withRemote(remote)

    # hold a map of name/vertex for use in asserting results
    return g.V().group().by('name').by(tail()).next()


def __create_lookup_e(remote):
    g = Graph().traversal().withRemote(remote)

    # hold a map of the "name"/edge for use in asserting results - "name" in this context is in the form of
    # outgoingV-label->incomingV
    projection_of_edges = g.E().group(). \
        by(project("o", "l", "i").
           by(outV().values("name")).
           by(label()).
           by(inV().values("name"))). \
        by(tail()).next()
    edges = {}

    # in GraphSON 3.0 the "key" will be a dictionary and this can work more nicely - right now it's stuck as
    # a string and has to be parsed
    for key, value in projection_of_edges.items():
        o = re.search("o=(.+?)[,\}]", key).group(1)
        l = re.search("l=(.+?)[,\}]", key).group(1)
        i = re.search("i=(.+?)[,\}]", key).group(1)
        edges[o + "-" + l + "->" + i] = value

    return edges
