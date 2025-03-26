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

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from radish import pick


@pick
def create_lookup_v(remote):
    g = traversal().with_(remote)

    # hold a map of name/vertex for use in asserting results
    return g.V().group().by('name').by(__.tail()).next()


@pick
def create_lookup_e(remote):
    g = traversal().with_(remote)

    # hold a map of the "name"/edge for use in asserting results - "name" in this context is in the form of
    # outgoingV-label->incomingV
    edges = {}
    edge_map = g.E().group(). \
        by(__.project('o', 'l', 'i').by(__.out_v().values('name')).by(__.label()).by(__.in_v().values('name'))). \
        by(__.tail()).next()

    for key, value in edge_map.items():
        edges[_get_edge_keys(key)] = value

    return edges


def _get_edge_keys(key_map):
    outV = key_map.get('o')
    label = key_map.get('l')
    inV = key_map.get('i')
    return f'{outV}-{label}->{inV}'


@pick
def create_lookup_vp(remote):
    g = traversal().with_(remote)

    # hold a map of the "name"/vertexproperty for use in asserting results - "name" in this context is in the form of
    # vertexName-propName->propVal where the propVal must be typed according to the gherkin spec. note that the toy
    # graphs only deal in String/Int/Float/Double types so none of the other types are accounted for here.
    vps = {}
    props = g.V().properties().group(). \
        by(__.project('n', 'k', 'v').by(__.element().values('name')).by(__.key()).by(__.value())). \
        by(__.tail()).next()

    for key, value in props.items():
        vps[_get_v_keys(key)] = value

    return vps


# since gremlin-lang doesn't allow lambdas, we are using the same logic as javascript to construct the keys
def _get_v_keys(key_map):
    k = key_map.get('k')
    val = key_map.get('v')
    if k == 'weight':
        val = f'd[{val}].d'
    elif k == 'age' or k == 'since' or k == 'skill':
        val = f'd[{val}].i'
    name = key_map.get('n')

    return f'{name}-{k}->{val}'
