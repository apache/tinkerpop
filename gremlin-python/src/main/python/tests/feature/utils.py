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
    g = traversal().withRemote(remote)

    # hold a map of name/vertex for use in asserting results
    return g.V().group().by('name').by(__.tail()).next()


@pick
def create_lookup_e(remote):
    g = traversal().withRemote(remote)

    # hold a map of the "name"/edge for use in asserting results - "name" in this context is in the form of
    # outgoingV-label->incomingV
    return g.E().group(). \
        by(lambda: ("it.outVertex().value('name') + '-' + it.label() + '->' + it.inVertex().value('name')", "gremlin-groovy")). \
        by(__.tail()).next()


@pick
def create_lookup_vp(remote):
    g = traversal().withRemote(remote)

    # hold a map of the "name"/vertexproperty for use in asserting results - "name" in this context is in the form of
    # vertexName-propName->propVal where the propVal must be typed according to the gherkin spec. note that the toy
    # graphs only deal in String/Int/Float/Double types so none of the other types are accounted for here.
    return g.V().properties().group(). \
        by(lambda: ("{ it -> \n" +
                    "  def val = it.value()\n" +
                    "  if (val instanceof Integer)\n" +
                    "    val = 'd[' + val + '].i'\n" +
                    "  else if (val instanceof Float)\n" +
                    "    val = 'd[' + val + '].f'\n" +
                    "  else if (val instanceof Double)\n" +
                    "    val = 'd[' + val + '].d'\n" +
                    "  return it.element().value('name') + '-' + it.key() + '->' + val\n" +
                    "}", "gremlin-groovy")). \
        by(__.tail()).next()
