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

@StepClassMap @StepCall @TinkerServiceRegistry
Feature: Step - call()

  Scenario: g_call
    Given the empty graph
    And the traversal of
      """
      g.call()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | tinker.search |
      | tinker.degree.centrality |

  Scenario: g_callXlistX
    Given the empty graph
    And the traversal of
      """
      g.call("--list")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | tinker.search |
      | tinker.degree.centrality |

  Scenario: g_callXlistX_withXstring_stringX
    Given the empty graph
    And the traversal of
      """
      g.call("--list").with("service", "tinker.search")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | tinker.search |

  Scenario: g_callXlistX_withXstring_traversalX
    Given the empty graph
    And the traversal of
      """
      g.call("--list").with("service", __.constant("tinker.search"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | tinker.search |

  Scenario: g_callXlist_mapX
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"service\": \"tinker.search\"}]"
    And the traversal of
      """
      g.call("--list", xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | tinker.search |

  Scenario: g_callXlist_traversalX
    Given the empty graph
    And the traversal of
      """
      g.call("--list", __.project("service").by(__.constant("tinker.search")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | tinker.search |

# The parameterized xx1 map will fail, but just passing in the map will pass, need to check
  Scenario: g_callXlist_map_traversalX
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"x\": \"y\"}]"
    And the traversal of
      """
      g.call("--list", xx1, __.project("service").by(__.constant("tinker.search")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | tinker.search |

  Scenario: g_callXsearch_mapX
    Given the modern graph
    And using the parameter xx1 defined as "m[{\"search\": \"mar\"}]"
    And the traversal of
      """
      g.call("tinker.search", xx1).element()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_callXsearch_traversalX
    Given the modern graph
    And the traversal of
      """
      g.call("tinker.search", __.project("search").by(__.constant("vada"))).element()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_callXsearchX_withXstring_stringX
    Given the modern graph
    And the traversal of
      """
      g.call("tinker.search").with("search", "vada").element()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_callXsearchX_withXstring_traversalX
    Given the modern graph
    And the traversal of
      """
      g.call("tinker.search").with("search", __.constant("vada")).element()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_callXsearch_mapX_withXstring_VertexX
    Given the modern graph
    And using the parameter xx1 defined as "m[{\"search\": \"mar\"}]"
    And the traversal of
      """
      g.call("tinker.search", xx1).with("type", "Vertex").element()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_callXsearch_mapX_withXstring_EdgeX
    Given the modern graph
    And using the parameter xx1 defined as "m[{\"search\": \"mar\"}]"
    And the traversal of
      """
      g.call("tinker.search", xx1).with("type", "Edge").element()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_callXsearch_mapX_withXstring_VertexPropertyX
    Given the modern graph
    And using the parameter xx1 defined as "m[{\"search\": \"mar\"}]"
    And the traversal of
      """
      g.call("tinker.search", xx1).with("type", "VertexProperty").element()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_callXdcX
    Given the modern graph
    And the traversal of
      """
      g.V().as("v").call("tinker.degree.centrality").project("vertex", "degree").by(select("v")).by()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"vertex": "v[marko]",  "degree": "d[0].l"}] |
      | m[{"vertex": "v[vadas]",  "degree": "d[1].l"}] |
      | m[{"vertex": "v[lop]",    "degree": "d[3].l"}] |
      | m[{"vertex": "v[josh]",   "degree": "d[1].l"}] |
      | m[{"vertex": "v[ripple]", "degree": "d[1].l"}] |
      | m[{"vertex": "v[peter]",  "degree": "d[0].l"}] |

  Scenario: g_V_whereXcallXdcXX
    Given the modern graph
    And the traversal of
      """
      g.V().where(call("tinker.degree.centrality").is(3))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |

  Scenario: g_V_callXdcX_withXdirection_OUTX
    Given the modern graph
    And the traversal of
      """
      g.V().as("v").call("tinker.degree.centrality").with("direction", OUT).project("vertex", "degree").by(select("v")).by()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"vertex": "v[marko]",  "degree": "d[3].l"}] |
      | m[{"vertex": "v[vadas]",  "degree": "d[0].l"}] |
      | m[{"vertex": "v[lop]",    "degree": "d[0].l"}] |
      | m[{"vertex": "v[josh]",   "degree": "d[2].l"}] |
      | m[{"vertex": "v[ripple]", "degree": "d[0].l"}] |
      | m[{"vertex": "v[peter]",  "degree": "d[1].l"}] |

  Scenario: g_V_callXdc_mapX_withXdirection_OUTX
    Given the modern graph
    And using the parameter xx1 defined as "m[{\"x\": \"y\"}]"
    And the traversal of
      """
      g.V().as("v").call("tinker.degree.centrality", xx1).with("direction", OUT).project("vertex", "degree").by(select("v")).by()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"vertex": "v[marko]",  "degree": "d[3].l"}] |
      | m[{"vertex": "v[vadas]",  "degree": "d[0].l"}] |
      | m[{"vertex": "v[lop]",    "degree": "d[0].l"}] |
      | m[{"vertex": "v[josh]",   "degree": "d[2].l"}] |
      | m[{"vertex": "v[ripple]", "degree": "d[0].l"}] |
      | m[{"vertex": "v[peter]",  "degree": "d[1].l"}] |

  Scenario: g_V_callXdc_traversalX
    Given the modern graph
    And the traversal of
      """
      g.V().as("v").call("tinker.degree.centrality", __.project("direction").by(__.constant(OUT))).project("vertex", "degree").by(select("v")).by()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"vertex": "v[marko]",  "degree": "d[3].l"}] |
      | m[{"vertex": "v[vadas]",  "degree": "d[0].l"}] |
      | m[{"vertex": "v[lop]",    "degree": "d[0].l"}] |
      | m[{"vertex": "v[josh]",   "degree": "d[2].l"}] |
      | m[{"vertex": "v[ripple]", "degree": "d[0].l"}] |
      | m[{"vertex": "v[peter]",  "degree": "d[1].l"}] |

  Scenario: g_V_callXdc_map_traversalX
    Given the modern graph
    And using the parameter xx1 defined as "m[{\"x\": \"y\"}]"
    And the traversal of
      """
      g.V().as("v").call("tinker.degree.centrality", xx1, __.project("direction").by(__.constant(OUT))).project("vertex", "degree").by(select("v")).by()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"vertex": "v[marko]",  "degree": "d[3].l"}] |
      | m[{"vertex": "v[vadas]",  "degree": "d[0].l"}] |
      | m[{"vertex": "v[lop]",    "degree": "d[0].l"}] |
      | m[{"vertex": "v[josh]",   "degree": "d[2].l"}] |
      | m[{"vertex": "v[ripple]", "degree": "d[0].l"}] |
      | m[{"vertex": "v[peter]",  "degree": "d[1].l"}] |

