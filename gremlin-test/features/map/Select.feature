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

Feature: Step - select()

  Scenario: get_g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).as("a").out("knows").as("b").select("a", "b")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"a": "v[marko]", "b": "v[vadas]"}] |
      | m[{"a": "v[marko]", "b": "v[josh]"}] |

  Scenario: g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX_byXnameX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).as("a").out("knows").as("b").select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"a": "marko", "b": "vadas"}] |
      | m[{"a": "marko", "b": "josh"}] |

  Scenario: g_VX1X_asXaX_outXknowsX_asXbX_selectXaX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).as("a").out("knows").as("b").select("a")
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |
      | v[marko] |

  Scenario: g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).as("a").out("knows").as("b").select("a").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | marko |
      | marko |

  Scenario: g_V_asXaX_out_asXbX_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"a": "marko", "b": "lop"}] |
      | m[{"a": "marko", "b": "vadas"}] |
      | m[{"a": "marko", "b": "josh"}] |
      | m[{"a": "josh", "b": "ripple"}] |
      | m[{"a": "josh", "b": "lop"}] |
      | m[{"a": "peter", "b": "lop"}] |

  Scenario: g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().aggregate("x").as("b").select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"a": "marko", "b": "lop"}] |
      | m[{"a": "marko", "b": "vadas"}] |
      | m[{"a": "marko", "b": "josh"}] |
      | m[{"a": "josh", "b": "ripple"}] |
      | m[{"a": "josh", "b": "lop"}] |
      | m[{"a": "peter", "b": "lop"}] |

  Scenario: g_V_asXaX_name_order_asXbX_selectXa_bX_byXnameX_by_XitX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").values("name").order().as("b").select("a", "b").by("name").by()
      """
    When iterated to list
    Then the result should be unordered
      | m[{"a": "marko", "b": "marko"}] |
      | m[{"a": "vadas", "b": "vadas"}] |
      | m[{"a": "josh", "b": "josh"}] |
      | m[{"a": "ripple", "b": "ripple"}] |
      | m[{"a": "lop", "b": "lop"}] |
      | m[{"a": "peter", "b": "peter"}] |

  Scenario: g_V_hasXname_gremlinX_inEXusesX_order_byXskill_incrX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX
    Given the crew graph
    And the traversal of
      """
      g.V().has("name", "gremlin").inE("uses").order().by("skill", Order.incr).as("a").outV().as("b").select("a", "b").by("skill").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"a": 3, "b": "matthias"}] |
      | m[{"a": 4, "b": "marko"}] |
      | m[{"a": 5, "b": "stephen"}] |
      | m[{"a": 5, "b": "daniel"}] |

  Scenario: g_V_hasXname_isXmarkoXX_asXaX_selectXaX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", __.is("marko")).as("a").select("a")
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |

  Scenario: g_V_label_groupCount_asXxX_selectXxX
    Given the modern graph
    And the traversal of
      """
      g.V().label().groupCount().as("x").select("x")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"software": 2, "person": 4}] |

  Scenario: g_V_hasLabelXpersonX_asXpX_mapXbothE_label_groupCountX_asXrX_selectXp_rX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").as("p").map(__.bothE().label().groupCount()).as("r").select("p", "r")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"p": "v[marko]", "r": {"created": 1, "knows": 2}}] |
      | m[{"p": "v[vadas]", "r": {"knows": 1}}] |
      | m[{"p": "v[josh]", "r": {"created": 2, "knows": 1}}] |
      | m[{"p": "v[peter]", "r": {"created": 1}}] |

  Scenario: g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_chooseXselectXaX__selectXaX__selectXbXX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.outE().count().is(0L), __.as("a"), __.as("b")).choose(__.select("a"), __.select("a"), __.select("b"))
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_VX1X_asXhereX_out_selectXhereX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).as("here").out().select("here")
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |
      | v[marko] |
      | v[marko] |

  Scenario: g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX
    Given the modern graph
    And using the parameter v4Id is "v[josh].id"
    And the traversal of
      """
      g.V(v4Id).as("here").out().select("here")
      """
    When iterated to list
    Then the result should be unordered
      | v[josh] |
      | v[josh] |

  Scenario: g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name
    Given the modern graph
    And using the parameter v4Id is "v[josh].id"
    And the traversal of
      """
      g.V(v4Id).out().as("here").has("lang", "java").select("here").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | ripple |
      | lop |

  Scenario: g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).outE().as("here").inV().has("name", "vadas").select("here")
      """
    When iterated to list
    Then the result should be unordered
      | e[marko-knows->vadas] |

  Scenario: g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).outE("knows").has("weight", 1.0).as("here").inV().has("name", "josh").select("here")
      """
    When iterated to list
    Then the result should be unordered
      | e[marko-knows->josh] |

  Scenario: g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).outE("knows").as("here").has("weight", 1.0).as("fake").inV().has("name", "josh").select("here")
      """
    When iterated to list
    Then the result should be unordered
      | e[marko-knows->josh] |

  Scenario: g_V_asXhereXout_name_selectXhereX
    Given the modern graph
    And the traversal of
      """
      g.V().as("here").out().values("name").select("here")
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |
      | v[marko] |
      | v[marko] |
      | v[josh] |
      | v[josh] |
      | v[peter] |

  Scenario: g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().out("created").union(as("project").in("created").has("name", "marko").select("project"),as("project").in("created").in("knows").has("name", "marko").select("project")).groupCount().by("name")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"ripple": 1, "lop": 6}] |