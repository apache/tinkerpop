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

Feature: Step - has()

  Scenario: g_V_outXcreatedX_hasXname__mapXlengthX_isXgtX3XXX_name
    Given the modern graph
    And using the parameter l1 defined as "c[it.get().length()]"
    And the traversal of
      """
      g.V().out("created").has("name", __.map(l1).is(P.gt(3))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |

  Scenario: g_VX1X_hasXnameX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).has("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_VX1X_hasXcircumferenceX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).has("circumference")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VX1X_hasXname_markoX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).has("name", "marko")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_VX2X_hasXname_markoX
    Given the modern graph
    And using the parameter v1Id defined as "v[vadas].id"
    And the traversal of
      """
      g.V(v1Id).has("name", "marko")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasXname_markoX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", "marko")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_V_hasXname_blahX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", "blah")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasXage_gt_30X
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", P.gt(30))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[peter] |

  Scenario: g_V_hasXage_isXgt_30XX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", __.is(P.gt(30)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[peter] |

  Scenario: g_VX1X_hasXage_gt_30X
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).has("age", P.gt(30))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VX2X_hasXage_gt_30X
    Given the modern graph
    And using the parameter v2Id defined as "v[josh].id"
    And the traversal of
      """
      g.V(v2Id).has("age", P.gt(30))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

  Scenario: g_VXv1X_hasXage_gt_30X
    Given the modern graph
    And using the parameter v1 defined as "v[marko]"
    And the traversal of
      """
      g.V(v1).has("age", P.gt(30))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VXv2X_hasXage_gt_30X
    Given the modern graph
    And using the parameter v2 defined as "v[josh]"
    And the traversal of
      """
      g.V(v2).has("age", P.gt(30))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

  Scenario: g_VX1X_out_hasIdX2X
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter v2Id defined as "v[vadas].id"
    And the traversal of
      """
      g.V(v1Id).out().hasId(v2Id)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_VX1X_out_hasXid_2AsString_3AsStringX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].sid"
    And using the parameter v2Id defined as "v[vadas].sid"
    And using the parameter v3Id defined as "v[lop].sid"
    And the traversal of
      """
      g.V(v1Id).out().hasId(v2Id, v3Id)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |

  Scenario: g_V_hasXblahX
    Given the modern graph
    And the traversal of
      """
      g.V().has("blah")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_EX7X_hasXlabelXknowsX
    Given the modern graph
    And using the parameter e7Id defined as "e[marko-knows->vadas].id"
    And the traversal of
      """
      g.E(e7Id).hasLabel("knows")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |

  Scenario: g_E_hasXlabelXknowsX
    Given the modern graph
    And the traversal of
      """
      g.E().hasLabel("knows")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |
      | e[marko-knows->josh] |

  Scenario: g_E_hasLabelXuses_traversesX
    Given the crew graph
    And the traversal of
      """
      g.E().hasLabel("uses", "traverses")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-uses->gremlin] |
      | e[marko-uses->tinkergraph] |
      | e[stephen-uses->gremlin] |
      | e[stephen-uses->tinkergraph] |
      | e[daniel-uses->gremlin] |
      | e[daniel-uses->tinkergraph] |
      | e[matthias-uses->gremlin] |
      | e[matthias-uses->tinkergraph] |
      | e[gremlin-traverses->tinkergraph] |

  Scenario: g_V_hasLabelXperson_software_blahX
    Given the modern graph
    And the traversal of
    """
    g.V().hasLabel("person","software", "blah")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |
      | v[lop] |
      | v[ripple] |

  Scenario: g_V_hasXperson_name_markoX_age
    Given the modern graph
    And the traversal of
    """
    g.V().has("person", "name", "marko").values("age")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |

  Scenario: g_VX1X_outE_hasXweight_inside_0_06X_inV
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter lower defined as "d[0.0].d"
    And using the parameter upper defined as "d[0.6].d"
    And the traversal of
    """
    g.V(v1Id).outE().has("weight", P.inside(lower, upper)).inV()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |

  Scenario: g_EX11X_outV_outE_hasXid_10X
    Given the modern graph
    And using the parameter e11Id defined as "e[josh-created->lop].id"
    And using the parameter e10Id defined as "e[josh-created->ripple].id"
    And the traversal of
    """
    g.E(e11Id).outV().outE().has(T.id, e10Id)
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->ripple] |

  Scenario: g_EX11X_outV_outE_hasXid_10AsStringX
    Given the modern graph
    And using the parameter e11Id defined as "e[josh-created->lop].sid"
    And using the parameter e10Id defined as "e[josh-created->ripple].sid"
    And the traversal of
    """
    g.E(e11Id).outV().outE().has(T.id, e10Id)
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->ripple] |

  Scenario: g_V_hasXlocationX
    Given the crew graph
    And the traversal of
    """
    g.V().has("location")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[matthias] |
      | v[stephen] |
      | v[daniel] |

  Scenario: g_V_hasLabelXpersonX_hasXage_notXlteX10X_andXnotXbetweenX11_20XXXX_andXltX29X_orXeqX35XXXX_name
    Given the modern graph
    And using the parameter d10 defined as "d[10].i"
    And using the parameter d11 defined as "d[11].i"
    And using the parameter d20 defined as "d[20].i"
    And using the parameter d29 defined as "d[29].i"
    And using the parameter d35 defined as "d[35].i"
    And the traversal of
    """
    g.V().hasLabel("person").has("age", P.not(P.lte(d10).and(P.not(P.between(d11, d20)))).and(P.lt(d29).or(P.eq(d35)))).values("name")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | peter |

  Scenario: g_V_in_hasIdXneqX1XX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
    """
    g.V().in().hasId(P.neq(v1Id))
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[josh] |
      | v[peter] |

  Scenario: g_V_hasXage_withinX27X_count
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", P.within(27)).count()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[1].l |

  Scenario: g_V_hasXage_withinX27_29X_count
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", P.within(27,29)).count()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[2].l |

  Scenario: g_V_hasXage_withoutX27X_count
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", P.without(27)).count()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[3].l |

  Scenario: g_V_hasXage_withoutX27_29X_count
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", P.without(27,29)).count()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[2].l |

