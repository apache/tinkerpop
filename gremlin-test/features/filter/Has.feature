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

  Scenario: g_VX4X_hasXage_gt_30X
    Given the modern graph
    And using the parameter v4Id defined as "v[josh].id"
    And the traversal of
      """
      g.V(v4Id).has("age", P.gt(30))
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

  Scenario: g_VXv4X_hasXage_gt_30X
    Given the modern graph
    And using the parameter v4 defined as "v[josh]"
    And using the parameter d30 defined as "d[30].i"
    And the traversal of
      """
      g.V(v4).has("age", P.gt(d30))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

  Scenario: g_VX1X_out_hasXid_lt_3X
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter v3Id defined as "v[lop].id"
    And the traversal of
      """
      g.V(v1Id).out().has(T.id, P.lt(v3Id))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_VX1AsStringX_out_hasXid_2AsStringX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].sid"
    And using the parameter v2Id defined as "v[vadas].sid"
    And the traversal of
      """
      g.V(v1Id).out().hasId(v2Id)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_VX1X_out_hasXid_2X
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

  Scenario: g_VX1X_out_hasXid_2_3X
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter v2Id defined as "v[vadas].id"
    And using the parameter v3Id defined as "v[lop].id"
    And the traversal of
      """
      g.V(v1Id).out().hasId(v2Id, v3Id)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |

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

  Scenario: g_V_both_dedup_properties_hasKeyXageX_value
    Given the modern graph
    And the traversal of
    """
    g.V().both().properties().dedup().hasKey("age").value()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_both_dedup_properties_hasKeyXageX_hasValueXgtX30XX_value
    Given the modern graph
    And using the parameter d30 defined as "d[30].i"
    And the traversal of
    """
    g.V().both().properties().dedup().hasKey("age").hasValue(P.gt(d30)).value()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_bothE_properties_dedup_hasKeyXweightX_value
    Given the modern graph
    And the traversal of
    """
    g.V().bothE().properties().dedup().hasKey("weight").value()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.5].d |
      | d[1.0].d |
      | d[0.4].d |
      | d[0.2].d |

  Scenario: g_V_bothE_properties_dedup_hasKeyXweightX_hasValueXltX0d3XX_value
    Given the modern graph
    And using the parameter d0d3 defined as "d[0.3].d"
    And the traversal of
    """
    g.V().bothE().properties().dedup().hasKey("weight").hasValue(P.lt(0.3)).value()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.2].d |

  Scenario: g_V_hasNotXageX_name
    Given the modern graph
    And the traversal of
    """
    g.V().hasNot("age").values("name")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | ripple |

  Scenario: g_V_hasIdX1X_hasIdX2X
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter v2Id defined as "v[vadas].id"
    And the traversal of
    """
    g.V().hasId(v1Id).hasId(v2Id)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasLabelXpersonX_hasLabelXsoftwareX
    Given the modern graph
    And the traversal of
    """
    g.V().hasLabel("person").hasLabel("software")
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasIdXemptyX_count
    Given the modern graph
    And using the parameter l defined as "l[]"
    And the traversal of
    """
    g.V().hasId(l).count()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].l |

  Scenario: g_V_hasIdXwithinXemptyXX_count
    Given the modern graph
    And using the parameter l defined as "l[]"
    And the traversal of
    """
    g.V().hasId(P.within(l)).count()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].l |

  Scenario: g_V_hasIdXwithoutXemptyXX_count
    Given the modern graph
    And using the parameter l defined as "l[]"
    And the traversal of
    """
    g.V().hasId(P.without(l)).count()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_V_notXhasIdXwithinXemptyXXX_count
    Given the modern graph
    And using the parameter l defined as "l[]"
    And the traversal of
    """
    g.V().not(__.hasId(P.within(l))).count()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_V_hasXname_containingXarkXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", TextP.containing("ark"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_V_hasXname_startingWithXmarXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", TextP.startingWith("mar"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_V_hasXname_endingWithXasXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", TextP.endingWith("as"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_V_hasXperson_name_containingXoX_andXltXmXXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person", "name", TextP.containing("o").and(P.lt("m")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

  Scenario: g_V_hasXname_gtXmX_andXcontainingXoXXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", P.gt("m").and(TextP.containing("o")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_V_hasXname_not_containingXarkXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", TextP.notContaining("ark"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_V_hasXname_not_startingWithXmarXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", TextP.notStartingWith("mar"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_V_hasXname_not_endingWithXasXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", TextP.notEndingWith("as"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |
