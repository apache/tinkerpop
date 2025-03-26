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

@StepClassFilter @StepHas
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
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).has("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_VX1X_hasXcircumferenceX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).has("circumference")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VX1X_hasXname_markoX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).has("name", "marko")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_VX1X_hasXname_markovarX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter xx1 defined as "marko"
    And the traversal of
      """
      g.V(vid1).has("name", xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_VX2X_hasXname_markoX
    Given the modern graph
    And using the parameter vid1 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid1).has("name", "marko")
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

  Scenario: g_V_hasXlabel_isXsoftwareXX
    Given the modern graph
    And the traversal of
      """
      g.V().has(T.label, __.is('software'))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[ripple] |

  Scenario: g_VX1X_hasXage_gt_30X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).has("age", P.gt(30))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasXpersonvar_age_gt_30X
    Given the modern graph
    And using the parameter xx1 defined as "person"
    And the traversal of
      """
      g.V().has(xx1, "age", P.gt(30))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[peter] |
      | v[josh] |

  Scenario: g_VX4X_hasXage_gt_30X
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).has("age", P.gt(30))
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
    And the traversal of
      """
      g.V(v4).has("age", P.gt(30))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

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

  Scenario: g_V_hasXblahX
    Given the modern graph
    And the traversal of
      """
      g.V().has("blah")
      """
    When iterated to list
    Then the result should be empty

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

  Scenario: g_V_hasXperson_name_markovarX_age
    Given the modern graph
    And using the parameter xx1 defined as "marko"
    And the traversal of
    """
    g.V().has("person", "name", xx1).values("age")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |

  Scenario: g_V_hasXpersonvar_name_markoX_age
    Given the modern graph
    And using the parameter xx1 defined as "person"
    And the traversal of
    """
    g.V().has(xx1, "name", "marko").values("age")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |

  Scenario: g_VX1X_outE_hasXweight_inside_0_06X_inV
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
    """
    g.V(vid1).outE().has("weight", P.inside(0.0, 0.6)).inV()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |

  Scenario: g_EX11X_outV_outE_hasXid_10X
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And using the parameter eid10 defined as "e[josh-created->ripple].id"
    And the traversal of
    """
    g.E(eid11).outV().outE().has(T.id, eid10)
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->ripple] |

  Scenario: g_EX11X_outV_outE_hasXid_10AsStringX
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].sid"
    And using the parameter eid10 defined as "e[josh-created->ripple].sid"
    And the traversal of
    """
    g.E(eid11).outV().outE().has(T.id, eid10)
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->ripple] |

  @MultiProperties @MetaProperties
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

  Scenario: g_V_hasXage_withinX27_nullX_count
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", P.within(27,null)).count()
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

  Scenario: g_V_hasXperson_age_withinX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person", "age", P.within())
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasXperson_age_withoutX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person", "age", P.without())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |

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

 Scenario: g_V_hasXname_regexXrMarXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", TextP.regex("^mar"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

 Scenario: g_V_hasXname_notRegexXrMarXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", TextP.notRegex("^mar"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[vadas] |
      | v[lop] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_V_hasXname_regexXTinkerXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("software").property("name", "Apache TinkerPop©")
      """
    And the traversal of
      """
      g.V().has("name", TextP.regex("Tinker")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Apache TinkerPop© |

  Scenario: g_V_hasXname_regexXTinkerUnicodeXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("software").property("name", "Apache TinkerPop©")
      """
    And the traversal of
      """
      g.V().has("name", TextP.regex("Tinker.*\u00A9")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Apache TinkerPop© |

  Scenario: g_V_hasXp_neqXvXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("p", P.neq("v"))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasXage_gtX18X_andXltX30XXorXgtx35XXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", P.gt(18).and(P.lt(30)).or(P.gt(35)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |

  Scenario: g_V_hasXage_gtX18X_andXltX30XXorXltx35XXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", P.gt(18).and(P.lt(30)).and(P.lt(35)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |

  Scenario: g_V_hasXk_withinXcXX_valuesXkX
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("k", "轉注").addV().property("k", "✦").addV().property("k", "♠").addV().property("k", "A")
      """
    And the traversal of
      """
      g.V().has("k", P.within("轉注", "✦", "♠")).values("k")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | 轉注 |
      | ✦ |
      | ♠ |

  Scenario: g_V_hasXnullX
    Given the modern graph
    And the traversal of
    """
    g.V().has(null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasXnull_testnullkeyX
    Given the modern graph
    And the traversal of
    """
    g.V().has(null, "test-null-key")
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_E_hasXnullX
    Given the modern graph
    And the traversal of
    """
    g.E().has(null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasXlabel_personX
    Given the modern graph
    And the traversal of
    """
    g.V().has(T.label, "person")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |

  Scenario: g_V_hasXlabel_eqXpersonXX
    Given the modern graph
    And the traversal of
    """
    g.V().has(T.label, eq("person"))
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |

  Scenario: g_V_hasXlabel_isXpersonXX
    Given the modern graph
    And the traversal of
    """
    g.V().has(T.label, __.is("person"))
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |

  Scenario: g_V_hasXname_nullX
    Given the modern graph
    And the traversal of
    """
    g.V().has("name", null)
    """
    When iterated to list
    Then the result should be empty