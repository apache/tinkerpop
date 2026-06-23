# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@StepClassFilter @StepHas
Feature: Step - has()

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
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).has("age", P.gt(30))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VXv4X_hasXage_gt_30X
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

  Scenario: g_VX1X_out_hasXid_2X
    Given the modern graph
    And using the parameter vid2 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid2).has("age", P.gt(30))
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

  Scenario: g_V_hasXname_nullX
    Given the modern graph
    And the traversal of
    """
    g.V().has("name", null)
    """
    When iterated to list
    Then the result should be empty
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_VXvid1X_valuesXnameXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", __.V(vid1).values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  @GraphComputerVerificationMidVNotSupported
  # has(key, traversal) with multi-result child traversal - takes first result (order-dependent)
  @InsertionOrderingRequired
  Scenario: g_V_hasXname_VXvid1X_outXknowsX_valuesXnameXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", __.V(vid1).out("knows").values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  # has(key, traversal) with multi-result child traversal (age) - takes first result (order-dependent)
  @GraphComputerVerificationMidVNotSupported
  @InsertionOrderingRequired
  Scenario: g_V_hasXage_VXvid1X_outXknowsX_valuesXageXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("age", __.V(vid1).out("knows").values("age"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_VXvid1X_valuesXnonexistentXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", __.V(vid1).values("nonexistent"))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasXname_notXidentityXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", __.not(__.identity()))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_gtXVXvid1X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("age", P.gt(__.V(vid1).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_lteXVXvid2X_valuesXageXXX
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V().has("age", P.lte(__.V(vid2).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_neqXVXvid3X_valuesXageXXX
    Given the modern graph
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V().has("age", P.neq(__.V(vid3).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_eqXVXvid1X_valuesXnameXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", P.eq(__.V(vid1).values("name")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_ltXVXvid1X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("age", P.lt(__.V(vid1).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_gteXVXvid3X_valuesXageXXX
    Given the modern graph
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V().has("age", P.gte(__.V(vid3).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_eqXVXvid1X_valuesXnonexistentXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("age", P.eq(__.V(vid1).values("nonexistent")))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXlabel_VXvid1X_labelXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has(T.label, __.V(vid1).label())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXperson_name_VXvid1X_valuesXnameXX_age
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("person", "name", __.V(vid1).values("name")).values("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |

  # Multi-traversal within() where one traversal produces multiple results - use fold() to collect
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_withinXVXvid1X_outXknowsX_valuesXnameX_constantXpeterXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", P.within(__.V(vid1).out("knows").values("name").fold(), __.constant("peter")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[peter] |

  # Multi-traversal within() where one traversal produces no results - still matches on the other
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_withinXVXvid1X_valuesXnonexistentX_constantXmarkoXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", P.within(__.V(vid1).values("nonexistent"), __.constant("marko")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  # Multi-traversal within() where all traversals produce no results - filters everything
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_withinXVXvid1X_valuesXnonexistentX_VXvid1X_valuesXnonexistentXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", P.within(__.V(vid1).values("nonexistent"), __.V(vid1).values("nonexistent")))
      """
    When iterated to list
    Then the result should be empty

  # Multi-traversal without() with three traversals
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_withoutXVXvid1X_valuesXnameX_VXvid2X_valuesXnameX_VXvid3X_valuesXnameXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And using the parameter vid3 defined as "v[peter].id"
    And the traversal of
      """
      g.V().has("name", P.without(__.V(vid1).values("name"), __.V(vid2).values("name"), __.V(vid3).values("name")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[lop] |
      | v[ripple] |

  # Multi-traversal within() - union of relationship traversals from different sources
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_withinXVXvid1X_outXknowsX_valuesXnameX_VXvid3X_outXcreatedX_valuesXnameXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V().has("name", P.within(__.V(vid1).out("knows").values("name").fold(),
                                __.V(vid3).out("created").values("name").fold()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[ripple] |
      | v[lop] |

  # Multi-traversal without() - exclusion from multiple relationship sources
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasLabelXsoftwareX_hasXname_withoutXVXvid1X_outXcreatedX_valuesXnameX_VXvid3X_outXcreatedX_valuesXnameXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V().hasLabel("software").
        has("name", P.without(__.V(vid1).out("created").values("name").fold(),
                              __.V(vid3).out("created").values("name").fold()))
      """
    When iterated to list
    Then the result should be empty

  # Multi-traversal within() with is() - cross-label dynamic filtering, use fold() for multi-result
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasLabelXpersonX_valuesXageX_isXwithinXVXvid1X_valuesXageX_V_hasXname_lopX_inXcreatedX_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().hasLabel("person").values("age").
        is(P.within(__.V(vid1).values("age").fold(),
                    __.V().has("name","lop").in("created").values("age").fold()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[32].i |
      | d[35].i |

  # Multi-traversal within() - dynamic edge filtering via inV property check
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_outEXknowsX_filterXinV_hasXname_withinXV_hasXname_lopX_inXcreatedX_valuesXnameX_V_hasXname_rippleX_inXcreatedX_valuesXnameXXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("knows").
        filter(__.inV().has("name", P.within(__.V().has("name","lop").in("created").values("name").fold(),
                                             __.V().has("name","ripple").in("created").values("name").fold())))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->josh] |

  # hasLabel with a child traversal
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasLabelXVXvid1X_labelXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().hasLabel(__.V(vid1).label())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |

  # hasId with P.eq(traversal)
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasIdXeqXVXvid1X_idXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().hasId(P.eq(__.V(vid1).id()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  # hasId with traversal passed directly
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasIdXVXvid1X_idXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().hasId(__.V(vid1).id())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  # hasKey with a child traversal
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_propertiesXageX_hasKeyXVXvid1X_propertiesXageX_keyXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().properties("age").hasKey(__.V(vid1).properties("age").key())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vp[marko-age->d[29].i] |
      | vp[vadas-age->d[27].i] |
      | vp[josh-age->d[32].i] |
      | vp[peter-age->d[35].i] |

  # hasKey with P.eq(traversal)
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_propertiesXageX_hasKeyXeqXVXvid1X_propertiesXageX_keyXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().properties("age").hasKey(P.eq(__.V(vid1).properties("age").key()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vp[marko-age->d[29].i] |
      | vp[vadas-age->d[27].i] |
      | vp[josh-age->d[32].i] |
      | vp[peter-age->d[35].i] |

  # hasValue with a child traversal
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_propertiesXageX_hasValueXVXvid1X_valuesXageXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().properties("age").hasValue(__.V(vid1).values("age"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vp[marko-age->d[29].i] |

  # hasValue with P.eq(traversal)
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_propertiesXageX_hasValueXeqXVXvid1X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().properties("age").hasValue(P.eq(__.V(vid1).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vp[marko-age->d[29].i] |
