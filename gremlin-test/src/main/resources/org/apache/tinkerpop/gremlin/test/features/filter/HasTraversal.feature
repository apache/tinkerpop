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
Feature: Step - has() with traversal arguments

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
  # has(key, traversal) with multi-result child traversal — takes first result (order-dependent)
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

  # has(key, traversal) with multi-result child traversal (age) — takes first result (order-dependent)
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

  @GraphComputerVerificationMidVNotSupported
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

  # Multi-traversal within() where one traversal produces multiple results
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_withinXVXvid1X_outXknowsX_valuesXnameX_constantXpeterXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", P.within(__.V(vid1).out("knows").values("name"), __.constant("peter")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[peter] |

  # Multi-traversal within() where one traversal produces no results — still matches on the other
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

  # Multi-traversal within() where all traversals produce no results — filters everything
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

  # Multi-traversal within() — union of relationship traversals from different sources
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_withinXVXvid1X_outXknowsX_valuesXnameX_VXvid3X_outXcreatedX_valuesXnameXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V().has("name", P.within(__.V(vid1).out("knows").values("name"), __.V(vid3).out("created").values("name")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[ripple] |
      | v[lop] |

  # Multi-traversal without() — exclusion from multiple relationship sources
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasLabelXsoftwareX_hasXname_withoutXVXvid1X_outXcreatedX_valuesXnameX_VXvid3X_outXcreatedX_valuesXnameXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V().hasLabel("software").has("name", P.without(__.V(vid1).out("created").values("name"), __.V(vid3).out("created").values("name")))
      """
    When iterated to list
    Then the result should be empty

  # Multi-traversal within() with is() — cross-label dynamic filtering
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasLabelXpersonX_valuesXageX_isXwithinXVXvid1X_valuesXageX_V_hasXname_lopX_inXcreatedX_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().hasLabel("person").values("age").is(P.within(__.V(vid1).values("age"), __.V().has("name","lop").in("created").values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[32].i |
      | d[35].i |

  # Multi-traversal within() — dynamic edge filtering via inV property check
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_outEXknowsX_filterXinV_hasXname_withinXV_hasXname_lopX_inXcreatedX_valuesXnameX_V_hasXname_rippleX_inXcreatedX_valuesXnameXXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("knows").filter(__.inV().has("name", P.within(__.V().has("name","lop").in("created").values("name"), __.V().has("name","ripple").in("created").values("name"))))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->josh] |
