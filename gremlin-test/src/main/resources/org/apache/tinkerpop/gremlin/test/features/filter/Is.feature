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

@StepClassFilter @StepIs
Feature: Step - is()

  @TinyGremlin
  Scenario: g_V_valuesXageX_isX32X
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(32)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32].i |

  @TinyGremlin
  Scenario: g_V_valuesXageX_isX32varX
    Given the modern graph
    And using the parameter xx1 defined as "d[32].i"
    And the traversal of
      """
      g.V().values("age").is(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32].i |

  @TinyGremlin
  Scenario: g_V_valuesXageX_isXlte_30X
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.lte(30))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |
      | d[29].i |

  @TinyGremlin
  Scenario: g_V_valuesXageX_isXlte_30varX
    Given the modern graph
    And using the parameter xx1 defined as "d[30].i"
    And the traversal of
      """
      g.V().values("age").is(P.lte(xx1))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |
      | d[29].i |

  @TinyGremlin
  Scenario: g_V_valuesXageX_isXgte_29X_isXlt_34X
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.gte(29)).is(P.lt(34))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[32].i |

  @TinyGremlin
  Scenario: g_V_valuesXageX_isXgte_29vaarX_isXlt_34varX
    Given the modern graph
    And using the parameter xx1 defined as "d[29].i"
    And using the parameter xx2 defined as "d[34].i"
    And the traversal of
      """
      g.V().values("age").is(P.gte(xx1)).is(P.lt(xx2))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[32].i |

  Scenario: g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.in("created").count().is(1)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |

  Scenario: g_V_whereXinXcreatedX_count_isXgte_2XX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.in("created").count().is(P.gte(2))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
  Scenario: g_V_valuesXageX_isXconstantX29XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(__.constant(29))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |

  Scenario: g_V_valuesXnameX_isXconstantXmarkoXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").is(__.constant("marko"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: g_V_valuesXageX_isXgtXconstantX29XXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.gt(__.constant(29)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_valuesXageX_isXltXconstantX32XXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.lt(__.constant(32)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |

  Scenario: g_V_valuesXageX_isXneqXconstantX35XXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.neq(__.constant(35)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_isXwithinXVXvid1X_outXknowsX_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().values("age").is(P.within(__.V(vid1).out("knows").values("age").fold()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |
      | d[32].i |

  Scenario: g_V_valuesXageX_isXconstantX99999XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(__.constant(99999))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasLabelXpersonX_valuesXageX_chooseXgtXconstantX29XX_olderX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("age").
        choose(__.is(P.gt(__.constant(29))),
               __.constant("older than marko"),
               __.constant("not older"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | not older |
      | not older |
      | older than marko |
      | older than marko |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasLabelXpersonX_valuesXageX_chooseXgteXmeanAgeX_aboveX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("age").
        choose(__.is(P.gte(__.V().hasLabel("person").values("age").mean())),
               __.constant("above average"),
               __.constant("below average"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | below average |
      | below average |
      | above average |
      | above average |

  Scenario: g_VXvid1X_outXknowsX_valuesXageX_fold_allXgteXconstantX27XXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("knows").values("age").fold().all(P.gte(__.constant(27)))
      """
    When iterated to list
    Then the result should have a count of 1

  Scenario: g_VXvid1X_outXknowsX_valuesXageX_fold_allXgtXconstantX27XXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("knows").values("age").fold().all(P.gt(__.constant(27)))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VXvid1X_outXknowsX_valuesXageX_fold_anyXeqXconstantX32XXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("knows").values("age").fold().any(P.eq(__.constant(32)))
      """
    When iterated to list
    Then the result should have a count of 1

  Scenario: g_VXvid1X_outXknowsX_valuesXageX_fold_noneXeqXconstantX35XXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("knows").values("age").fold().none(P.eq(__.constant(35)))
      """
    When iterated to list
    Then the result should have a count of 1

  Scenario: g_VXvid1X_outXknowsX_valuesXageX_fold_noneXeqXconstantX32XXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("knows").values("age").fold().none(P.eq(__.constant(32)))
      """
    When iterated to list
    Then the result should be empty

  # Empty-result handling: predicate should treat "no results" as "no match", not null.
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_injectXnullX_isXeqXV9999_valuesXnameXXX
    Given the modern graph
    And the traversal of
      """
      g.inject(null).is(P.eq(__.V(9999).values("name")))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_injectXmarkoX_isXV9999_valuesXnameXX
    Given the modern graph
    And the traversal of
      """
      g.inject("marko").is(__.V(9999).values("name"))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_injectXmarkoX_chooseXeqXV9999_valuesXnameXX_matched_unmatchedX
    Given the modern graph
    And the traversal of
      """
      g.inject("marko").choose(__.is(P.eq(__.V(9999).values("name"))), __.constant("matched"), __.constant("unmatched"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | unmatched |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_injectXlistX_noneXeqXV9999_valuesXnameXXX
    Given the modern graph
    And the traversal of
      """
      g.inject(["marko","josh"]).none(P.eq(__.V(9999).values("name")))
      """
    When iterated to list
    Then the result should have a count of 1

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_eqXV9999_valuesXnameXXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", P.eq(__.V(9999).values("name")))
      """
    When iterated to list
    Then the result should be empty

  # Multi-traversal without() in is() context
  Scenario: g_V_valuesXageX_isXwithoutXconstantX29X_constantX32XXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.without(__.constant(29), __.constant(32)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |
      | d[35].i |

  # Child traversal using select() to reference a labeled step value
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_asXaX_V_valuesXageX_isXgtXselectXaX_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").V().values("age").is(P.gt(__.select("a").values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32].i |
      | d[35].i |

  # Child traversal using sack() to filter against the traverser's sack value
  Scenario: g_withSackX30X_V_valuesXageX_isXgtXsackXX
    Given the modern graph
    And the traversal of
      """
      g.withSack(30).V().values("age").is(P.gt(__.sack()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32].i |
      | d[35].i |
