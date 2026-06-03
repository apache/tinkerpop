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
Feature: Step - is() with traversal-bearing predicates

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_isXVXvid1X_valuesXageXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().values("age").is(__.V(vid1).values("age"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_isXVXvid1X_valuesXnameXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().values("name").is(__.V(vid1).values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_isXgtXVXvid1X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().values("age").is(P.gt(__.V(vid1).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32].i |
      | d[35].i |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_isXltXVXvid3X_valuesXageXXX
    Given the modern graph
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V().values("age").is(P.lt(__.V(vid3).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_isXneqXVXvid4X_valuesXageXXX
    Given the modern graph
    And using the parameter vid4 defined as "v[peter].id"
    And the traversal of
      """
      g.V().values("age").is(P.neq(__.V(vid4).values("age")))
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

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_isXVXvid1X_valuesXnonexistentXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().values("age").is(__.V(vid1).values("nonexistent"))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasLabelXpersonX_valuesXageX_chooseXgtXVXvid1X_valuesXageXX_olderX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().hasLabel("person").values("age").choose(P.gt(__.V(vid1).values("age")), __.constant("older than marko"), __.constant("not older"))
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
      g.V().hasLabel("person").values("age").choose(P.gte(__.V().hasLabel("person").values("age").mean()), __.constant("above average"), __.constant("below average"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | below average |
      | below average |
      | above average |
      | above average |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_outXknowsX_valuesXageX_fold_allXgteXVXvid2X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid1).out("knows").values("age").fold().all(P.gte(__.V(vid2).values("age")))
      """
    When iterated to list
    Then the result should have a count of 1

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_outXknowsX_valuesXageX_fold_allXgtXVXvid2X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid1).out("knows").values("age").fold().all(P.gt(__.V(vid2).values("age")))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_outXknowsX_valuesXageX_fold_anyXeqXVXvid3X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid1).out("knows").values("age").fold().any(P.eq(__.V(vid3).values("age")))
      """
    When iterated to list
    Then the result should have a count of 1

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_outXknowsX_valuesXageX_fold_noneXeqXVXvid4X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid4 defined as "v[peter].id"
    And the traversal of
      """
      g.V(vid1).out("knows").values("age").fold().none(P.eq(__.V(vid4).values("age")))
      """
    When iterated to list
    Then the result should have a count of 1

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_outXknowsX_valuesXageX_fold_noneXeqXVXvid3X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid1).out("knows").values("age").fold().none(P.eq(__.V(vid3).values("age")))
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
      g.inject("marko").choose(P.eq(__.V(9999).values("name")), __.constant("matched"), __.constant("unmatched"))
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
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_isXwithoutXVXvid1X_valuesXageX_VXvid2X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[josh].id"
    And the traversal of
      """
      g.V().values("age").is(P.without(__.V(vid1).values("age"), __.V(vid2).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |
      | d[35].i |
