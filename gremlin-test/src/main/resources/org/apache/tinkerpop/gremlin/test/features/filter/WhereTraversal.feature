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

@StepClassFilter @StepWhere
Feature: Step - where(P) with traversal-bearing predicates

  # where(P.gt(traversal)) — compare current traverser value against resolved traversal result
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_whereXgtXVXvid1X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().values("age").where(P.gt(__.V(vid1).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32].i |
      | d[35].i |

  # where(P.lt(traversal)) — filter ages less than josh's age
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_whereXltXVXvid3X_valuesXageXXX
    Given the modern graph
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V().values("age").where(P.lt(__.V(vid3).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |

  # where(P.eq(traversal)) — exact match against resolved value
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_whereXeqXVXvid1X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().values("age").where(P.eq(__.V(vid1).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |

  # where(P.within(traversal)) — collection membership against resolved traversal results
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_whereXwithinXVXvid1X_outXknowsX_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().values("age").where(P.within(__.V(vid1).out("knows").values("age").fold()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |
      | d[32].i |

  # where(P.neq(traversal)) — not equal to resolved value
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_whereXneqXVXvid1X_valuesXnameXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().values("name").where(P.neq(__.V(vid1).values("name")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |

  # Empty traversal result — filters out (no match)
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_whereXeqXV9999_valuesXageXXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").where(P.eq(__.V(9999).values("age")))
      """
    When iterated to list
    Then the result should be empty

  # where(P.gt(traversal)) with by() modulator — compare property values
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_whereXgtXVXvid1X_valuesXageXXX_byXageX_valuesXnameX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().where(P.gt(__.V(vid1).values("age"))).by("age").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |
      | peter |
