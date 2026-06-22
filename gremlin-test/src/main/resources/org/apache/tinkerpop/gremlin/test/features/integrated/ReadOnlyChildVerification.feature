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

@StepClassIntegrated
Feature: Read-Only Child Verification - mutating steps blocked in filter/lookup contexts

  # ===== FILTER CONTEXT: has() with mutating child traversals =====
  # All error scenarios use addV()/drop() which ComputerVerificationStrategy also rejects on OLAP
  # with a different message, so we exclude them from GraphComputer runs.

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_addVXxX_valuesXnameXX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", __.addV("x").values("name"))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_V_drop_constantXxXX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", __.V().drop().constant("x"))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_V_mapXaddVXxXX_valuesXnameXX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", __.V().map(__.addV("x")).values("name"))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_eqXaddVXxX_valuesXnameXXX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", P.eq(__.addV("x").values("name")))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_withinXaddVXxX_valuesXnameXXX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", P.within(__.addV("x").values("name")))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_gtXaddVXxX_valuesXageXXX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", P.gt(__.addV("x").values("age")))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  # ===== FILTER CONTEXT: is() with mutating child traversals =====

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_isXaddVXxX_valuesXageXX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(__.addV("x").values("age"))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_isXgtXaddVXxX_valuesXageXXX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.gt(__.addV("x").values("age")))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  # ===== LOOKUP CONTEXT: V()/E() with mutating child traversals =====

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_VXaddVXxX_idX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().V(__.addV("x").id())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  @GraphComputerVerificationMidENotSupported
  Scenario: g_V_EXaddVXxX_idX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().E(__.addV("x").id())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXaddVXxX_idX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V(__.addV("x").id())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_EXaddVXxX_idX_rejected
    Given the modern graph
    And the traversal of
      """
      g.E(__.addV("x").id())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  # ===== MUTATION CONTEXT: property(traversal) blocks ALL mutating steps =====

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_propertyXV_mapXdropX_projectXxX_byXnameXX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().property(__.V().map(__.drop()).project("x").by("name"))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_propertyXaddVXtempX_projectXkX_byXnameXX_rejected
    Given the modern graph
    And the traversal of
      """
      g.V().property(__.addV("temp").project("k").by("name"))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "mutating step"

  # ===== VALID TRAVERSALS: should NOT be rejected =====

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_VXvid1X_valuesXnameXX_passes_verification
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
  Scenario: g_V_hasXage_gtXVXvid1X_valuesXageXXX_passes_verification
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
  Scenario: g_V_VXoutXknowsX_idX_valuesXnameX_passes_verification
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).V(__.out("knows").id()).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |
