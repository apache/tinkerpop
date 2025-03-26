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

@StepClassIntegrated
Feature: Step - ReservedKeysVerificationStrategy

  @WithReservedKeysVerificationStrategy
  Scenario: g_withStrategiesXReservedKeysVerificationStrategyXthrowException_trueXX_addVXpersonX_propertyXid_123X_propertyXname_markoX
    Given the empty graph
    And the traversal of
      """
      g.withStrategies(ReservedKeysVerificationStrategy(throwException: true)).addV("person").property("id", 123).property("name", "marko")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "is setting a property key to a reserved word: id"

  @WithReservedKeysVerificationStrategy
  Scenario: g_withStrategiesXReservedKeysVerificationStrategyXthrowException_trueXX_addVXpersonX_propertyXage_29X_propertyXname_markoX
    Given the empty graph
    And the traversal of
      """
      g.withStrategies(ReservedKeysVerificationStrategy(throwException: true, keys: ["age"])).addV("person").property("age", 29).property("name", "marko")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "is setting a property key to a reserved word: age"

  Scenario: g_withoutStrategiesXReservedKeysVerificationStrategyX_addVXpersonX_propertyXid_123X_propertyXname_markoX
    Given the empty graph
    And the traversal of
      """
      g.withoutStrategies(ReservedKeysVerificationStrategy).addV("person").property("id", 123).property("name", "marko")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
