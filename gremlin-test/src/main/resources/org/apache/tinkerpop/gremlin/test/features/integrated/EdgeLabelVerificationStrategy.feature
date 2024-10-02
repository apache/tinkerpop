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
Feature: Step - EdgeLabelVerificationStrategy

  @WithEdgeLabelVerificationStrategy
  Scenario: g_withStrategiesXEdgeLabelVerificationStrategyXthrowException_true_logWarning_falseXX_V
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(EdgeLabelVerificationStrategy(throwException: true, logWarning: false)).V().out()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The provided traversal contains a vertex step without any specified edge label"

  @WithEdgeLabelVerificationStrategy
  Scenario: g_withStrategiesXEdgeLabelVerificationStrategyXthrowException_false_logWarning_falseXX_V
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(EdgeLabelVerificationStrategy(throwException: false, logWarning: false)).V().out()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[lop] |
      | v[lop] |
      | v[vadas] |
      | v[josh] |
      | v[ripple]  |

  Scenario: g_withoutStrategiesXEdgeLabelVerificationStrategyX_V
    Given the modern graph
    And the traversal of
      """
      g.withoutStrategies(EdgeLabelVerificationStrategy).V().out()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[lop] |
      | v[lop] |
      | v[vadas] |
      | v[josh] |
      | v[ripple]  |

