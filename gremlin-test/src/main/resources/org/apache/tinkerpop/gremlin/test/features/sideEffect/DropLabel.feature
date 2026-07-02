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

@StepClassSideEffect @StepDropLabel
Feature: Step - dropLabel() / dropLabels()

  @MultiLabel
  Scenario: g_V_dropLabelXaX_labels
    Given the empty graph
    And the graph initializer of
      """
      g.addV("a", "b")
      """
    And the traversal of
      """
      g.V().dropLabel("a").labels().fold()
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().hasLabel(\"a\")"
    And the graph should return 1 for count of "g.V().hasLabel(\"b\")"

  @MultiLabel
  Scenario: g_V_dropLabels_labels
    Given the empty graph
    And the graph initializer of
      """
      g.addV("a", "b")
      """
    And the traversal of
      """
      g.V().dropLabels().labels()
      """
    When iterated to list
    Then the result should have a count of 0

  @MultiLabel
  Scenario: g_V_dropLabelXa_bX_labels
    Given the empty graph
    And the graph initializer of
      """
      g.addV("a", "b", "c")
      """
    And the traversal of
      """
      g.V().dropLabel("a", "b").labels()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | c |

  @MultiLabel
  Scenario: g_V_dropLabels_defaultLabel
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person")
      """
    And the traversal of
      """
      g.V().dropLabels().labels()
      """
    When iterated to list
    Then the result should have a count of 0

  @MultiLabel
  Scenario: g_E_dropLabelXknowsX_labels
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").as("a").addV("person").as("b").addE("knows").from("a").to("b")
      """
    And the traversal of
      """
      g.E().dropLabel("knows").labels().fold()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Label mutation is not supported"

  @MultiLabel
  Scenario: g_E_dropLabels_labels
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").as("a").addV("person").as("b").addE("knows").from("a").to("b")
      """
    And the traversal of
      """
      g.E().dropLabels().labels()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Label mutation is not supported"

  @MultiLabel
  Scenario: g_V_dropLabelXnonExistentX_labels
    Given the empty graph
    And the graph initializer of
      """
      g.addV("a", "b")
      """
    And the traversal of
      """
      g.V().dropLabel("xyz").labels()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | a |
      | b |

  @GraphComputerVerificationStrategyNotSupported
  Scenario: g_V_dropLabelXpersonX_single_label_graph
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").dropLabel("person")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Label mutation is not supported"

  @GraphComputerVerificationStrategyNotSupported
  Scenario: g_V_dropLabels_single_label_graph
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").dropLabels()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Label mutation is not supported"