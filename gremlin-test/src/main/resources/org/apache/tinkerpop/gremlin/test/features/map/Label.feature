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

@StepClassMap @StepLabel
Feature: Step - label()

  Scenario: g_V_label_single_label_graph
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").label()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | person |
      | person |
      | person |
      | person |

  @MultiLabel
  Scenario: g_V_label_deprecated_multilabel
    Given the zoo graph
    And the traversal of
      """
      g.V().has("name", "tux").label().count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |

  @MultiLabel
  Scenario: g_V_label_deprecated_multilabel_value_is_one_of_labels
    Given the zoo graph
    And the traversal of
      """
      g.V().has("name", "tux").filter(__.label().is(P.within("animal", "bird", "aquatic", "endangered")))
      """
    When iterated to list
    Then the result should have a count of 1

  @MultiLabel
  Scenario: g_E_label_deprecated_multilabel_graph
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").as("a").addV("person").as("b").addE("knows").from("a").to("b")
      """
    And the traversal of
      """
      g.E().label()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | knows |
