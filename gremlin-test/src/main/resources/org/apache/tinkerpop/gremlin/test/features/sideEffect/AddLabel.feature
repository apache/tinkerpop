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

@StepClassSideEffect @StepAddLabel @MultiLabel
Feature: Step - addLabel()

  @MultiLabel
  Scenario: g_V_hasLabelXpersonX_hasXname_markoX_addLabelXemployeeX_labels
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko")
      """
    And the traversal of
      """
      g.V().hasLabel("person").has("name", "marko").addLabel("employee").labels().fold()
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().hasLabel(\"person\").hasLabel(\"employee\")"

  @MultiLabel
  Scenario: g_V_addLabelXa_bX_labels_count
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person")
      """
    And the traversal of
      """
      g.V().addLabel("a", "b").labels().count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l |

  @MultiLabel
  Scenario: g_V_addLabelXexistingX_labels_count
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person")
      """
    And the traversal of
      """
      g.V().addLabel("person").labels().count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |
