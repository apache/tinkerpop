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

@StepClassSideEffect @StepAddLabel
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

  @MultiLabel
  Scenario: g_E_addLabelXfriendX_labels_fold
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").as("a").addV("person").property("name", "josh").as("b").addE("knows").from("a").to("b")
      """
    And the traversal of
      """
      g.E().addLabel("friend").labels().fold()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Label mutation is not supported"

  Scenario: g_V_addLabelXemployeeX_single_label_graph
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person")
      """
    And the traversal of
      """
      g.V().hasLabel("person").addLabel("employee")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Label mutation is not supported"

  Scenario: g_V_addLabelXa_bX_single_label_graph
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person")
      """
    And the traversal of
      """
      g.V().hasLabel("person").addLabel("a", "b")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Label mutation is not supported"

  @MultiLabel
  Scenario: g_V_addLabelXconstantXemployeeXX_labels_count
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person")
      """
    And the traversal of
      """
      g.V().addLabel(constant("employee")).labels().count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[2].l |

  @MultiLabel
  Scenario: g_V_addLabelXconstantXlistXa_bXXX_labels_count
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person")
      """
    And the traversal of
      """
      g.V().addLabel(constant(["a", "b"])).labels().count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l |

  @MultiLabel
  Scenario: g_V_addLabelXconstantXaX_constantXbXX_labels_count
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person")
      """
    And the traversal of
      """
      g.V().addLabel(constant("a"), constant("b")).labels().count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l |

  @MultiLabel
  Scenario: g_V_addLabelXconstantXlistXa_bXX_constantXcXX_labels
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person")
      """
    And the traversal of
      """
      g.V().addLabel(constant(["a", "b"]), constant("c")).labels().fold()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Collection"