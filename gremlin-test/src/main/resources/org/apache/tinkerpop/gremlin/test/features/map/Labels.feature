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

@StepClassMap @StepLabels
Feature: Step - labels()

  @MultiLabel
  Scenario: g_V_hasLabelXpersonX_labels
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").labels()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | person |
      | person |
      | person |
      | person |

  @MultiLabel
  Scenario: g_V_labels_multilabel
    Given the empty graph
    And the graph initializer of
      """
      g.addV("a", "b")
      """
    And the traversal of
      """
      g.V().labels()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | a |
      | b |

  @MultiLabel
  Scenario: g_addVXa_bX_labels_count
    Given the empty graph
    And the graph initializer of
      """
      g.addV("a", "b")
      """
    And the traversal of
      """
      g.V().labels().count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[2].l |

  @MultiLabel
  Scenario: g_addV_labels
    Given the empty graph
    And the graph initializer of
      """
      g.addV()
      """
    And the traversal of
      """
      g.V().labels()
      """
    When iterated to list
    Then the result should have a count of 0

  @MultiLabel
  Scenario: g_E_labels
    Given the modern graph
    And the traversal of
      """
      g.E().hasLabel("knows").labels()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | knows |
      | knows |

  Scenario: g_V_hasLabelXpersonX_labels_single_label_graph
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").labels()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | person |
      | person |
      | person |
      | person |

  Scenario: g_V_labels_single_label_graph
    Given the modern graph
    And the traversal of
      """
      g.V().labels()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | person |
      | person |
      | person |
      | person |
      | software |
      | software |

  Scenario: g_E_labels_single_label_graph
    Given the modern graph
    And the traversal of
      """
      g.E().hasLabel("knows").labels()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | knows |
      | knows |

  @MultiLabel
  Scenario: g_V_label_deprecated_multilabel
    Given the empty graph
    And the graph initializer of
      """
      g.addV("a", "b").property("name", "test")
      """
    And the traversal of
      """
      g.V().label().count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |

  @MultiLabel
  Scenario: g_V_label_deprecated_multilabel_value_is_one_of_labels
    Given the empty graph
    And the graph initializer of
      """
      g.addV("a", "b").property("name", "test")
      """
    And the traversal of
      """
      g.V().filter(__.label().is(P.within("a", "b")))
      """
    When iterated to list
    Then the result should have a count of 1