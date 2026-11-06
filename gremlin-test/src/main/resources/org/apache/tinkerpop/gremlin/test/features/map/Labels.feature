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
    Given the zoo graph
    And the traversal of
      """
      g.V().has("name", "tux").labels()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | animal |
      | bird |
      | aquatic |
      | endangered |

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

  Scenario: g_V_hasLabelXanimalX_projectXname_labelsX_byXnameX_byXlabels_order_foldX
    Given the zoo graph
    And the intent
      """
      For each animal, return a map of its name and all of its labels in alphabetical order.
      """
    And the traversal of
      """
      g.V().hasLabel("animal").project("name","labels").by("name").by(labels().order().fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name":"tux","labels":"l[animal,aquatic,bird,endangered]"}] |
      | m[{"name":"atlas","labels":"l[animal,aquatic,endangered,reptile]"}] |
      | m[{"name":"ripple","labels":"l[animal,aquatic,mammal]"}] |
      | m[{"name":"monty","labels":"l[animal,nocturnal,reptile]"}] |
      | m[{"name":"echo","labels":"l[animal,flying,mammal,nocturnal]"}] |
      | m[{"name":"blaze","labels":"l[animal,endangered,mammal,nocturnal]"}] |
      | m[{"name":"titan","labels":"l[animal,endangered,mammal]"}] |
      | m[{"name":"bitsy","labels":"l[animal,mammal,nocturnal]"}] |
      | m[{"name":"splash","labels":"l[animal,aquatic,endangered,mammal,nocturnal]"}] |
      | m[{"name":"tinker","labels":"l[animal,endangered,mammal,nocturnal]"}] |
