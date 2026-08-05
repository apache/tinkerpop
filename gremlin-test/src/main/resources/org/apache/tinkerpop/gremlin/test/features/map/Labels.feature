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

  Scenario: g_V_hasXname_markoX_projectXlabelsX_byXT_labelsX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", "marko").project("labels").by(T.labels)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"labels": "s[person]"}] |

  @MultiLabel
  Scenario: g_V_hasXname_tuxX_projectXlabelsX_byXT_labelsX
    Given the zoo graph
    And the traversal of
      """
      g.V().has("name", "tux").project("labels").by(T.labels)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"labels": "s[animal,bird,aquatic,endangered]"}] |

  Scenario: g_V_hasXT_labels_personX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().has(T.labels, "person").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
      | peter |

  @MultiLabel
  Scenario: g_withXmultilabelX_V_hasXT_labels_withinXbird_reptileXX_valuesXnameX
    Given the zoo graph
    And the traversal of
      """
      g.with("multilabel").V().has(T.labels, P.within("bird", "reptile")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | tux |
      | atlas |
      | monty |
