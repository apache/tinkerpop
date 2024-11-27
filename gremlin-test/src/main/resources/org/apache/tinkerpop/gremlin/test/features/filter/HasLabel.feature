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

@StepClassFilter @StepHasLabel
Feature: Step - hasLabel()

  Scenario: g_EX7X_hasLabelXknowsX
    Given the modern graph
    And using the parameter eid7 defined as "e[marko-knows->vadas].id"
    And the traversal of
      """
      g.E(eid7).hasLabel("knows")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |

  Scenario: g_E_hasLabelXknowsX
    Given the modern graph
    And the traversal of
      """
      g.E().hasLabel("knows")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |
      | e[marko-knows->josh] |

  @MultiProperties @MetaProperties
  Scenario: g_E_hasLabelXuses_traversesX
    Given the crew graph
    And the traversal of
      """
      g.E().hasLabel("uses", "traverses")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-uses->gremlin] |
      | e[marko-uses->tinkergraph] |
      | e[stephen-uses->gremlin] |
      | e[stephen-uses->tinkergraph] |
      | e[daniel-uses->gremlin] |
      | e[daniel-uses->tinkergraph] |
      | e[matthias-uses->gremlin] |
      | e[matthias-uses->tinkergraph] |
      | e[gremlin-traverses->tinkergraph] |

  Scenario: g_V_hasLabelXperson_software_blahX
    Given the modern graph
    And the traversal of
    """
    g.V().hasLabel("person","software", "blah")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |
      | v[lop] |
      | v[ripple] |

  Scenario: g_V_hasLabelXperson_softwarevarX
    Given the modern graph
    And using the parameter xx1 defined as "software"
    And the traversal of
    """
    g.V().hasLabel("person",xx1)
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |
      | v[lop] |
      | v[ripple] |

  Scenario: g_V_hasLabelXpersonX_hasLabelXsoftwareX
    Given the modern graph
    And the traversal of
    """
    g.V().hasLabel("person").hasLabel("software")
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasLabelXpersonvarX_hasLabelXsoftwareX
    Given the modern graph
    And using the parameter xx1 defined as "person"
    And the traversal of
    """
    g.V().hasLabel(xx1).hasLabel("software")
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasLabelXpersonvar_softwarevarX
    Given the modern graph
    And using the parameter xx1 defined as "person"
    And using the parameter xx2 defined as "software"
    And the traversal of
    """
    g.V().hasLabel(xx1,xx2)
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |
      | v[lop] |
      | v[ripple] |

  Scenario: g_V_hasLabelXpersonX_hasXage_notXlteX10X_andXnotXbetweenX11_20XXXX_andXltX29X_orXeqX35XXXX_name
    Given the modern graph
    And the traversal of
    """
    g.V().hasLabel("person").has("age", P.not(P.lte(10).and(P.not(P.between(11, 20)))).and(P.lt(29).or(P.eq(35)))).values("name")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | peter |

  Scenario: g_V_hasLabelXnullX
    Given the modern graph
    And the traversal of
    """
    g.V().hasLabel(null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasXlabel_nullX
    Given the modern graph
    And the traversal of
    """
    g.V().has(T.label, null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasLabelXnull_nullX
    Given the modern graph
    And the traversal of
    """
    g.V().hasLabel(null, null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasLabelXnull_personX
    Given the modern graph
    And the traversal of
    """
    g.V().hasLabel(null, "person")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |

  Scenario: g_E_hasLabelXnullX
    Given the modern graph
    And the traversal of
    """
    g.E().hasLabel(null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_E_hasXlabel_nullX
    Given the modern graph
    And the traversal of
    """
    g.E().has(T.label, null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_properties_hasLabelXnullX
    Given the modern graph
    And the traversal of
    """
    g.V().properties().hasLabel(null)
    """
    When iterated to list
    Then the result should be empty