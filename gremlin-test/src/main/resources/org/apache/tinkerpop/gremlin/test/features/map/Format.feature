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

@StepClassMap @StepFormat
Feature: Step - format()

  Scenario: g_VX1X_formatXstrX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", "marko").format("Hello world")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Hello world |

  Scenario: g_V_formatXstrX
    Given the modern graph
    And the traversal of
      """
      g.V().format("%{name} is %{age} years old")
      """
    When iterated to list
    # software don't have age, so filtered out
    Then the result should be unordered
      | result |
      | marko is 29 years old |
      | vadas is 27 years old |
      | josh is 32 years old |
      | peter is 35 years old |

  Scenario: g_V_elementMap_formatXstrX
    Given the modern graph
    And the traversal of
      """
      g.V().elementMap().format("%{name} is %{age} years old")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko is 29 years old |
      | vadas is 27 years old |
      | josh is 32 years old |
      | peter is 35 years old |

  Scenario: g_V_hasLabelXpersonX_asXaX_valuesXnameX_asXp1X_selectXaX_inXknowsX_formatXstrX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").as("a").values("name").as("p1").select("a").in("knows").format("%{p1} knows %{name}")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas knows marko |
      | josh knows marko |

  Scenario:  g_V_asXsX_label_asXsubjectX_selectXsX_outE_asXpX_label_asXpredicateX_selectXpX_inV_label_asXobjectX_formatXstrX
    Given the modern graph
    And the traversal of
      """
      g.V().as("s").label().as("subject").
        select("s").outE().as("p").label().as("predicate").
        select("p").inV().label().as("object").format("%{subject} %{predicate} %{object}")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | person created software |
      | person knows person |
      | person knows person |
      | person created software |
      | person created software |
      | person created software |