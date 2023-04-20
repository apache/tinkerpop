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

@StepClassMap @GraphComputerVerificationElementSupported
Feature: Step - element()

  # VertexProperty -> Vertex
  Scenario: g_VX1X_properties_element
    Given the modern graph
    And using the parameter v2 defined as "v[josh]"
    And the traversal of
      """
      g.V(v2).properties().element().limit(1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

  Scenario: g_V_properties_element
    Given the modern graph
    And the traversal of
      """
      g.V().properties().element()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |
      | v[vadas] |
      | v[vadas] |
      | v[lop] |
      | v[lop] |
      | v[josh] |
      | v[josh] |
      | v[ripple] |
      | v[ripple] |
      | v[peter] |
      | v[peter] |

  Scenario: g_V_propertiesXageX_element
    Given the modern graph
    And the traversal of
      """
      g.V().properties("age").element()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |

  # EdgeProperty -> Edge
  Scenario: g_EX_properties_element
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.E(eid11).properties().element().limit(1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |

  Scenario: g_E_properties_element
    Given the modern graph
    And the traversal of
      """
      g.E().properties().element()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-created->lop] |
      | e[marko-knows->josh] |
      | e[marko-knows->vadas] |
      | e[peter-created->lop] |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  # MetaProperty -> VertexProperty
  @MultiMetaProperties
  Scenario: g_VXv7_properties_properties_element_element
    Given the crew graph
    And using the parameter v7 defined as "v[stephen]"
    And the traversal of
      """
      g.V(v7).properties().properties().element().element().limit(1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[stephen] |

  @MultiMetaProperties
  Scenario: g_V_properties_properties_element_element
    Given the crew graph
    And using the parameter v7 defined as "v[stephen]"
    And the traversal of
      """
      g.V(v7).properties().properties().element().element()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[stephen] |
      | v[stephen] |
      | v[stephen] |
      | v[stephen] |
      | v[stephen] |