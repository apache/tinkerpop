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
  Scenario: g_V_properties_element
    Given the modern graph
    And using the parameter xx1 defined as "v[josh]"
    And the traversal of
      """
      g.V(xx1).properties().element().limit(1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

  # EdgeProperty -> Edge
  Scenario: g_E_properties_element
    Given the modern graph
    And using the parameter xx1 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.E(xx1).properties().element().limit(1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |

  # MetaProperty -> VertexProperty
  @MultiMetaProperties
  Scenario: g_V_properties_properties_element_element
    Given the crew graph
    And using the parameter xx1 defined as "v[stephen]"
    And the traversal of
      """
      g.V(xx1).properties().properties().element().element().limit(1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[stephen] |