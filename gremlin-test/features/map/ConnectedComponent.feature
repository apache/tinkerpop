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

@StepClassMap @StepConnectedComponent
Feature: Step - connectedComponent()
                
  Scenario: g_V_connectedComponent_hasXcomponentX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().connectedComponent().has("gremlin.connectedComponentVertexProgram.component")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_V_dedup_connectedComponent_hasXcomponentX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().dedup().connectedComponent().has("gremlin.connectedComponentVertexProgram.component")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_V_hasLabelXsoftwareX_connectedComponent_project_byXnameX_byXcomponentX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().hasLabel("software").connectedComponent().project("name","component").by("name").by("gremlin.connectedComponentVertexProgram.component")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": "lop", "component": "1"}] |
      | m[{"name": "ripple", "component": "1"}] |

  Scenario: g_V_connectedComponent_withXEDGES_bothEXknowsXX_withXPROPERTY_NAME_clusterX_project_byXnameX_byXclusterX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().hasLabel("person").connectedComponent().with("~tinkerpop.connectedComponent.edges", __.bothE("knows")).with("~tinkerpop.connectedComponent.propertyName", "cluster").project("name","cluster").by("name").by("cluster")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": "marko", "cluster": "1"}] |
      | m[{"name": "vadas", "cluster": "1"}] |
      | m[{"name": "josh", "cluster": "1"}] |
      | m[{"name": "peter", "cluster": "6"}] |