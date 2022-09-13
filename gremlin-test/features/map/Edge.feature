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

@StepClassMap @StepE
Feature: Step - E()

  Scenario: g_EX11X_E
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.E(eid11).E()
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

 Scenario: g_injectX1X_coalesceXEX_hasLabelXtestsX_addEXtestsX_from_V_hasXnameX_XjoshXX_toXV_hasXnameX_XvadasXXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "josh").
        addV("person").property("name", "vadas")
      """
    And the traversal of
      """
      g.inject(1).coalesce(E().hasLabel("tests"), addE("tests").from(V().has("name","josh")).to(V().has("name","vadas")))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E().hasLabel(\"tests\")"
