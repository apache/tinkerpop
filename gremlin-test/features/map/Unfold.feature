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

@StepClassMap @StepUnfold
Feature: Step - unfold()

  Scenario: g_V_localXoutE_foldX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().local(__.outE().fold()).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-created->lop] |
      | e[marko-knows->vadas] |
      | e[marko-knows->josh] |
      | e[josh-created->ripple] |
      | e[josh-created->lop] |
      | e[peter-created->lop] |

  Scenario: g_V_valueMap_unfold_mapXkeyX
    Given the modern graph
    And using the parameter l1 defined as "c[it.get().getKey()]"
    And the traversal of
      """
      g.V().valueMap().unfold().map(l1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | name   |
      | age    |
      | name   |
      | age    |
      | name   |
      | lang   |
      | lang   |
      | name   |
      | age    |
      | name   |
      | name   |
      | age    |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_VX1X_repeatXboth_simplePathX_untilXhasIdX6XX_path_byXnameX_unfold
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid6 defined as "v[peter].id"
    And the traversal of
      """
      g.V(vid1).repeat(__.both().simplePath()).until(__.hasId(vid6)).path().by("name").unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | lop  |
      | peter |
      | marko |
      | josh  |
      | lop   |
      | peter |