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

@StepClassFilter @StepNone
Feature: Step - none()

  Scenario: g_V_none
    Given the modern graph
    And the traversal of
      """
      g.V().none()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_none_none
    Given the modern graph
    And the traversal of
      """
      g.V().none().none()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_none_fold
    Given the modern graph
    And the traversal of
      """
      g.V().none().fold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[] |

  Scenario: g_V_none_fold_none
    Given the modern graph
    And the traversal of
      """
      g.V().none().fold().none()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_none_fold_constantX1X
    Given the modern graph
    And the traversal of
      """
      g.V().none().fold().constant(1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |

  Scenario: g_V_projectXxX_byXcoalesceXage_isXgtX29XX_noneXX_selectXxX
    Given the modern graph
    And the traversal of
      """
      g.V().project("x").by(__.coalesce(__.values("age").is(P.gt(29)), __.none())).
        select("x")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32].i |
      | d[35].i |