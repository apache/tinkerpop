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

Feature: Step - mean()

  Scenario: g_V_age_mean
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").mean()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[30.75].d |

  Scenario: g_V_foo_mean
    Given the modern graph
    And the traversal of
      """
      g.V().values("foo").mean()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_age_fold_meanXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().mean(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[30.75].d |

  Scenario: g_V_foo_fold_meanXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("foo").fold().mean(Scope.local)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_meanX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").group().by("name").by(__.bothE().values("weight").mean())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[1.0].d","lop":"d[0.3333333333333333].d"}] |
