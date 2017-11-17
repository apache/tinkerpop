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

Feature: Step - min()

  Scenario: g_V_age_min
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |

  Scenario: g_V_repeatXbothX_timesX5X_age_min
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.both()).times(5).values("age").min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |

  Scenario: g_V_hasLabelXsoftwareX_group_byXnameX_byXbothE_weight_minX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").group().by("name").by(__.bothE().values("weight").min())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[1.0].d","lop":"d[0.2].d"}] |
