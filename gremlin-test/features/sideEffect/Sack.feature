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

Feature: Step - sack()

  Scenario: g_withSackXhelloX_V_outE_sackXassignX_byXlabelX_inV_sack
    Given the modern graph
    And the traversal of
      """
      g.withSack("hello").V().outE().sack(Operator.assign).by(T.label).inV().sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | created |
      | knows |
      | knows |
      | created |
      | created |
      | created |

  Scenario: g_withSackX0X_V_outE_sackXsumX_byXweightX_inV_sack_sum
    Given the modern graph
    And the traversal of
      """
      g.withSack(0.0).V().outE().sack(Operator.sum).by("weight").inV().sack().sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3.5].d |

  Scenario: g_withSackX0X_V_repeatXoutE_sackXsumX_byXweightX_inVX_timesX2X_sack
    Given the modern graph
    And the traversal of
      """
      g.withSack(0.0).V().repeat(__.outE().sack(Operator.sum).by("weight").inV()).times(2).sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[2.0].d |
      | d[1.4].d |

  Scenario: g_withSackX0X_V_outE_sackXsum_weightX_inV_sack_sum
    Given an unsupported test
    Then nothing should happen because
      """
      This API is deprecated - will not test.
      """

  Scenario: g_withSackX0X_V_repeatXoutE_sackXsum_weightX_inVX_timesX2X_sack
    Given an unsupported test
    Then nothing should happen because
      """
      This API is deprecated - will not test.
      """