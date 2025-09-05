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

@StepClassFilter @StepNot
Feature: Step - not()

  Scenario: g_V_notXhasXage_gt_27XX_name
    Given the modern graph
    And the traversal of
      """
      g.V().not(__.has("age", P.gt(27))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | lop |
      | ripple |

  Scenario: g_V_notXnotXhasXage_gt_27XXX_name
    Given the modern graph
    And the traversal of
      """
      g.V().not(__.not(__.has("age", P.gt(27)))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | josh |
      | peter |

  Scenario: g_V_notXhasXname_gt_27XX_name
    Given the modern graph
    And the traversal of
      """
      g.V().not(__.has("name", P.gt(27))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |
