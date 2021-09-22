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

@StepClassSideEffect @StepCap
Feature: Step - cap()

  Scenario: g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age").groupCount("a").by("name").out().cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"peter":"d[1].l", "vadas":"d[1].l", "josh":"d[1].l", "marko": "d[1].l"}] |

  Scenario: g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX
    Given an unsupported test
    Then nothing should happen because
      """
      The result is [a:[32:1,35:1,27:1,29:1],b:[ripple:1,lop:1]] which is not supported by this test suite
      """