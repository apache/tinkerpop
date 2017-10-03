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

Feature: Step - groupCount()

  Scenario: g_V_outXcreatedX_groupCount_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().out("created").groupCount().by("name")
      """
    When iterated to list
    Then the result should be ordered
      | m[{"ripple": 1, "lop": 3}] |

  Scenario: g_V_outXcreatedX_name_groupCount
    Given the modern graph
    And the traversal of
      """
      g.V().out("created").values("name").groupCount()
      """
    When iterated to list
    Then the result should be ordered
      | m[{"ripple": 1, "lop": 3}] |

  Scenario: g_V_groupCount_byXbothE_countX
    Given an unsupported test
    Then nothing should happen because
      """
      The result returned is not supported under GraphSON 2.x and therefore cannot be properly asserted. More
      specifically it has vertex keys which basically get toString()'d under GraphSON 2.x. This test can be supported
      with GraphSON 3.x.
      """

  Scenario: g_V_both_groupCountXaX_out_capXaX_selectXkeysX_unfold_both_groupCountXaX_capXaX
    Given an unsupported test
    Then nothing should happen because
      """
      The result returned is not supported under GraphSON 2.x and therefore cannot be properly asserted. More
      specifically it has vertex keys which basically get toString()'d under GraphSON 2.x. This test can be supported
      with GraphSON 3.x.
      """