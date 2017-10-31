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

Feature: Step - store()

  Scenario: g_V_storeXa_nameX_out_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().store("a").by("name").out().cap("a")      
      """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter  |

  Scenario: g_VX1X_storeXaX_byXnameX_out_storeXaX_byXnameX_name_capXaX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).store("a").by("name").out().store("a").by("name").values("name").cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |

  Scenario: g_withSideEffectXa_setX_V_both_name_storeXaX_capXaX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter initial defined as "s[]"
    And the traversal of
      """
      g.withSideEffect("a", initial).V().both().values("name").store("a").cap("a")
      """
    When iterated next
    Then nothing should happen because
      """
      The result returned is not supported under GraphSON 2.x and therefore cannot be properly asserted. More
      specifically it requires specification of a Set as a parameter which only becomes available in
      GraphSON 3.0.
      """

  Scenario: g_V_storeXaX_byXoutEXcreatedX_countX_out_out_storeXaX_byXinEXcreatedX_weight_sumX
    Given the modern graph
    And the traversal of
      """
      g.V().store("a").
             by(__.outE("created").count()).
        out().out().store("a").
                      by(__.inE("created").values("weight").sum()).
        cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | d[1] |
      | d[1] |
      | d[0] |
      | d[0] |
      | d[0] |
      | d[2] |
      | d[1.0] |
      | d[1.0] |