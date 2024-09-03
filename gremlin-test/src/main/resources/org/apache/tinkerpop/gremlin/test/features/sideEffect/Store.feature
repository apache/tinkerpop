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

@StepClassSideEffect @StepStore
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
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).store("a").by("name").out().store("a").by("name").values("name").cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |

  @GremlinLangScriptOnly
  Scenario: g_withSideEffectXa_setX_V_both_name_storeXaX_capXaX
    Given the modern graph
    And using the parameter xx1 defined as "s[]"
    And the traversal of
      """
      g.withSideEffect("a", xx1).V().both().values("name").store("a").cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,vadas,lop,josh,ripple,peter] |

  @GremlinLangScriptOnly
  Scenario: g_withSideEffectXa_set_inlineX_V_both_name_storeXaX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.withSideEffect("a", {"alice"}).V().both().values("name").store("a").cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[alice,marko,vadas,lop,josh,ripple,peter] |

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
      | d[1].l |
      | d[1].l |
      | d[0].l |
      | d[0].l |
      | d[0].l |
      | d[2].l |
      | d[1.0].d |
      | d[1.0].d |