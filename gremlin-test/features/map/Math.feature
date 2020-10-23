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

Feature: Step - math()

  Scenario: g_V_outE_mathX0_minus_itX_byXweightX
    Given the modern graph
    And the traversal of
      """
      g.V().outE().math("0-_").by("weight")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[-0.4].d |
      | d[-0.4].d |
      | d[-0.5].d |
      | d[-1.0].d |
      | d[-1.0].d |
      | d[-0.2].d |

  Scenario: g_V_hasXageX_valueMap_mathXit_plus_itXbyXselectXageX_unfoldXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age").valueMap().math("_+_").by(__.select("age").unfold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[64.0].d |
      | d[58.0].d |
      | d[54.0].d |
      | d[70.0].d |

  Scenario: g_V_asXaX_outXknowsX_asXbX_mathXa_plus_bX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("knows").as("b").math("a + b").by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[56.0].d |
      | d[61.0].d |

  Scenario: g_withSideEffectXx_100X_V_age_mathX__plus_xX
    Given the modern graph
    And using the parameter x defined as "d[100].i"
    And the traversal of
      """
      g.withSideEffect("x", x).V().values("age").math("_ + x")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[129.0].d |
      | d[127.0].d |
      | d[132.0].d |
      | d[135.0].d |

  Scenario: g_V_asXaX_outXcreatedX_asXbX_mathXb_plus_aX_byXinXcreatedX_countX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("created").as("b").math("b + a").by(__.in("created").count()).by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32.0].d |
      | d[33.0].d |
      | d[35.0].d |
      | d[38.0].d |

  Scenario: g_withSackX1X_injectX1X_repeatXsackXsumX_byXconstantX1XXX_timesX5X_emit_mathXsin__X_byXsackX
    Given the modern graph
    And the traversal of
      """
      g.withSack(1).inject(1).
        repeat(__.sack(Operator.sum).by(__.constant(1))).
          times(5).
          emit().
        math("sin _").by(__.sack())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.9092974268256817].d |
      | d[0.1411200080598672].d |
      | d[-0.7568024953079282].d |
      | d[-0.9589242746631385].d |
      | d[-0.27941549819892586].d |

  Scenario: g_V_projectXa_b_cX_byXbothE_weight_sumX_byXbothE_countX_byXnameX_order_byXmathXa_div_bX_descX_selectXcX
    Given the modern graph
    And the traversal of
      """
      g.V().
        project("a", "b", "c").
          by(__.bothE().values("weight").sum()).
          by(__.bothE().count()).
          by("name").
        order().by(__.math("a / b"), Order.desc).
        select("c")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | ripple |
      | josh |
      | marko |
      | vadas |
      | lop   |
      | peter |
