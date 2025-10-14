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

@StepClassFilter @StepTail
Feature: Step - tail()

  Scenario: g_V_valuesXnameX_order_tailXglobal_2X
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").order().tail(Scope.global, 2)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | ripple |
      | vadas |

  Scenario: g_V_valuesXnameX_order_tailX2X
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").order().tail(2)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | ripple |
      | vadas |

  Scenario: g_V_valuesXnameX_order_tailX2varX
    Given the modern graph
    And using the parameter xx1 defined as "d[2].l"
    And the traversal of
      """
      g.V().values("name").order().tail(xx1)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | ripple |
      | vadas |

  Scenario: g_V_valuesXnameX_order_tail
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").order().tail()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | vadas |

  Scenario: g_V_valuesXnameX_order_tailX7X
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").order().tail(7)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | josh   |
      | lop    |
      | marko  |
      | peter  |
      | ripple |
      | vadas  |

  Scenario: g_V_repeatXbothX_timesX3X_tailX7X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(both()).times(3).tail(7)
      """
    When iterated to list
    Then the result should have a count of 7

  Scenario: g_V_repeatXin_outX_timesX3X_tailX7X_count
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.in().out()).times(3).tail(7).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[7].l |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_1X_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select("a").by(__.unfold().values("name").fold()).tail(Scope.local, 1).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocalX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select("a").by(__.unfold().values("name").fold()).tail(Scope.local).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop |

  Scenario: g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_2X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").out().as("c").select("a","b","c").by("name").tail(Scope.local, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"b":"josh","c":"ripple"}] |
      | m[{"b":"josh","c":"lop"}] |

  Scenario: g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_2varX
    Given the modern graph
    And using the parameter xx1 defined as "d[2].l"
    And the traversal of
      """
      g.V().as("a").out().as("b").out().as("c").select("a","b","c").by("name").tail(Scope.local, xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"b":"josh","c":"ripple"}] |
      | m[{"b":"josh","c":"lop"}] |

  Scenario: g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_tailXlocal_1X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").out().as("c").select("a","b","c").by("name").tail(Scope.local, 1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"c":"ripple"}] |
      | m[{"c":"lop"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_tailXlocal_1X_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select(Pop.mixed, "a").by(__.unfold().values("name").fold()).tail(Scope.local, 1).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_tailXlocalX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select(Pop.mixed, "a").by(__.unfold().values("name").fold()).tail(Scope.local).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXlimitXlocal_0XX_tailXlocal_1X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select(Pop.mixed, "a").by(__.limit(Scope.local, 0)).tail(Scope.local, 1)
      """
    When iterated next
    Then the result should be empty

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_tailXlocal_2X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select(Pop.mixed, "a").by(__.unfold().values("name").fold()).tail(Scope.local, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[josh,ripple] |
      | l[josh,lop] |

  Scenario: g_VX1X_valuesXageX_tailXlocal_5X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).values("age").tail(Scope.local, 50)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |

  # Test consistent collection output for tail(local) - single element should return collection
  Scenario: g_injectXlistX1_2_3XX_tailXlocal_1X
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3]).tail(Scope.local, 1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[3].i] |

  # Test that Map local tail 1 - should still return single entry
  Scenario: g_VX1X_valueMapXnameX_tailXlocal_1X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).valueMap("name").tail(Scope.local, 1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name":["marko"]}] |

  # Test unfold() can be used to extract single elements from collections for tail
  Scenario: g_injectX1_2_3X_tailXlocal_1X_unfold
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3]).tail(Scope.local, 1).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].i |

  # Test tail(local) with multiple collections
  Scenario: g_injectX1_2_3_4_5_6X_tailXlocal_1X
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3], [4, 5, 6]).tail(Scope.local, 1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[3].i] |
      | l[d[6].i] |

  # Test tail(local) with multiple elements from collections
  Scenario: g_injectX1_2_3_4_5X_tailXlocal_2X
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3, 4, 5]).tail(Scope.local, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[4].i,d[5].i] |

  # Test tail works in repeat with a single loop
  Scenario: g_VX3X_repeatXout_order_byXperformancesX_tailX2XX_timesX1X_valuesXnameX
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(__.out().order().by("performances").tail(2)).times(1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ME AND MY UNCLE |
      | DRUMS |

  # Test tail runs per iteration in repeat with multiple iterations
  @GraphComputerVerificationOrderingNotSupported
  Scenario: g_VX3X_repeatXout_order_byXperformancesX_tailX2XX_timesX2X_valuesXnameX
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(__.out().order().by("performances").tail(2)).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | THE OTHER ONE |
      | SUGAR MAGNOLIA |

  # Test object-local tail works in repeat with a single loop
  Scenario: g_VX2X_repeatXout_localXorder_byXperformancesX_tailX1XXX_timesX1X_valuesXnameX
    Given the grateful graph
    And using the parameter vid2 defined as "v[IM A MAN].id"
    And the traversal of
      """
      g.V(vid2).repeat(__.out().local(order().by("performances").tail(1))).times(1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | JAM |
      | JACK STRAW |

  # Test object-local tail runs per iteration in repeat with multiple iterations
  Scenario: g_VX250X_repeatXout_localXorder_byXperformancesX_tailX1XXX_timesX2X_valuesXnameX
    Given the grateful graph
    And using the parameter vid250 defined as "v[SIMPLE TWIST OF FATE].id"
    And the traversal of
      """
      g.V(vid250).repeat(__.out().local(__.order().by("performances").tail(1))).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | STUCK INSIDE OF MOBILE |
      | STUCK INSIDE OF MOBILE |
      | WICKED MESSENGER |
      | TANGLED UP IN BLUE |
      | SHELTER FROM THE STORM |
      | RAINY DAY WOMAN |
      | CUMBERLAND BLUES |
      | WHEN PUSH COMES TO SHOVE |
      | JOHN BROWN |
      | SIMPLE TWIST OF FATE |
      | BABY BLUE |

  # Test tail inside repeat can be followed by other range-based steps
  Scenario: g_VX3X_repeatXout_order_byXperformancesX_tailX3X_limitX1XX_timesX2X_valuesXnameX
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(__.out().order().by("performances").tail(3).limit(1)).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | THE OTHER ONE |

  # Test tail inside repeat can be preceded by other range-based steps
  Scenario: g_VX3X_repeatXout_order_byXperformancesX_limitX5X_tailX1XX_timesX2X_valuesXnameX
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(__.out().order().by("performances").limit(5).tail(1)).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | LET THE GOOD TIMES ROLL |

  # Test order on edge weight with tail in repeat leads to ordered walk
  Scenario: g_VX3X_repeatXoutE_order_byXweightX_tailX2X_inVX_timesX2X_valuesXnameX
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(outE().order().by("weight").tail(2).inV()).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | SUGAR MAGNOLIA |
      | AROUND AND AROUND |
