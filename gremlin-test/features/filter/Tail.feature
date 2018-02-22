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
      g.V().repeat(__.both()).times(3).tail(7)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[peter] |
      | v[peter] |
      | v[peter] |
      | v[peter] |
      | v[marko] |
      | v[marko] |
      | v[marko] |

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

  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocal_1X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select("a").by(__.unfold().values("name").fold()).tail(Scope.local, 1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop |

  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXaX_byXunfold_valuesXnameX_foldX_tailXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select("a").by(__.unfold().values("name").fold()).tail(Scope.local)
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

  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_tailXlocal_1X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select(Pop.mixed, "a").by(__.unfold().values("name").fold()).tail(Scope.local, 1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop |

  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX_tailXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select(Pop.mixed, "a").by(__.unfold().values("name").fold()).tail(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop |

  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXlimitXlocal_0XX_tailXlocal_1X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select(Pop.mixed, "a").by(__.limit(Scope.local, 0)).tail(Scope.local, 1)
      """
    When iterated to list
    Then the result should be empty

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