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

Feature: Step - count()

  Scenario: g_V_count
    Given the modern graph
    And the traversal of
      """
      g.V().count()
      """
    When iterated to list
    Then the result should be ordered
      | d[6] |

  Scenario: g_V_out_count
    Given the modern graph
    And the traversal of
      """
      g.V().out().count()
      """
    When iterated to list
    Then the result should be ordered
      | d[6] |

  Scenario: g_V_both_both_count
    Given the modern graph
    And the traversal of
      """
      g.V().both().both().count()
      """
    When iterated to list
    Then the result should be ordered
      | d[30] |

  Scenario: g_V_fold_countXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().count(Scope.local)
      """
    When iterated to list
    Then the result should be ordered
      | d[6] |

  Scenario: g_V_hasXnoX_count
    Given the modern graph
    And the traversal of
      """
      g.V().has("no").count()
      """
    When iterated to list
    Then the result should be ordered
      | d[0] |

  Scenario: g_V_whereXinXkknowsX_outXcreatedX_count_is_0XX_name
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.in("knows").out("created").count().is(0)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | marko |
      | lop  |
      | ripple |
      | peter |

  Scenario: g_V_repeatXoutX_timesX8X_count
    Given the grateful graph
    And the traversal of
      """
      g.V().repeat(__.out()).times(8).count()
      """
    When iterated to list
    Then the result should be ordered
      | d[2505037961767380] |

  Scenario: g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count
    Given the grateful graph
    And the traversal of
      """
      g.V().
        repeat(__.out()).
          times(5).as("a").
        out("writtenBy").as("b").
        select("a", "b").
        count()
      """
    When iterated to list
    Then the result should be ordered
      | d[24309134024] |

  Scenario: g_V_repeatXoutX_timesX3X_count
    Given the grateful graph
    And the traversal of
      """
      g.V().repeat(__.out()).times(3).count()
      """
    When iterated to list
    Then the result should be ordered
      | d[14465066] |
