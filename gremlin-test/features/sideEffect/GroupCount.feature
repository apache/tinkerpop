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

@StepClassSideEffect @StepGroupCount
Feature: Step - groupCount()

  Scenario: g_V_outXcreatedX_groupCount_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().out("created").groupCount().by("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"ripple":"d[1].l", "lop":"d[3].l"}] |

  Scenario: g_V_groupCount_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().groupCount().by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[27].i":"d[1].l", "d[29].i":"d[1].l", "d[32].i":"d[1].l", "d[35].i":"d[1].l"}] |

  Scenario: g_withStrategiesXProductiveByStrategyX_V_groupCount_byXageX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().groupCount().by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"null":"d[2].l", "d[27].i":"d[1].l", "d[29].i":"d[1].l", "d[32].i":"d[1].l", "d[35].i":"d[1].l"}] |

  Scenario: g_V_outXcreatedX_name_groupCount
    Given the modern graph
    And the traversal of
      """
      g.V().out("created").values("name").groupCount()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"ripple":"d[1].l", "lop":"d[3].l"}] |

  Scenario: g_V_outXcreatedX_groupCountXaX_byXnameX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().out("created").
        groupCount("a").
          by("name").
        cap("a")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"ripple":"d[1].l", "lop":"d[3].l"}] |

  Scenario: g_V_outXcreatedX_name_groupCountXaX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().out("created").values("name").
        groupCount("a").
        cap("a")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"ripple":"d[1].l", "lop":"d[3].l"}] |

  Scenario: g_V_repeatXout_groupCountXaX_byXnameXX_timesX2X_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().
        repeat(__.out().groupCount("a").by("name")).
          times(2).
        cap("a")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"ripple":"d[2].l", "lop":"d[4].l", "josh":"d[1].l", "vadas":"d[1].l"}] |

  Scenario: g_V_both_groupCountXaX_byXlabelX_asXbX_barrier_whereXselectXaX_selectXsoftwareX_isXgtX2XXX_selectXbX_name
    Given the modern graph
    And the traversal of
      """
      g.V().both().
        groupCount("a").
          by(T.label).as("b").
        barrier().
        where(__.select("a").
              select("software").
              is(P.gt(2))).
        select("b").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | lop |
      | lop |
      | peter |
      | marko |
      | marko |
      | marko |
      | ripple |
      | vadas |
      | josh |
      | josh |
      | josh |

  Scenario: g_V_unionXoutXknowsX__outXcreatedX_inXcreatedXX_groupCount_selectXvaluesX_unfold_sum
    Given the modern graph
    And the traversal of
      """
      g.V().union(__.out("knows"),
                  __.out("created").in("created")).
        groupCount().
        select(Column.values).
        unfold().
        sum()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[12].l |

  Scenario: g_V_hasXnoX_groupCount
    Given the modern graph
    And the traversal of
      """
      g.V().has("no").groupCount()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{}] |

  Scenario: g_V_hasXnoX_groupCountXaX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().has("no").groupCount("a").cap("a")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{}] |

  Scenario: g_V_unionXrepeatXoutX_timesX2X_groupCountXmX_byXlangXX__repeatXinX_timesX2X_groupCountXmX_byXnameXX_capXmX
    Given the modern graph
    And the traversal of
      """
      g.V().union(__.repeat(__.out()).times(2).groupCount("m").by("lang"),
                  __.repeat(__.in()).times(2).groupCount("m").by("name")).cap("m")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"marko":"d[2].l", "java":"d[2].l"}] |

  Scenario: g_V_outXcreatedX_groupCountXxX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.V().out("created").groupCount("x").cap("x")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"v[ripple]":"d[1].l","v[lop]":"d[3].l"}] |

  Scenario: g_V_groupCount_byXbothE_countX
    Given the modern graph
    And the traversal of
      """
      g.V().groupCount().by(__.bothE().count())
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"d[1].l":"d[3].l","d[3].l":"d[3].l"}] |

  Scenario: g_V_both_groupCountXaX_out_capXaX_selectXkeysX_unfold_both_groupCountXaX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().both().groupCount("a").out().cap("a").select(Column.keys).unfold().both().groupCount("a").cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"v[marko]":"d[6].l","v[vadas]":"d[2].l","v[lop]":"d[6].l","v[josh]":"d[6].l","v[ripple]":"d[2].l","v[peter]":"d[2].l"}] |

  Scenario: g_V_hasXperson_name_markoX_bothXknowsX_groupCount_byXvaluesXnameX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person", "name", "marko").both("knows").groupCount().by(__.values("name").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"l[josh]":"d[1].l","l[vadas]":"d[1].l"}] |