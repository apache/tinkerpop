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
      | m[{"ripple": 1, "lop": 3}] |

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
      | m[{"ripple": 1, "lop": 3}] |

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
      | m[{"ripple":2, "lop": 4, "josh": 1, "vadas": 1}] |

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
      | d[12] |

  Scenario: g_V_hasXnoX_groupCount
    Given the modern graph
    And the traversal of
      """
      g.V().has("no").groupCount()
      """
    When iterated to list
    Then the result should be ordered
      | m[{}] |

  Scenario: g_V_hasXnoX_groupCountXaX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().has("no").groupCount("a").cap("a")
      """
    When iterated to list
    Then the result should be ordered
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
      | m[{"marko": 2, "java": 2}] |

  Scenario: g_V_outXcreatedX_groupCountXxX_capXxX
    Given an unsupported test
    Then nothing should happen because
      """
      The result returned is not supported under GraphSON 2.x and therefore cannot be properly asserted. More
      specifically it has vertex keys which basically get toString()'d under GraphSON 2.x. This test can be supported
      with GraphSON 3.x.
      """

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