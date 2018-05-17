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

Feature: Step - dedup()

  Scenario: g_V_out_in_valuesXnameX_fold_dedupXlocalX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().out().in().values("name").fold().dedup(Scope.local).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | josh  |
      | peter |

  Scenario: g_V_out_asXxX_in_asXyX_selectXx_yX_byXnameX_fold_dedupXlocal_x_yX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().out().as("x").in().as("y").select("x", "y").by("name").fold().dedup(Scope.local, "x", "y").unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"x":"lop","y":"marko"}] |
      | m[{"x":"lop","y":"josh"}] |
      | m[{"x":"lop","y":"peter"}] |
      | m[{"x":"vadas","y":"marko"}] |
      | m[{"x":"josh","y":"marko"}] |
      | m[{"x":"ripple","y":"josh"}] |

  Scenario: g_V_both_dedup_name
    Given the modern graph
    And the traversal of
      """
      g.V().both().dedup().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | vadas |
      | josh  |
      | peter |
      | marko |
      | ripple |

  Scenario: g_V_both_hasXlabel_softwareX_dedup_byXlangX_name
    Given the modern graph
    And the traversal of
      """
      g.V().both().has(T.label, "software").dedup().by("lang").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |

  Scenario: g_V_both_name_order_byXa_bX_dedup_value
    Given the modern graph
    And using the parameter l defined as "c[a,b -> a.value().compareTo(b.value())]"
    And the traversal of
      """
      g.V().both().properties("name").order().by(l).dedup().value()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |
      | lop  |
      | marko |
      | peter |
      | ripple |
      | vadas  |

  Scenario: g_V_both_both_name_dedup
    Given the modern graph
    And the traversal of
      """
      g.V().both().both().values("name").dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |
      | lop  |
      | marko |
      | peter |
      | ripple |
      | vadas  |

  Scenario: g_V_both_both_dedup
    Given the modern graph
    And the traversal of
      """
      g.V().both().both().dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[lop]  |
      | v[marko] |
      | v[peter] |
      | v[ripple] |
      | v[vadas]  |

  Scenario: g_V_both_both_dedup_byXlabelX
    Given the modern graph
    And the traversal of
      """
      g.V().both().both().dedup().by(T.label)
      """
    When iterated to list
    Then the result should have a count of 2

  Scenario: g_V_group_byXlabelX_byXbothE_weight_dedup_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by(T.label).by(__.bothE().values("weight").dedup().fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"software":[1.0,0.4,0.2],"person":[0.5,1.0,0.4,0.2]}] |

  Scenario: g_V_asXaX_both_asXbX_dedupXa_bX_byXlabelX_selectXa_bX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").both().as("b").dedup("a", "b").by(T.label).select("a", "b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]"}] |
      | m[{"a":"v[marko]","b":"v[vadas]"}] |
      | m[{"a":"v[lop]","b":"v[marko]"}] |

  Scenario: g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_dedupXa_bX_path
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("created").as("b").in("created").as("c").dedup("a", "b").path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],v[lop],v[marko]] |
      | p[v[josh],v[ripple],v[josh]] |
      | p[v[josh],v[lop],v[marko]] |
      | p[v[peter],v[lop],v[marko]] |

  Scenario: g_V_outE_asXeX_inV_asXvX_selectXeX_order_byXweight_ascX_selectXvX_valuesXnameX_dedup
    Given the modern graph
    And the traversal of
      """
      g.V().outE().as("e").inV().as("v").select("e").order().by("weight", Order.asc).select("v").values("name").dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | vadas |
      | josh |
      | ripple |

  Scenario: g_V_both_both_dedup_byXoutE_countX_name
    Given the modern graph
    And the traversal of
      """
      g.V().both().both().dedup().by(__.outE().count()).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | josh |
      | peter |
      | ripple |

  Scenario: g_V_groupCount_selectXvaluesX_unfold_dedup
    Given the modern graph
    And the traversal of
      """
      g.V().groupCount().select(Column.values).unfold().dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |

  Scenario: g_V_asXaX_repeatXbothX_timesX3X_emit_name_asXbX_group_byXselectXaXX_byXselectXbX_dedup_order_foldX_selectXvaluesX_unfold_dedup
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").repeat(__.both()).times(3).emit().values("name").as("b").group().by(__.select("a")).by(__.select("b").dedup().order().fold()).select(Column.values).unfold().dedup()
      """
    When iterated next
    Then the result should be unordered
      | result |
      | josh |
      | lop  |
      | marko |
      | peter |
      | ripple |
      | vadas  |

  Scenario: g_V_repeatXdedupX_timesX2X_count
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.dedup()).times(2).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].l |

