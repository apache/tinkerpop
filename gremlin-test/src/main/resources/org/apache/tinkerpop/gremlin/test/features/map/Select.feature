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

@StepClassMap @StepSelect
Feature: Step - select()

  Scenario: g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").out("knows").as("b").select("a", "b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "v[marko]", "b": "v[vadas]"}] |
      | m[{"a": "v[marko]", "b": "v[josh]"}] |

  Scenario: g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX_byXnameX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").out("knows").as("b").
        select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "marko", "b": "vadas"}] |
      | m[{"a": "marko", "b": "josh"}] |

  Scenario: g_VX1X_asXaX_outXknowsX_asXbX_selectXaX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").out("knows").as("b").select("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |

  # ProductiveBy
  @GraphComputerVerificationReferenceOnly
  Scenario: g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").out("knows").as("b").
        select("a").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | marko |

  Scenario: g_V_asXaX_out_asXbX_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").
        select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "marko", "b": "lop"}] |
      | m[{"a": "marko", "b": "vadas"}] |
      | m[{"a": "marko", "b": "josh"}] |
      | m[{"a": "josh", "b": "ripple"}] |
      | m[{"a": "josh", "b": "lop"}] |
      | m[{"a": "peter", "b": "lop"}] |

  Scenario: g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().aggregate("x").as("b").
        select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "marko", "b": "lop"}] |
      | m[{"a": "marko", "b": "vadas"}] |
      | m[{"a": "marko", "b": "josh"}] |
      | m[{"a": "josh", "b": "ripple"}] |
      | m[{"a": "josh", "b": "lop"}] |
      | m[{"a": "peter", "b": "lop"}] |

  Scenario: g_V_asXaX_name_order_asXbX_selectXa_bX_byXnameX_by_XitX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").
        values("name").
        order().as("b").
        select("a", "b").
          by("name").by()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "marko", "b": "marko"}] |
      | m[{"a": "vadas", "b": "vadas"}] |
      | m[{"a": "josh", "b": "josh"}] |
      | m[{"a": "ripple", "b": "ripple"}] |
      | m[{"a": "lop", "b": "lop"}] |
      | m[{"a": "peter", "b": "peter"}] |

  @MultiProperties @MetaProperties
  Scenario: g_V_hasXname_gremlinX_inEXusesX_order_byXskill_ascX_asXaX_outV_asXbX_selectXa_bX_byXskillX_byXnameX
    Given the crew graph
    And the traversal of
      """
      g.V().has("name", "gremlin").
        inE("uses").
        order().by("skill", Order.asc).as("a").
        outV().as("b").
        select("a", "b").
          by("skill").
          by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"d[3].i", "b": "matthias"}] |
      | m[{"a":"d[4].i", "b": "marko"}] |
      | m[{"a":"d[5].i", "b": "stephen"}] |
      | m[{"a":"d[5].i", "b": "daniel"}] |

  Scenario: g_V_hasXname_isXmarkoXX_asXaX_selectXaX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", __.is("marko")).as("a").select("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_V_label_groupCount_asXxX_selectXxX
    Given the modern graph
    And the traversal of
      """
      g.V().label().groupCount().as("x").select("x")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"software":"d[2].l", "person":"d[4].l"}] |

  Scenario: g_V_hasLabelXpersonX_asXpX_mapXbothE_label_groupCountX_asXrX_selectXp_rX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").as("p").
        map(__.bothE().label().groupCount()).as("r").
        select("p", "r")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"p": "v[marko]", "r": {"created": "d[1].l", "knows": "d[2].l"}}] |
      | m[{"p": "v[vadas]", "r": {"knows": "d[1].l"}}] |
      | m[{"p": "v[josh]", "r": {"created": "d[2].l", "knows": "d[1].l"}}] |
      | m[{"p": "v[peter]", "r": {"created": "d[1].l"}}] |

  Scenario: g_V_chooseXoutE_count_isX0X__asXaX__asXbXX_chooseXselectXaX__selectXaX__selectXbXX
    Given the modern graph
    And using the parameter xx1 defined as "d[0].l"
    And the traversal of
      """
      g.V().choose(__.outE().count().is(xx1),
                   __.as("a"),
                   __.as("b")).
            choose(__.select("a"),
                   __.select("a"),
                   __.select("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_VX1X_groupXaX_byXconstantXaXX_byXnameX_selectXaX_selectXaX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).group("a").
                  by(__.constant("a")).
                  by(__.values("name")).
        barrier().
        select("a").select("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: g_VX1X_asXhereX_out_selectXhereX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("here").out().select("here")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |
      | v[marko] |

  Scenario: g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).as("here").out().select("here")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[josh] |

  Scenario: g_VX4X_out_asXhereX_hasXlang_javaX_selectXhereX_name
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).out().as("here").
        has("lang", "java").
        select("here").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop |

  Scenario: g_VX1X_outE_asXhereX_inV_hasXname_vadasX_selectXhereX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE().as("here").
        inV().has("name", "vadas").
        select("here")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |

  Scenario: g_VX1X_outEXknowsX_hasXweight_1X_asXhereX_inV_hasXname_joshX_selectXhereX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("knows").
        has("weight", 1.0).as("here").
        inV().has("name", "josh").
        select("here")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->josh] |

  Scenario: g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_asXfakeX_inV_hasXname_joshX_selectXhereX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("knows").as("here").
        has("weight", 1.0).as("fake").
        inV().has("name", "josh").
        select("here")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->josh] |

  Scenario: g_V_asXhereXout_name_selectXhereX
    Given the modern graph
    And the traversal of
      """
      g.V().as("here").
        out().values("name").
        select("here")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |
      | v[marko] |
      | v[josh] |
      | v[josh] |
      | v[peter] |

  Scenario: g_V_outXcreatedX_unionXasXprojectX_inXcreatedX_hasXname_markoX_selectXprojectX__asXprojectX_inXcreatedX_inXknowsX_hasXname_markoX_selectXprojectXX_groupCount_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().out("created").
        union(__.as("project").in("created").has("name", "marko").select("project"),
              __.as("project").in("created").in("knows").has("name", "marko").select("project")).
        groupCount().
          by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[1].l", "lop":"d[6].l"}] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_untilXout_outX_repeatXin_asXaXX_selectXaX_byXtailXlocalX_nameX
    Given the modern graph
    And the traversal of
      """
      g.V().until(__.out().out()).repeat(__.in().as("a")).select("a").by(__.tail(Scope.local).values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | marko |
      | marko |
      | marko |
      | marko |

  Scenario: g_V_outE_weight_groupCount_selectXkeysX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().outE().values("weight").groupCount().select(Column.keys).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.5].d |
      | d[1.0].d |
      | d[0.4].d |
      | d[0.2].d |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_hasLabelXsoftwareX_asXnameX_asXlanguageX_asXcreatorsX_selectXname_language_creatorsX_byXnameX_byXlangX_byXinXcreatedX_name_fold_orderXlocalXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").as("name").as("language").as("creators").select("name", "language", "creators").by("name").by("lang").
                    by(__.in("created").values("name").fold().order(Scope.local))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name":"lop","language":"java","creators":["josh","marko","peter"]}] |
      | m[{"name":"ripple","language":"java","creators":["josh"]}] |

  Scenario: g_V_outE_weight_groupCount_unfold_selectXkeysX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().outE().values("weight").groupCount().unfold().select(Column.keys).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.5].d |
      | d[1.0].d |
      | d[0.4].d |
      | d[0.2].d |

  Scenario: g_V_outE_weight_groupCount_unfold_selectXvaluesX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().outE().values("weight").groupCount().unfold().select(Column.values).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |
      | d[2].l |
      | d[2].l |
      | d[1].l |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_untilXout_outX_repeatXin_asXaX_in_asXbXX_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().until(__.out().out()).repeat(__.in().as("a").in().as("b")).select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"josh","b":"marko"}] |
      | m[{"a":"josh","b":"marko"}] |

  Scenario: g_V_outE_weight_groupCount_selectXvaluesX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().outE().values("weight").groupCount().select(Column.values).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |
      | d[2].l |
      | d[2].l |
      | d[1].l |

  Scenario: g_V_asXaX_whereXoutXknowsXX_selectXaX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").where(__.out("knows")).select("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_VX1X_asXaX_repeatXout_asXaXX_timesX2X_selectXfirst_aX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").repeat(__.out().as("a")).times(2).select(Pop.first, "a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_outXknowsX_asXbX_localXselectXa_bX_byXnameXX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("knows").as("b").local(__.select("a", "b").by("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"vadas"}] |
      | m[{"a":"marko","b":"josh"}] |

  Scenario: g_VX1X_asXaX_repeatXout_asXaXX_timesX2X_selectXlast_aX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").repeat(__.out().as("a")).times(2).select(Pop.last, "a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[ripple] |
      | v[lop] |

  Scenario: g_VX1X_outEXknowsX_asXhereX_hasXweight_1X_inV_hasXname_joshX_selectXhereX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("knows").as("here").has("weight", 1.0).inV().has("name", "josh").select("here")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->josh] |

  Scenario: g_V_asXaX_hasXname_markoX_asXbX_asXcX_selectXa_b_cX_by_byXnameX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").has("name", "marko").as("b").as("c").select("a", "b", "c").by().by("name").by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"marko","c":"d[29].i"}] |

  Scenario: g_V_outE_weight_groupCount_selectXvaluesX_unfold_groupCount_selectXvaluesX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().outE().values("weight").groupCount().select(Column.values).unfold().groupCount().select(Column.values).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[2].l |
      | d[2].l |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_groupXmX_by_byXbothE_countX_barrier_selectXmX_selectXselectXaXX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").group("m").by().by(__.bothE().count()).barrier().select("m").select(__.select("a"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l |
      | d[1].l |
      | d[3].l |
      | d[3].l |
      | d[1].l |
      | d[1].l |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_groupXmX_by_byXbothE_countX_barrier_selectXmX_selectXselectXaXX_byXmathX_plus_XX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").group("m").by().by(__.bothE().count()).barrier().select("m").select(__.select("a")).by(__.math("_+_"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].d |
      | d[2].d |
      | d[6].d |
      | d[6].d |
      | d[2].d |
      | d[2].d |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_outXknowsX_asXaX_selectXall_constantXaXX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("knows").as("a").select(Pop.all, __.constant("a"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[v[marko],v[vadas]] |
      | l[v[marko],v[josh]] |

  Scenario: g_V_selectXaX
    Given the modern graph
    And the traversal of
      """
      g.V().select("a")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_selectXaX_count
    Given the modern graph
    And the traversal of
      """
      g.V().select("a").count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].l |

  Scenario: g_V_selectXa_bX
    Given the modern graph
    And the traversal of
      """
      g.V().select("a","b")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valueMap_selectXaX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().select("a")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valueMap_selectXa_bX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().select("a","b")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_selectXfirst_aX
    Given the modern graph
    And the traversal of
      """
      g.V().select(Pop.first, "a")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_selectXfirst_a_bX
    Given the modern graph
    And the traversal of
      """
      g.V().select(Pop.first, "a","b")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valueMap_selectXfirst_aX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().select(Pop.first, "a")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valueMap_selectXfirst_a_bX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().select(Pop.first, "a","b")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_selectXlast_aX
    Given the modern graph
    And the traversal of
      """
      g.V().select(Pop.last, "a")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_selectXlast_a_bX
    Given the modern graph
    And the traversal of
      """
      g.V().select(Pop.last, "a","b")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valueMap_selectXlast_aX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().select(Pop.last, "a")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valueMap_selectXlast_a_bX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().select(Pop.last, "a","b")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_selectXall_aX
    Given the modern graph
    And the traversal of
      """
      g.V().select(Pop.all, "a")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_selectXall_a_bX
    Given the modern graph
    And the traversal of
      """
      g.V().select(Pop.all, "a","b")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valueMap_selectXall_aX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().select(Pop.all, "a")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valueMap_selectXall_a_bX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().select(Pop.all, "a","b")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_asXa_bX_out_asXcX_path_selectXkeysX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a", "b").out().as("c").path().select(Column.keys)
      """
    When iterated next
    Then the result should be ordered
      | result |
      | s[a,b] |
      | s[c]   |
    And the graph should return 6 for count of "g.V().as(\"a\", \"b\").out().as(\"c\").path().select(Column.keys)"

  Scenario: g_V_hasXperson_name_markoX_barrier_asXaX_outXknows_selectXaX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name","marko").barrier().as("a").out("knows").select("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |

  Scenario: g_V_hasXperson_name_markoX_elementMapXnameX_asXaX_unionXidentity_identityX_selectXaX_selectXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name","marko").elementMap("name").as("a").union(__.identity(),__.identity()).select("a").select("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | marko |

  Scenario: g_V_hasXperson_name_markoX_count_asXaX_unionXidentity_identityX_selectXaX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name","marko").count().as("a").union(__.identity(),__.identity()).select("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |
      | d[1].l |

  Scenario: g_V_hasXperson_name_markoX_path_asXaX_unionXidentity_identityX_selectXaX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name","marko").path().as("a").union(__.identity(),__.identity()).select("a").unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |

  # ProductiveBy
  @GraphComputerVerificationReferenceOnly
  Scenario: g_EX11X_propertiesXweightX_asXaX_selectXaX_byXkeyX
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.E(eid11).properties("weight").as("a").select("a").by(T.key)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | weight |

  # ProductiveBy
  @GraphComputerVerificationReferenceOnly
  Scenario: g_EX11X_propertiesXweightX_asXaX_selectXaX_byXvalueX
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.E(eid11).properties("weight").as("a").select("a").by(T.value)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.4].d |

  Scenario: g_V_asXaX_selectXaX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").select("a").by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_asXa_nX_selectXa_nX_byXageX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a","n").select("a","n").by("age").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"d[29].i","n":"marko"}] |
      | m[{"a":"d[27].i","n":"vadas"}] |
      | m[{"a":"d[32].i","n":"josh"}] |
      | m[{"a":"d[35].i","n":"peter"}] |

  @GraphComputerVerificationReferenceOnly @WithProductiveByStrategy
  Scenario: g_withStrategiesXProductiveByStrategyX_V_asXaX_selectXaX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().as("a").select("a").by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | null |
      | d[32].i |
      | null |
      | d[35].i |

  Scenario: g_withSideEffectXk_nullX_injectXxX_selectXkX
    Given the empty graph
    And the traversal of
      """
      g.withSideEffect("k",null).inject("x").select("k")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  Scenario: g_V_out_in_selectXall_a_a_aX_byXunfold_name_foldX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("A").property("name", "a1").as("a1").
        addV("B").property("name", "b1").as("b1").
        addE("ab").from("a1").to("b1")
      """
    And the traversal of
      """
      g.V().as("a").out().as("a").in().as("a").
        select(Pop.all, "a", "a", "a").
          by(unfold().values('name').fold())
      """
    When iterated to list
    Then the result should be of
      | result |
      | m[{"a":["a1","b1","a1"]}] |
      | m[{"a":["a1","a1","b1"]}] |
      | m[{"a":["b1","a1","a1"]}] |

  @StepClassIntegrated
  Scenario: g_withoutStrategiesXLazyBarrierStrategyX_V_asXlabelX_aggregateXlocal_xX_selectXxX_selectXlabelX
    Given the modern graph
    And the traversal of
      """
      g.withoutStrategies(LazyBarrierStrategy).V().as("label").aggregate(local,"x").select("x").select("label")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_V_name_asXaX_selectXfirst_aX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").as("a").select(Pop.first, "a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |

  Scenario: g_V_name_asXaX_selectXlast_aX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").as("a").select(Pop.last, "a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |

  Scenario: g_V_name_asXaX_selectXmixed_aX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").as("a").select(Pop.mixed, "a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |

  Scenario: g_V_name_asXaX_selectXall_aX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").as("a").select(Pop.all, "a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko] |
      | l[vadas] |
      | l[lop] |
      | l[josh] |
      | l[ripple] |
      | l[peter] |

  Scenario: g_V_hasLabelXpersonX_name_asXaX_concatXXX_asXaX_length_asXaX_selectXaX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").as("a").concat("X").as("a").length().as("a").
        select("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].i |
      | d[6].i |
      | d[5].i |
      | d[6].i |

  Scenario: g_V_hasLabelXpersonX_name_asXaX_concatXXX_asXaX_length_asXaX_selectXfirst_aX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").as("a").concat("X").as("a").length().as("a").
        select(Pop.first, "a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
      | peter |

  Scenario: g_V_hasLabelXpersonX_name_asXaX_concatXXX_asXaX_length_asXaX_selectXlast_aX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").as("a").concat("X").as("a").length().as("a").
        select(Pop.last, "a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].i |
      | d[6].i |
      | d[5].i |
      | d[6].i |

  Scenario: g_V_hasLabelXpersonX_name_asXaX_concatXXX_asXaX_concatXYZX_asXaX_selectXmixed_aX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").as("a").concat("X").as("a").concat("YZ").as("a").
        select(Pop.mixed, "a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko,markoX,markoXYZ] |
      | l[vadas,vadasX,vadasXYZ] |
      | l[josh,joshX,joshXYZ] |
      | l[peter,peterX,peterXYZ] |

  Scenario: g_V_hasLabelXpersonX_name_asXaX_concatXXX_asXaX_concatXYZX_asXaX_selectXall_aX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").as("a").concat("X").as("a").concat("YZ").as("a").
        select(Pop.all, "a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko,markoX,markoXYZ] |
      | l[vadas,vadasX,vadasXYZ] |
      | l[josh,joshX,joshXYZ] |
      | l[peter,peterX,peterXYZ] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXmixed_aX_byXunfold_valuesXnameX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select(Pop.mixed, "a").by(__.unfold().values("name").fold())
      """
    When iterated to list
    Then the result should be of
      | result |
      | l[marko,josh,ripple] |
      | l[marko,ripple,josh] |
      | l[josh,marko,ripple] |
      | l[josh,ripple,marko] |
      | l[ripple,marko,josh] |
      | l[ripple,josh,marko] |
      | l[marko,josh,lop] |
      | l[marko,lop,josh] |
      | l[josh,marko,lop] |
      | l[josh,lop,marko] |
      | l[lop,marko,josh] |
      | l[lop,josh,marko] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_out_asXaX_out_asXaX_selectXall_aX_byXunfold_valuesXnameX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("a").out().as("a").select(Pop.all, "a").by(__.unfold().values("name").fold())
      """
    When iterated to list
    Then the result should be of
      | result |
      | l[marko,josh,ripple] |
      | l[marko,ripple,josh] |
      | l[josh,marko,ripple] |
      | l[josh,ripple,marko] |
      | l[ripple,marko,josh] |
      | l[ripple,josh,marko] |
      | l[marko,josh,lop] |
      | l[marko,lop,josh] |
      | l[josh,marko,lop] |
      | l[josh,lop,marko] |
      | l[lop,marko,josh] |
      | l[lop,josh,marko] |