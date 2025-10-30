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

@StepClassSideEffect @StepGroup
Feature: Step - group()

  Scenario: g_V_group_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"l[v[ripple]]", "peter":"l[v[peter]]", "vadas":"l[v[vadas]]", "josh": "l[v[josh]]", "lop":"l[v[lop]]", "marko":"l[v[marko]]"}] |

  Scenario: g_V_group_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[35].i":"l[v[peter]]", "d[27].i":"l[v[vadas]]", "d[32].i": "l[v[josh]]", "d[29].i":"l[v[marko]]"}] |

  @WithProductiveByStrategy
  Scenario: g_withStrategiesXProductiveByStrategyX_V_group_byXageX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().group().by("age")
      """
    When iterated to list
    Then the result should be of
      | result |
      | m[{"null":"l[v[lop],v[ripple]]", "d[35].i":"l[v[peter]]", "d[27].i":"l[v[vadas]]", "d[32].i": "l[v[josh]]", "d[29].i":"l[v[marko]]"}] |
      | m[{"null":"l[v[ripple],v[lop]]", "d[35].i":"l[v[peter]]", "d[27].i":"l[v[vadas]]", "d[32].i": "l[v[josh]]", "d[29].i":"l[v[marko]]"}] |
    And the result should have a count of 1

  Scenario: g_V_group_byXnameX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by("name").by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":[], "peter":"l[d[35].i]", "vadas":"l[d[27].i]", "josh": "l[d[32].i]", "lop":[], "marko":"l[d[29].i]"}] |

  Scenario: g_V_group_byXnameX_by
    Given the modern graph
    And the traversal of
      """
      g.V().group().by("name").by()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"l[v[ripple]]", "peter":"l[v[peter]]", "vadas":"l[v[vadas]]", "josh": "l[v[josh]]", "lop":"l[v[lop]]", "marko":"l[v[marko]]"}] |

  Scenario: g_V_hasXlangX_group_byXlangX_byXcountX
    Given the modern graph
    And the traversal of
      """
      g.V().has("lang").group().by("lang").by(__.count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"java":"d[2].l"}] |

  Scenario: g_V_group_byXoutE_countX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().order().by("name").group().by(__.outE().count()).by("name")
      """
    When iterated to list
    Then the result should be of
      | result |
      | m[{"d[0].l":["lop","ripple","vadas"],"d[1].l":["peter"],"d[2].l":["josh"],"d[3].l":["marko"]}] |
      | m[{"d[0].l":["lop","vadas","ripple"],"d[1].l":["peter"],"d[2].l":["josh"],"d[3].l":["marko"]}] |
      | m[{"d[0].l":["vadas","lop","ripple"],"d[1].l":["peter"],"d[2].l":["josh"],"d[3].l":["marko"]}] |
      | m[{"d[0].l":["vadas","ripple","lop"],"d[1].l":["peter"],"d[2].l":["josh"],"d[3].l":["marko"]}] |
      | m[{"d[0].l":["ripple","vadas","lop"],"d[1].l":["peter"],"d[2].l":["josh"],"d[3].l":["marko"]}] |
      | m[{"d[0].l":["ripple","lop","vadas"],"d[1].l":["peter"],"d[2].l":["josh"],"d[3].l":["marko"]}] |
    And the result should have a count of 1

  Scenario: g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX
    Given the grateful graph
    And the traversal of
      """
      g.V().repeat(__.both("followedBy")).times(2).group().by("songType").by(__.count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"original":"d[771317].l", "":"d[160968].l", "cover":"d[368579].l"}] |

  Scenario: g_V_group_byXvaluesXnameX_substringX1XX_byXconstantX1XX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by(__.values("name").substring(0,1)).by(__.constant(1))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"p":"d[1].i", "r":"d[1].i", "v":"d[1].i", "j":"d[1].i", "l":"d[1].i", "m":"d[1].i"}] |

  Scenario: g_V_out_group_byXlabelX_selectXpersonX_unfold_outXcreatedX_name_limitX2X
    Given the modern graph
    And the traversal of
      """
      g.V().out().group().by(T.label).select("person").unfold().out("created").values("name").limit(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop |

  Scenario: g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX
    Given the grateful graph
    And the traversal of
      """
      g.V().hasLabel("song").group().by("name").by(__.properties().groupCount().by(T.label))
      """
    When iterated next
    Then the result should have a count of 584

  Scenario: g_V_repeatXunionXoutXknowsX_groupXaX_byXageX__outXcreatedX_groupXbX_byXnameX_byXcountXX_groupXaX_byXnameXX_timesX2X_capXa_bX
    Given an unsupported test
    Then nothing should happen because
      """
      The result is [a:[32:[v[4]],27:[v[2]],josh:[v[4]],ripple:[v[5],v[5]],lop:[v[3],v[3],v[3],v[3]],vadas:[v[2]]],b:[ripple:2,lop:4]] which is not supported by this test suite
      """

  Scenario: g_V_group_byXbothE_countX_byXgroup_byXlabelXX
    Given an unsupported test
    Then nothing should happen because
      """
      The result is [1:[software:[v[5]],person:[v[2],v[6]]],3:[software:[v[3]],person:[v[1],v[4]]]] which is not supported by this test suite
      """

  Scenario: g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX
    Given the grateful graph
    And the traversal of
      """
      g.V().out("followedBy").group().by("songType").by(__.bothE().group().by(T.label).by(__.values("weight").sum()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"cover":{"followedBy":"d[777982].i"}, "":{"followedBy":"d[179350].i"}, "original":{"followedBy":"d[2185613].i"}}] |

  Scenario: g_V_group_byXlabelX_byXbothE_groupXaX_byXlabelX_byXweight_sumX_weight_sumX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by(T.label).by(__.bothE().group("a").by(T.label).by(__.values("weight").sum()).values("weight").sum())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"software":"d[2.0].d", "person":"d[5.0].d"}] |

  # This test works, but the test frameworks don't all support it well because of how parameters are now processed
  # in 3.6.0 where we need more than just simple JSON processing. would be nice to "ignore" it somehow as it does
  # work in some situations but not all test frameworks have that ability either (i.e. to ignore prior to parsing
  # the parameter). TINKERPOP-2699
  #
  # The post-ordering really isn't really right but works around TINKERPOP-2600
#  Scenario: g_withSideEffectXa__marko_666_noone_blahX_V_groupXaX_byXnameX_byXoutE_label_foldX_capXaX
#    Given the modern graph
#    And using the parameter xx1 defined as "m[{\"marko\":\"l[\"666\"]\", \"noone\":\"l[\"blah\"]\"}]"
#    And the traversal of
#      """
#      g.withSideEffect("a", xx1).V().group("a").by("name").by(__.outE().label().fold()).cap("a").unfold().group().by(Column.keys).by(select(Column.values).order(Scope.local).by(Order.asc))
#      """
#    When iterated to list
#    Then the result should be unordered
#      | result |
#      | m[{"ripple":[], "peter":["created"], "noone":["blah"], "vadas":[], "josh":["created", "created"], "lop":[], "marko":["666", "created", "knows", "knows"]}] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_hasLabelXpersonX_asXpX_outXcreatedX_group_byXnameX_byXselectXpX_valuesXageX_sumX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").as("p").out("created").group().by("name").by(__.select("p").values("age").sum())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[32].i", "lop":"d[96].i"}] |

  Scenario: g_V_group_byXlabelX_byXlabel_countX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by(__.label()).by(__.label().count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"software":"d[2].l", "person":"d[4].l"}] |

  # validates that a reducing barrier in the value traversal produces an output when a collecting barrier does not.
  Scenario: g_V_hasXperson_name_withinXvadas_peterXX_group_by_byXout_order_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name",P.within("vadas","peter")).group().by().by(__.out().order().fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"v[vadas]":[], "v[peter]":"l[v[lop]]"}] |

  Scenario: g_V_hasXperson_name_withinXvadas_peterXX_group_by_byXout_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name",P.within("vadas","peter")).group().by().by(__.out().fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"v[vadas]":"l[]", "v[peter]":"l[v[lop]]"}] |

  # validates that a collecting barrier produces a filtering effect if it is unproductive.
  Scenario: g_V_hasXperson_name_withinXvadas_peterXX_group_by_byXout_orderX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name",P.within("vadas","peter")).group().by().by(__.out().order())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"v[peter]":"v[lop]"}] |

  Scenario: g_V_hasXperson_name_withinXvadas_peterXX_group_by_byXout_order_countX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name",P.within("vadas","peter")).group().by().by(__.out().order().count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"v[vadas]":"d[0].l", "v[peter]":"d[1].l"}] |

  Scenario: g_V_hasXperson_name_withinXvadas_peterXX_group_by_byXout_order_fold_countXlocalXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name",P.within("vadas","peter")).group().by().by(__.out().order().fold().count(local))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"v[vadas]":"d[0].l", "v[peter]":"d[1].l"}] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_group_by_byXout_label_foldX_selectXvaluesX_unfold_orderXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by().by(__.out().label().fold()).select(Column.values).unfold().order(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[person,person,software] |
      | l[] |
      | l[] |
      | l[software,software] |
      | l[] |
      | l[software] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_group_by_byXout_label_dedup_foldX_selectXvaluesX_unfold_orderXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by().by(__.out().label().dedup().fold()).select(Column.values).unfold().order(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[person,software] |
      | l[] |
      | l[] |
      | l[software] |
      | l[] |
      | l[software] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_group_by_byXout_label_limitX0X_foldX_selectXvaluesX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().group().by().by(__.out().label().limit(0).fold()).select(Column.values).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[] |
      | l[] |
      | l[] |
      | l[] |
      | l[] |
      | l[] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_group_by_byXout_label_limitX10X_foldX_selectXvaluesX_unfold_orderXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by().by(__.out().label().limit(10).fold()).select(Column.values).unfold().order(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[person,person,software] |
      | l[] |
      | l[] |
      | l[software,software] |
      | l[] |
      | l[software] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_group_by_byXout_label_tailX10X_foldX_selectXvaluesX_unfold_orderXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by().by(__.out().label().tail(10).fold()).select(Column.values).unfold().order(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[person,person,software] |
      | l[] |
      | l[] |
      | l[software,software] |
      | l[] |
      | l[software] |

  Scenario: g_V_groupXaX_byXnameX_by_selectXaX_countXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().group("a").by("name").by().select("a").count(local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |
      | d[6].l |
      | d[6].l |
      | d[6].l |
      | d[6].l |
      | d[6].l |

  Scenario: g_V_localXgroupXaX_byXnameX_by_selectXaX_countXlocalXX
    Given the modern graph
    And the traversal of
      """
      g.V().local(group("a").by("name").by().select("a").count(local))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |
      | d[2].l |
      | d[3].l |
      | d[4].l |
      | d[5].l |
      | d[6].l |

  Scenario: g_V_group_byXvaluesXnameXX_byXboth_countX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by(values('name')).by(both().count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"marko":"d[3].l", "vadas":"d[1].l", "lop":"d[3].l", "josh":"d[3].l", "ripple":"d[1].l", "peter":"d[1].l"}] |
