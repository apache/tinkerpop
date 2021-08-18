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
      | m[{"null":"l[v[lop],v[ripple]]", "d[35].i":"l[v[peter]]", "d[27].i":"l[v[vadas]]", "d[32].i": "l[v[josh]]", "d[29].i":"l[v[marko]]"}] |

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

  Scenario: g_V_groupXaX_byXnameX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().group("a").by("name").cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"l[v[ripple]]", "peter":"l[v[peter]]", "vadas":"l[v[vadas]]", "josh": "l[v[josh]]", "lop":"l[v[lop]]", "marko":"l[v[marko]]"}] |

  Scenario: g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().has("lang").group("a").by("lang").by("name").out().cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"java":["lop","ripple"]}] |

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

  Scenario: g_V_repeatXout_groupXaX_byXnameX_byXcountX_timesX2X_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.out().group("a").by("name").by(__.count())).times(2).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[2].l", "vadas":"d[1].l", "josh":"d[1].l", "lop":"d[4].l"}] |

  Scenario: g_V_group_byXoutE_countX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by(__.outE().count()).by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[0].l":["vadas","lop","ripple"],"d[1].l":["peter"],"d[2].l":["josh"],"d[3].l":["marko"]}] |

  Scenario: g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().group("a").by(T.label).by(__.outE().values("weight").sum()).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"person":"d[3.5].d"}] |

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

  Scenario: g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX
    Given the grateful graph
    And the traversal of
      """
      g.V().repeat(__.both("followedBy")).times(2).group("a").by("songType").by(__.count()).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"original":"d[771317].l", "":"d[160968].l", "cover":"d[368579].l"}] |

  Scenario: g_V_group_byXname_substring_1X_byXconstantX1XX
    Given the modern graph
    And using the parameter l1 defined as "c[it.value('name').substring(0, 1)]"
    And the traversal of
      """
      g.V().group().by(l1).by(__.constant(1))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"p":"d[1].i", "r":"d[1].i", "v":"d[1].i", "j":"d[1].i", "l":"d[1].i", "m":"d[1].i"}] |

  Scenario: g_V_groupXaX_byXname_substring_1X_byXconstantX1XX_capXaX
    Given the modern graph
    And using the parameter l1 defined as "c[it.value('name').substring(0, 1)]"
    And the traversal of
      """
      g.V().group("a").by(l1).by(__.constant(1)).cap("a")
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

  Scenario: g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX
    Given the grateful graph
    And the traversal of
      """
      g.V().hasLabel("song").group("a").by("name").by(__.properties().groupCount().by(T.label)).out().cap("a")
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
      | m[{"cover":{"followedBy":"d[777982].l"}, "":{"followedBy":"d[179350].l"}, "original":{"followedBy":"d[2185613].l"}}] |

  Scenario: g_V_groupXmX_byXnameX_byXinXknowsX_nameX_capXmX
    Given the modern graph
    And the traversal of
      """
      g.V().group("m").by("name").by(__.in("knows").values("name")).cap("m")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"vadas":"marko", "josh":"marko"}] |

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

  # The post-ordering really isn't really right but works around TINKERPOP-2600
  Scenario: g_withSideEffectXa__marko_666_noone_blahX_V_groupXaX_byXnameX_byXoutE_label_foldX_capXaX
    Given the modern graph
    And using the parameter xx1 defined as "m[{\"marko\":[\"666\"], \"noone\":[\"blah\"]}]"
    And the traversal of
      """
      g.withSideEffect("a", xx1).V().group("a").by("name").by(__.outE().label().fold()).cap("a").unfold().group().by(keys).by(select(values).order(Scope.local).by(Order.asc))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":[], "peter":["created"], "noone":["blah"], "vadas":[], "josh":["created", "created"], "lop":[], "marko":["666", "created", "knows", "knows"]}] |

  Scenario: g_V_hasLabelXpersonX_asXpX_outXcreatedX_group_byXnameX_byXselectXpX_valuesXageX_sumX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").as("p").out("created").group().by("name").by(__.select("p").values("age").sum())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[32].l", "lop":"d[96].l"}] |

  Scenario: g_V_hasLabelXpersonX_asXpX_outXcreatedX_groupXaX_byXnameX_byXselectXpX_valuesXageX_sumX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").as("p").out("created").group("a").by("name").by(__.select("p").values("age").sum()).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[32].l", "lop":"d[96].l"}] |


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

  Scenario: g_V_groupXmX_byXlabelX_byXlabel_countX_capXmX
    Given the modern graph
    And the traversal of
      """
      g.V().group("m").by(__.label()).by(__.label().count()).cap("m")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"software":"d[2].l", "person":"d[4].l"}] |
