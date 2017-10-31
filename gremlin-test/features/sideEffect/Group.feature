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
      | m[{"java":2}] |

  Scenario: g_V_repeatXout_groupXaX_byXnameX_byXcountX_timesX2X_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.out().group("a").by("name").by(__.count())).times(2).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":2, "vadas":1, "josh":1, "lop":4}] |

  Scenario: g_V_group_byXoutE_countX_byXnameX
    Given an unsupported test
    Then nothing should happen because
      """
      The result returned is not supported under GraphSON 2.x and therefore cannot be properly asserted. More
      specifically it has numeric keys which basically get toString()'d under GraphSON 2.x. This test can be supported
      with GraphSON 3.x.
      """

  Scenario: g_V_groupXaX_byXlabelX_byXoutE_weight_sumX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().group("a").by(T.label).by(__.outE().values("weight").sum()).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"software":0, "person":3.5}] |

  Scenario: g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX
    Given the grateful graph
    And the traversal of
      """
      g.V().repeat(__.both("followedBy")).times(2).group().by("songType").by(__.count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"original":771317, "":160968, "cover":368579}] |

  Scenario: g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX
    Given the grateful graph
    And the traversal of
      """
      g.V().repeat(__.both("followedBy")).times(2).group("a").by("songType").by(__.count()).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"original":771317, "":160968, "cover":368579}] |

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
      | m[{"p":1, "r":1, "v":1, "j":1, "l":1, "m":1}] |

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
      | m[{"p":1, "r":1, "v":1, "j":1, "l":1, "m":1}] |

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

  # TODO: can we change the traversal to allow for a better assertion
  Scenario: g_V_hasLabelXsongX_group_byXnameX_byXproperties_groupCount_byXlabelXX
    Given the grateful graph
    And the traversal of
      """
      g.V().hasLabel("song").group().by("name").by(__.properties().groupCount().by(T.label))
      """
    When iterated next
    Then should have a result count of 584

  Scenario: g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX
    Given the grateful graph
    And the traversal of
      """
      g.V().hasLabel("song").group("a").by("name").by(__.properties().groupCount().by(T.label)).out().cap("a")
      """
    When iterated next
    Then should have a result count of 584

  Scenario: g_V_repeatXunionXoutXknowsX_groupXaX_byXageX__outXcreatedX_groupXbX_byXnameX_byXcountXX_groupXaX_byXnameXX_timesX2X_capXa_bX
    Given an unsupported test
    Then nothing should happen because
      """
      The result returned is not supported under GraphSON 2.x and therefore cannot be properly asserted. More
      specifically it has numeric keys which basically get toString()'d under GraphSON 2.x. This test can be supported
      with GraphSON 3.x.
      """

  Scenario: g_V_group_byXbothE_countX_byXgroup_byXlabelXX
    Given an unsupported test
    Then nothing should happen because
      """
      The result returned is not supported under GraphSON 2.x and therefore cannot be properly asserted. More
      specifically it has numeric keys which basically get toString()'d under GraphSON 2.x. This test can be supported
      with GraphSON 3.x.
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
      | m[{"cover":{"followedBy":777982, "sungBy":0, "writtenBy":0}, "":{"followedBy":179350}, "original":{"followedBy":2185613, "sungBy":0, "writtenBy":0}}] |

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
      | m[{"software":2.0, "person":5.0}] |

  Scenario: g_withSideEffectXa__marko_666_noone_blahX_V_groupXaX_byXnameX_byXoutE_label_foldX_capXaX
    Given the modern graph
    And using the parameter m defined as "m[{\"marko\":[666], \"noone\":[\"blah\"]}]"
    And the traversal of
      """
      g.withSideEffect("a", m).V().group("a").by("name").by(__.outE().label().fold()).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":[], "peter":["created"], "noone":["blah"], "vadas":[], "josh":["created", "created"], "lop":[], "marko":[666, "created", "knows", "knows"]}] |
