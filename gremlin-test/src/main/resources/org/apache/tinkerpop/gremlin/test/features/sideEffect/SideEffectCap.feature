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

@StepClassSideEffect @StepCap
Feature: Step - cap()

  Scenario: g_V_hasXageX_groupCountXaX_byXnameX_out_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age").groupCount("a").by("name").out().cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"peter":"d[1].l", "vadas":"d[1].l", "josh":"d[1].l", "marko": "d[1].l"}] |

  Scenario: g_V_groupXaX_byXageX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().group("a").by("age").cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[35].i":"l[v[peter]]", "d[27].i":"l[v[vadas]]", "d[32].i": "l[v[josh]]", "d[29].i":"l[v[marko]]"}] |

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
    Then the result should be of
      | result |
      | m[{"java":["lop","ripple"]}] |
      | m[{"java":["ripple","lop"]}] |
    And the result should have a count of 1

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

  Scenario: g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX
    Given the grateful graph
    And the traversal of
      """
      g.V().hasLabel("song").group("a").by("name").by(__.properties().groupCount().by(T.label)).out().cap("a")
      """
    When iterated next
    Then the result should have a count of 584

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_hasLabelXpersonX_asXpX_outXcreatedX_groupXaX_byXnameX_byXselectXpX_valuesXageX_sumX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").as("p").out("created").group("a").by("name").by(__.select("p").values("age").sum()).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[32].i", "lop":"d[96].i"}] |

  @GraphComputerVerificationStarGraphExceeded
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

  Scenario: g_V_chooseXlabel_person__age_groupCountXaX__name_groupCountXbXX_capXa_bX
    Given an unsupported test
    Then nothing should happen because
      """
      The result is [a:[32:1,35:1,27:1,29:1],b:[ripple:1,lop:1]] which is not supported by this test suite
      """