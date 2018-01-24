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

Feature: Step - union()

  Scenario: g_V_unionXout__inX_name
    Given the modern graph
    And the traversal of
      """
      g.V().union(__.out(), __.in()).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | vadas |
      | josh |
      | marko |
      | marko |
      | josh |
      | peter |
      | ripple |
      | lop |
      | marko |
      | josh |
      | lop |

  Scenario: g_VX1X_unionXrepeatXoutX_timesX2X__outX_name
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).union(__.repeat(__.out()).times(2), __.out()).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop |
      | lop   |
      | vadas |
      | josh  |

  Scenario: g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.label().is("person"), __.union(__.out().values("lang"), __.out().values("name")), __.in().label())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | java   |
      | lop    |
      | vadas  |
      | josh   |
      | person |
      | person |
      | person |
      | java   |
      | java   |
      | ripple |
      | lop    |
      | person |
      | java   |
      | lop    |

  Scenario: g_V_chooseXlabel_is_person__unionX__out_lang__out_nameX__in_labelX_groupCount
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.label().is("person"), __.union(__.out().values("lang"), __.out().values("name")), __.in().label()).groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"java":"d[4].l","ripple":"d[1].l","person":"d[4].l","vadas":"d[1].l","josh":"d[1].l","lop":"d[3].l"}] |

  Scenario: g_V_unionXrepeatXunionXoutXcreatedX__inXcreatedXX_timesX2X__repeatXunionXinXcreatedX__outXcreatedXX_timesX2XX_label_groupCount
    Given the modern graph
    And the traversal of
      """
      g.V().union(__.repeat(__.union(
                            __.out("created"),
                            __.in("created"))).times(2),
                  __.repeat(__.union(
                            __.in("created"),
                            __.out("created"))).times(2)).label().groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"software":"d[12].l","person":"d[20].l"}] |

  Scenario: g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter v2Id defined as "v[vadas].id"
    And the traversal of
      """
      g.V(v1Id, v2Id).union(__.outE().count(), __.inE().count(), __.outE().values("weight").sum())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l   |
      | d[1.9].d |
      | d[1].l   |

  Scenario: g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter v2Id defined as "v[vadas].id"
    And the traversal of
      """
      g.V(v1Id, v2Id).local(__.union(__.outE().count(), __.inE().count(), __.outE().values("weight").sum()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l   |
      | d[0].l   |
      | d[1.9].d |
      | d[0].i   |
      | d[0].l   |
      | d[1].l   |

  Scenario: g_VX1_2X_localXunionXcountXX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter v2Id defined as "v[vadas].id"
    And the traversal of
      """
      g.V(v1Id, v2Id).local(__.union(__.count()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l   |
      | d[1].l   |