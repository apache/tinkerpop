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

@StepClassBranch @StepUnion
Feature: Step - union()

  Scenario: g_unionXX
    Given the modern graph
    And the traversal of
       """
       g.union()
       """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_unionXV_name
    Given the modern graph
    And the traversal of
       """
       g.union(__.V().values("name"))
       """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko  |
      | vadas  |
      | lop    |
      | josh   |
      | ripple |
      | peter  |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_unionXVXv1X_VX4XX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[vadas].id"
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
       """
       g.union(__.V(vid1), __.V(vid4)).values("name")
       """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas  |
      | josh   |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_unionXV_hasLabelXsoftwareX_V_hasLabelXpersonXX_name
    Given the modern graph
    And the traversal of
       """
       g.union(__.V().hasLabel("software"), __.V().hasLabel("person")).values("name")
       """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko  |
      | vadas  |
      | lop    |
      | josh   |
      | ripple |
      | peter  |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_unionXV_out_out_V_hasLabelXsoftwareXX_path
    Given the modern graph
    And the traversal of
       """
       g.union(__.V().out().out(), __.V().hasLabel("software")).path()
       """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[marko],v[josh],v[lop]] |
      | p[v[lop]] |
      | p[v[ripple]] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_unionXV_out_out_V_hasLabelXsoftwareXX_path_byXnameX
    Given the modern graph
    And the traversal of
       """
       g.union(__.V().out().out(), __.V().hasLabel("software")).path().by("name")
       """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[marko,josh,ripple] |
      | p[marko,josh,lop] |
      | p[lop] |
      | p[ripple] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_unionXunionXV_out_outX_V_hasLabelXsoftwareXX_path_byXnameX
    Given the modern graph
    And the traversal of
       """
       g.union(__.union(__.V().out().out()), __.V().hasLabel("software")).path().by("name")
       """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[marko,josh,ripple] |
      | p[marko,josh,lop] |
      | p[lop] |
      | p[ripple] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_unionXinjectX1X_injectX2X
    Given the modern graph
    And the traversal of
       """
       g.union(__.inject(1), __.inject(2))
       """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[2].i |

  Scenario: g_V_unionXconstantX1X_constantX2X_constantX3XX
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
       """
       g.V(vid2).union(constant(1i), constant(2i), constant(3i))
       """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[2].i |
      | d[3].i |

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
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).union(__.repeat(__.out()).times(2), __.out()).values("name")
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
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid1, vid2).union(__.outE().count(), __.inE().count(), __.outE().values("weight").sum())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l   |
      | d[1.9].d |
      | d[1].l   |

  Scenario: g_VX1_2X_localXunionXoutE_count__inE_count__outE_weight_sumXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid1, vid2).local(__.union(__.outE().count(), __.inE().count(), __.outE().values("weight").sum()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l   |
      | d[0].l   |
      | d[1.9].d |
      | d[0].l   |
      | d[1].l   |

  Scenario: g_VX1_2X_localXunionXcountXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid1, vid2).local(__.union(__.count()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l   |
      | d[1].l   |

  Scenario: g_unionXaddVXpersonX_propertyXname_aliceX_addVXpersonX_propertyXname_bobX_addVXpersonX_propertyXname_chrisX_name
    Given the empty graph
    And the traversal of
      """
      g.union(__.addV("person").property("name", "alice"),
              __.addV("person").property("name", "bob"),
              __.addV("person").property("name", "chris")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | alice |
      | bob |
      | chris |

  Scenario: g_VX_hasLabelXpersonX_unionX_whereX_out_count_isXgtX2XXX_valuesXageX_notX_whereX_bothE_count_isXgt2XXX_valusXnameXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").union(where(outE().count().is(P.gt(2))).values("age"),
                                     __.not(where(outE().count().is(P.gt(2)))).values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | vadas |
      | josh |
      | peter |