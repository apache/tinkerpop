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

@StepClassBranch @StepChoose
Feature: Step - choose()

  Scenario: g_V_chooseXout_countX_optionX2L_nameX_optionX3L_ageX
    Given the modern graph
    And using the parameter xx1 defined as "d[2].l"
    And using the parameter xx2 defined as "d[3].l"
    And the traversal of
      """
      g.V().choose(__.out().count()).
        option(xx1, __.values("name")).
        option(xx2, __.values("age"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | josh |

  Scenario: g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX__identityX_name
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.hasLabel("person").and().out("created"),
                   __.out("knows"),
                   __.identity()).
        values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | ripple |
      | josh |
      | vadas |
      | vadas |

  Scenario: g_V_chooseXlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone__identityX_name
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.label()).
             option("blah", __.out("knows")).
             option("bleep", __.out("created")).
             option(Pick.none, __.identity()).
        values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | peter |
      | josh |
      | lop |
      | ripple |

  Scenario: g_V_chooseXoutXknowsX_count_isXgtX0XX__outXknowsXX_name
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.out("knows").count().is(P.gt(0)),
                   __.out("knows")).
        values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |
      | vadas |
      | josh |
      | peter |
      | lop  |
      | ripple |

  Scenario: g_V_hasLabelXpersonX_asXp1X_chooseXoutEXknowsX__outXknowsXX_asXp2X_selectXp1_p2X_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").as("p1").
        choose(__.outE("knows"), __.out("knows")).as("p2").
        select("p1", "p2").
          by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"p1":"marko", "p2":"vadas"}] |
      | m[{"p1":"marko", "p2":"josh"}] |
      | m[{"p1":"vadas", "p2":"vadas"}] |
      | m[{"p1":"josh", "p2":"josh"}] |
      | m[{"p1":"peter", "p2":"peter"}] |

  Scenario: g_V_hasLabelXpersonX_chooseXageX__optionX27L__constantXyoungXX_optionXnone__constantXoldXX_groupCount
    Given the modern graph
    And using the parameter xx1 defined as "d[27].l"
    And the traversal of
      """
      g.V().hasLabel("person").choose(__.values("age")).
          option(xx1, __.constant("young")).
          option(Pick.none, __.constant("old")).
        groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"young":"d[1].l", "old":"d[3].l"}] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1X_chooseXisX1X__constantX10Xfold__foldX
    Given the empty graph
    And using the parameter xx1 defined as "d[10].i"
    And using the parameter xx2 defined as "d[1].i"
    And the traversal of
      """
      g.inject(xx2).choose(__.is(xx2), __.constant(xx1).fold(), __.fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[10].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX2X_chooseXisX1X__constantX10Xfold__foldX
    Given the empty graph
    And using the parameter xx1 defined as "d[10].i"
    And using the parameter xx2 defined as "d[1].i"
    And using the parameter xx3 defined as "d[2].i"
    And the traversal of
      """
      g.inject(xx3).choose(__.is(xx2), __.constant(xx1).fold(), __.fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[2].i] |
