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
      | v[vadas] |
      | v[lop] |
      | josh |
      | v[ripple] |
      | v[peter] |

  Scenario: g_V_chooseXout_countX_optionX2L_nameX_optionX3L_ageX_optionXnone_discardX
    Given the modern graph
    And using the parameter xx1 defined as "d[2].l"
    And using the parameter xx2 defined as "d[3].l"
    And the traversal of
      """
      g.V().choose(__.out().count()).
        option(xx1, __.values("name")).
        option(xx2, __.values("age")).
        option(Pick.none, __.discard())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | josh |

  Scenario: g_V_chooseXhasLabelXpersonX_and_outXcreatedX__outXknowsX_identityX_name
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

  Scenario: g_V_chooseXhasLabelXpersonX_and_outXcreatedX_outXknowsX_name
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.hasLabel("person").and().out("created"),
                   __.out("knows")).
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

  Scenario: g_V_chooseXTlabelX_optionXperson__outXknowsX_nameX_optionXbleep_constantXbleepXX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(T.label).
              option("person", __.out("knows").values("name")).
              option("bleep", __.constant("bleep"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |
      | v[lop] |
      | v[ripple] |

  Scenario: g_V_chooseXTlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone__identityX_name
    Given the modern graph
    And the traversal of
      """
      g.V().choose(T.label).
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

  Scenario: g_V_chooseXTlabelX_optionXblah__outXknowsXX_optionXbleep__outXcreatedXX_optionXnone_discardX_name
    Given the modern graph
    And the traversal of
      """
      g.V().choose(T.label).
             option("blah", __.out("knows")).
             option("bleep", __.out("created")).
             option(Pick.none, __.discard()).
        values("name")
      """
    When iterated to list
    Then the result should be empty

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
    And using the parameter xx1 defined as "d[1].i"
    And the traversal of
      """
      g.inject(1i).choose(__.is(xx1), __.constant(10i).fold(), __.fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[10].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX2X_chooseXisX1X__constantX10Xfold__foldX
    Given the empty graph
    And using the parameter xx1 defined as "d[1].i"
    And the traversal of
      """
      g.inject(2i).choose(__.is(xx1), __.constant(10i).fold(), __.fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[2].i] |

  Scenario: g_V_chooseXhasLabelXpersonX_chooseXageX_optionXbetweenX26_30X_constantXxXX_optionXbetweenX20_30X_constantXyXX_optionXnone_constantXzXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        choose(__.values("age")).
          option(P.between(26, 30), __.constant("x")).
          option(P.between(20, 30), __.constant("y")).
          option(Pick.none, __.constant("z"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | x |
      | x |
      | z |
      | z |

  Scenario: g_V_chooseXhasLabelXpersonX_chooseXageX_optionXbetweenX26_30X_orXgtX34XX_constantXxXX_optionXgtX34X_constantXyXX_optionXnone_constantXzXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        choose(__.values("age")).
          option(P.between(26, 30).or(P.gt(34)), __.constant("x")).
          option(P.gt(34), __.constant("y")).
          option(Pick.none, __.constant("z"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | x |
      | x |
      | x |
      | z |

  Scenario: g_V_hasLabelXpersonX_chooseXageX_optionXbetweenX26_30X_nameX_optionXnone_nameX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        choose(__.values("age")).
          option(P.between(26, 30), __.values("name")).
          option(Pick.none, __.values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
      | peter |

  Scenario: g_V_chooseXhasLabelXpersonX_localXchooseXageX_optionXbetweenX26_30X_name_foldX_optionXnone_name_foldXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        local(__.choose(__.values("age")).
                   option(P.between(26, 30), __.values("name").fold()).
                   option(Pick.none, __.values("name").fold()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko] |
      | l[vadas] |
      | l[josh] |
      | l[peter] |

  Scenario: g_V_chooseXhasLabelXpersonX_mapXchooseXageX_optionXbetweenX26_30X_name_foldX_optionXnone_name_foldXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        map(__.choose(__.values("age")).
                 option(P.between(26, 30), __.values("name").fold()).
                 option(Pick.none, __.values("name").fold()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko] |
      | l[vadas] |
      | l[josh] |
      | l[peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_unionXV_VXhasLabelXpersonX_barrier_localXchooseXageX_optionXbetweenX26_30X_name_foldX_optionXnone_name_foldXX
    Given the modern graph
    And the traversal of
      """
      g.union(__.V(), __.V()).hasLabel("person").barrier().
        local(__.choose(__.values("age")).
                   option(P.between(26, 30), __.values("name").fold()).
                   option(Pick.none, __.values("name").fold()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko] |
      | l[marko] |
      | l[vadas] |
      | l[vadas] |
      | l[josh] |
      | l[josh] |
      | l[peter] |
      | l[peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_unionXV_VXhasLabelXpersonX_barrier_mapXchooseXageX_optionXbetweenX26_30X_name_foldX_optionXnone_name_foldXX
    Given the modern graph
    And the traversal of
      """
      g.union(__.V(), __.V()).hasLabel("person").barrier().
        map(__.choose(__.values("age")).
                 option(P.between(26, 30), __.values("name").fold()).
                 option(Pick.none, __.values("name").fold()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko] |
      | l[marko] |
      | l[vadas] |
      | l[vadas] |
      | l[josh] |
      | l[josh] |
      | l[peter] |
      | l[peter] |

  Scenario: g_V_chooseXageX_optionXbetweenX26_30X_nameX_optionXnone_nameX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.values("age")).
              option(P.between(26, 30), __.values("name")).
              option(Pick.none, __.values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | v[lop] |
      | josh |
      | v[ripple] |
      | peter |

  Scenario: g_V_chooseXageX_optionXbetweenX26_30X_nameX_optionXnone_nameX_optionXunproductive_labelX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.values("age")).
              option(P.between(26, 30), __.values("name")).
              option(Pick.none, __.values("name")).
              option(Pick.unproductive, __.label())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | software |
      | josh |
      | software |
      | peter |

  Scenario: g_V_chooseXageX_optionXbetweenX26_30X_nameX_optionXnone_nameX_optionXnone_identityX_optionXnone_failX_optionXunproductive_identityX_optionXunproductive_labelX_optionXnone_failX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.values("age")).
              option(P.between(26, 30), __.values("name")).
              option(Pick.none, __.values("name")).
              option(Pick.none, __.identity()).
              option(Pick.none, __.fail()).
              option(Pick.unproductive, __.label()).
              option(Pick.unproductive, __.identity()).
              option(Pick.unproductive, __.fail())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | software |
      | josh |
      | software |
      | peter |

  Scenario: g_V_chooseXage_nameX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.values("age"), __.values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | v[lop] |
      | josh |
      | v[ripple] |
      | peter |

  Scenario: g_V_chooseXageX_optionXbetweenX26_30X_nameX_optionXnone_discardX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.values("age")).
              option(P.between(26, 30), __.values("name")).
              option(Pick.none, __.discard())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | v[lop] |
      | v[ripple] |

  Scenario: g_V_chooseXnameX_optionXneqXyX_ageX_optionXnone_constantXxXX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.values("name")).
              option(P.neq("y"), __.values("age")).
              option(Pick.none, __.constant("x"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_hasLabelXpersonX_chooseXoutXcreatedX_count_isXeqX0XX__constantXdidnt_createX__constantXcreatedXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        choose(__.out("created").count().is(P.eq(0)),
               __.constant("didnt_create"),
               __.constant("created"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | didnt_create |
      | created |
      | created |
      | created |

  Scenario: g_V_hasLabelXpersonX_chooseXvaluesXageX_isXgtX30XX__valuesXageX__constantX30XX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        choose(__.values("age").is(P.gt(30)),
               __.values("age"),
               __.constant(30))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[30].i |
      | d[30].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_hasLabelXpersonX_chooseXvaluesXageX_isXgtX29XX_and_valuesXageX_isXltX35XX__valuesXnameX__constantXotherXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        choose(__.values("age").is(P.gt(29)).and().values("age").is(P.lt(35)),
               __.values("name"),
               __.constant("other"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | other |
      | other |
      | josh |
      | other |

  Scenario: g_V_chooseXhasXname_vadasX__valuesXnameX__valuesXageXX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.has("name", "vadas"),
                   __.values("name"),
                   __.values("age"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | vadas |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_hasLabelXpersonX_chooseXoutXcreatedX_countX_optionX0__constantXnoneXX_optionX1__constantXoneXX_optionX2__constantXmanyXX
    Given the modern graph
    And using the parameter xx0 defined as "d[0].l"
    And using the parameter xx1 defined as "d[1].l"
    And using the parameter xx2 defined as "d[2].l"
    And the traversal of
      """
      g.V().hasLabel("person").
        choose(__.out("created").count()).
          option(xx0, __.constant("none")).
          option(xx1, __.constant("one")).
          option(xx2, __.constant("many"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | none |
      | one |
      | many |
      | one |

  Scenario: g_V_hasLabelXpersonX_chooseXlocalXoutXknowsX_countX__optionX0__constantXnoFriendsXX__optionXnone__constantXhasFriendsXXX
    Given the modern graph
    And using the parameter xx0 defined as "d[0].l"
    And the traversal of
      """
      g.V().hasLabel("person").
        choose(__.local(__.out("knows").count())).
          option(xx0, __.constant("noFriends")).
          option(Pick.none, __.constant("hasFriends"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | hasFriends |
      | noFriends |
      | noFriends |
      | noFriends |

  Scenario: g_V_chooseXoutE_countX_optionX0__constantXnoneXX_optionXnone__constantXsomeXX
    Given the modern graph
    And using the parameter xx0 defined as "d[0].l"
    And the traversal of
      """
      g.V().choose(__.outE().count()).
        option(xx0, __.constant("none")).
        option(Pick.none, __.constant("some"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | some |
      | none |
      | some |
      | some |
      | none |
      | none |

  Scenario: g_V_chooseXlabelX_optionXperson__chooseXageX_optionXP_lt_30__constantXyoungXX_optionXP_gte_30__constantXoldXXX_optionXsoftware__constantXprogramXX_optionXnone__constantXunknownXX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.label()).
        option("person", __.choose(__.values("age")).
                            option(P.lt(30), __.constant("young")).
                            option(P.gte(30), __.constant("old"))).
        option("software", __.constant("program")).
        option(Pick.none, __.constant("unknown"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | young |
      | young |
      | old |
      | old |
      | program |
      | program |

  Scenario: g_V_chooseXhasXname_vadasX__valuesXnameXX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.has("name", "vadas"),
                   __.values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | vadas |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_V_hasLabelXpersonX_age_chooseXP_eqX29X_constantXmatchedX_constantXotherXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("age").
        choose(P.eq(29),
               __.constant("matched"),
               __.constant("other"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | matched |
      | other |
      | other |
      | other |

  Scenario: g_V_hasLabelXpersonX_age_chooseXP_eqX29X_constantXmatchedX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("age").
        choose(P.eq(29),
               __.constant("matched"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | matched |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_hasLabelXpersonX_chooseX_valuesXnameX_option1X_isXmarkoX_valuesXageXX_option2Xnone_valuesXnameXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").choose(values("name")).
        option(is("marko"), values("age")).
        option(Pick.none, values("name"))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Traversal is not allowed as a Pick token"

  Scenario: g_V_hasLabelXpersonX_chooseX_valuesXnameX_option1X_PeqXmarkoX_valuesXageXX_option2Xnone_valuesXnameXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").choose(values("name")).
        option(P.eq("marko"), values("age")).
        option(Pick.none, values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | vadas |
      | josh |
      | peter |
