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

Feature: Step - choose()

  Scenario: g_V_chooseXout_countX_optionX2L__nameX_optionX3L__valueMapX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.out().count()).
        option(2L, __.values("name")).
        option(3L, __.valueMap())
      """
    When iterated to list
    Then the result should be unordered
      | m[{"name":["marko"], "age":[29]}] |
      | josh |

  Scenario: g_V_chooseXlabel_eqXpersonX__outXknowsX__inXcreatedXX_name
    Given the modern graph
    And using the parameter l1 is "c[it.label() == 'person']"
    And the traversal of
      """
      g.V().choose(l1, __.out("knows"), __.in("created")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | josh |
      | vadas |
      | josh |
      | josh |
      | marko |
      | peter |

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
      | m[{"p1":"marko", "p2":"vadas"}] |
      | m[{"p1":"marko", "p2":"josh"}] |
      | m[{"p1":"vadas", "p2":"vadas"}] |
      | m[{"p1":"josh", "p2":"josh"}] |
      | m[{"p1":"peter", "p2":"peter"}] |
