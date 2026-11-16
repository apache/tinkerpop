# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@TinkerGQL @StepClassMap @StepMatch
Feature: Step - match() (String form)

  Scenario: g_match_person_selectXpX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (p:person)").select("p").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko  |
      | vadas  |
      | josh   |
      | peter  |

  Scenario: g_match_personXknowsX_person_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"vadas"}] |
      | m[{"a":"marko","b":"josh"}]  |

  Scenario: g_match_personXknowsX_personXcreatedX_software_selectXa_b_sX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->(b:person)-[:created]->(s:software)").select("a","b","s").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"josh","s":"ripple"}] |
      | m[{"a":"marko","b":"josh","s":"lop"}]    |

  Scenario: g_match_softwareXreversedCreatedX_person_selectXp_sX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (s:software)<-[:created]-(p:person)").select("p","s").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"p":"marko","s":"lop"}]   |
      | m[{"p":"josh","s":"ripple"}] |
      | m[{"p":"josh","s":"lop"}]    |
      | m[{"p":"peter","s":"lop"}]   |

  Scenario: g_match_personXundirectedKnowsX_person_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]-(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"vadas"}] |
      | m[{"a":"marko","b":"josh"}]  |
      | m[{"a":"vadas","b":"marko"}] |
      | m[{"a":"josh","b":"marko"}]  |

  Scenario: g_match_personXname_markoX_knowsPerson_selectXp_fX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (p:person {name: 'marko'})-[:knows]->(f:person)").select("p","f").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"p":"marko","f":"vadas"}] |
      | m[{"p":"marko","f":"josh"}]  |

  Scenario: g_match_personXname_paramX_knowsPerson_selectXp_fX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match('MATCH (p:person {name: $who})-[:knows]->(f:person)', ["who": "marko"]).select("p","f").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"p":"marko","f":"vadas"}] |
      | m[{"p":"marko","f":"josh"}]  |

  Scenario: g_match_multiPattern_sharedVariable_whereXa_neqXbXX_selectXa_b_sX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:created]->(s:software), (b:person)-[:created]->(s)").where("a", neq("b")).select("a","b","s").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"josh","s":"lop"}]  |
      | m[{"a":"marko","b":"peter","s":"lop"}] |
      | m[{"a":"josh","b":"marko","s":"lop"}]  |
      | m[{"a":"josh","b":"peter","s":"lop"}]  |
      | m[{"a":"peter","b":"marko","s":"lop"}] |
      | m[{"a":"peter","b":"josh","s":"lop"}]  |

  Scenario: g_match_terminalBindingMap
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->(b:person)")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[vadas]"}] |
      | m[{"a":"v[marko]","b":"v[josh]"}]  |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_inject_match_midTraversal_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.inject(1).match("MATCH (a:person)-[:knows]->(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"vadas"}] |
      | m[{"a":"marko","b":"josh"}]  |

  Scenario: g_match_anonymousXknowsX_person_selectXbX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH ()-[:knows]->(b:person)").select("b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas  |
      | josh   |

  Scenario: g_match_personXage_29iX_selectXpX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (p:person {age: 29i})").select("p").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko  |

  Scenario: g_match_absentAgeProperty_selectXvX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (v {age: null})").select("v").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop    |
      | ripple |

  Scenario: g_match_personXname_marko_age_29iX_selectXpX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (p:person {name: 'marko', age: 29i})").select("p").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko  |

  Scenario: g_match_personXknowsXeX_person_selectXeX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[e:knows]->(b:person)").select("e")
      """
    When iterated to list
    Then the result should be unordered
      | result                |
      | e[marko-knows->vadas] |
      | e[marko-knows->josh]  |

  Scenario: g_match_personXknowsXweight_1X_person_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows {weight: 1.0}]->(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result                      |
      | m[{"a":"marko","b":"josh"}] |

  Scenario: g_match_personXanyEdgeX_software_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[]->(b:software)").select("a","b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result                       |
      | m[{"a":"marko","b":"lop"}]   |
      | m[{"a":"josh","b":"ripple"}] |
      | m[{"a":"josh","b":"lop"}]    |
      | m[{"a":"peter","b":"lop"}]   |

  Scenario: g_match_personXknowsX_anyXcreatedX_software_selectXa_sX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->()-[:created]->(s:software)").select("a","s").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result                        |
      | m[{"a":"marko","s":"ripple"}] |
      | m[{"a":"marko","s":"lop"}]    |

  Scenario: g_match_personXage_paramX_knowsPerson_selectXp_fX_byXnameX_integer
    Given the modern graph
    And the traversal of
      """
      g.match('MATCH (p:person {age: $age})-[:knows]->(f:person)', ["age": 29]).select("p","f").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result                       |
      | m[{"p":"marko","f":"vadas"}] |
      | m[{"p":"marko","f":"josh"}]  |

  Scenario: g_match_noMatchPattern_emptyResult
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:software)-[:knows]->(b)").select("a","b")
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_inject_match_midTraversal_noMatch_emptyResult
    Given the modern graph
    And the traversal of
      """
      g.inject(1).match("MATCH (a:software)-[:knows]->(b:person)").select("a","b")
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_match_cyclicPattern_personXknowsX_personXcreatedX_softwareXcreatedX_selectXa_b_sX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->(b:person)-[:created]->(s:software)<-[:created]-(a)").select("a","b","s").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"josh","s":"lop"}] |

  Scenario: g_match_multiPattern_bridgeVariable_selectXa_b_sX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->(b:person), (b:person)-[:created]->(s:software)").select("a","b","s").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"josh","s":"ripple"}] |
      | m[{"a":"marko","b":"josh","s":"lop"}]    |

  Scenario: g_match_personXknowsXeVar_weight_1X_person_selectXa_e_bX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[e:knows {weight: 1.0}]->(b:person)").select("a","e","b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","e":"e[marko-knows->josh]","b":"v[josh]"}] |

  Scenario: g_match_anyXknowsX_any_selectXaX_byXnameX_concatXconstant_selectXbX_byXnameXX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a)-[:knows]->(b)").select("a").by("name").concat(constant(" knows "), select("b").by("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko knows vadas |
      | marko knows josh  |

  Scenario: g_match_personXknows_1_2X_person_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->{1,2}(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"vadas"}] |
      | m[{"a":"marko","b":"josh"}]  |

  Scenario: g_match_airportXcode_KUKX_route_1_2X_airport_selectXbX_byXcodeX
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'KUK'})-[:route]->{1,2}(b:airport)").select("b").by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | NUP |
      | BET |
      | ATT |

  Scenario: g_match_airportXcode_GYGX_route_2_X_airport_selectXbX_byXcodeX
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'GYG'})-[:route]->{2,}(b:airport)").select("b").by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | GYG |

  Scenario: g_match_airportXcode_GYGX_route_plusX_airport_selectXbX_byXcodeX
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'GYG'})-[:route]->+(b:airport)").select("b").by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | UMS |
      | GYG |

  Scenario: g_match_airportXcode_NUPX_reversedRoute_1_2X_airport_selectXbX_byXcodeX
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'NUP'})<-[:route]-{1,2}(b:airport)").select("b").by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | KUK |
      | BET |
      | ATT |

  Scenario: g_match_airportXcode_KUKX_undirectedRoute_1X_airport_selectXbX_byXcodeX
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'KUK'})-[:route]-{1}(b:airport)").select("b").by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | NUP |
      | BET |
      | ATT |

  Scenario: g_match_airportXcode_BGCX_undirectedRoute_1_2X_airport_selectXbX_byXcodeX
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'BGC'})-[:route]-{1,2}(b:airport)").select("b").by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | VRL |
      | VRL |
      | BGC |
      | BGC |
      | VSE |
      | VSE |
      | VSE |
      | VSE |

  Scenario: g_match_airportXcode_BQJX_routeXrVar_1X_airport_selectXrX
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'BQJ'})-[r:route]->{1}(b:airport)").select("r").by(__.unfold().map(__.inV().values("code")).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[DPT] |

  Scenario: g_match_airportXcode_KUKX_routeXrVar_2X_airport_selectXrX
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'KUK'})-[r:route]->{2}(b:airport)").select("r").by(__.unfold().map(__.inV().values("code")).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[NUP,BET] |
      | l[NUP,ATT] |

  Scenario: g_match_airportXrouteXrVar_2_3X_hub_selectXrX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("airport").property("name", "AAA").as("a").
        addV("airport").property("name", "BBB").as("b").
        addV("airport").property("name", "CCC").as("c").
        addV("hub").property("name", "DDD").as("d").
        addE("route").from("a").to("b").
        addE("route").from("b").to("c").
        addE("route").from("c").to("d")
      """
    And the traversal of
      """
      g.match("MATCH (a:airport)-[r:route]->{2,3}(b:hub)").select("r")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[e[BBB-route->CCC],e[CCC-route->DDD]] |
      | l[e[AAA-route->BBB],e[BBB-route->CCC],e[CCC-route->DDD]] |

  Scenario: g_match_airportXreversedRouteXrVar_2X_hub_selectXrX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("airport").property("name", "AAA").as("a").
        addV("airport").property("name", "BBB").as("b").
        addV("airport").property("name", "CCC").as("c").
        addV("hub").property("name", "DDD").as("d").
        addE("route").from("d").to("c").
        addE("route").from("c").to("b").
        addE("route").from("c").to("a")
      """
    And the traversal of
      """
      g.match("MATCH (a:airport)<-[r:route]-{2}(b:hub)").select("r")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[e[CCC-route->BBB],e[DDD-route->CCC]] |
      | l[e[CCC-route->AAA],e[DDD-route->CCC]] |

  Scenario: g_match_airportXundirectedRouteXrVar_2X_hub_selectXrX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("airport").property("name", "AAA").as("a").
        addV("airport").property("name", "BBB").as("b").
        addV("airport").property("name", "CCC").as("c").
        addV("hub").property("name", "DDD").as("d").
        addE("route").from("c").to("b").
        addE("route").from("c").to("d").
        addE("route").from("b").to("a").
        addE("route").from("d").to("a")
      """
    And the traversal of
      """
      g.match("MATCH (a:airport)-[r:route]-{2}(b:hub)").select("r")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[e[CCC-route->BBB],e[CCC-route->DDD]] |
      | l[e[BBB-route->AAA],e[DDD-route->AAA]] |

  Scenario: g_match_airportXparallelRoute_1X_airport_selectXbX_byXnameX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("airport").property("name", "AAA").as("a").
        addV("airport").property("name", "BBB").as("b").
        addE("route").from("a").to("b").
        addE("route").from("a").to("b")
      """
    And the traversal of
      """
      g.match("MATCH (a:airport {name: 'AAA'})-[:route]->{1}(b:airport)").select("b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | BBB    |
      | BBB    |

  Scenario: g_match_personXknows_starX_person_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->*(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "zero-length paths"

  Scenario: g_match_personXknows_0_2X_person_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->{0,2}(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "zero-length paths"

  Scenario: g_match_personXknows_noMin_2X_person_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->{,2}(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "zero-length paths"

  Scenario: g_match_airportXcode_BGCX_route_1_3X_airportXroute_1_3X_cycle_selectXa_bX_byXcodeX
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'BGC'})-[:route]->{1,3}(b:airport), (b)-[:route]->{1,3}(a)").select("a","b").by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"BGC","b":"BGC"}] |
      | m[{"a":"BGC","b":"CAT"}] |
      | m[{"a":"BGC","b":"VRL"}] |
      | m[{"a":"BGC","b":"VRL"}] |
      | m[{"a":"BGC","b":"VRL"}] |
      | m[{"a":"BGC","b":"VRL"}] |
      | m[{"a":"BGC","b":"VSE"}] |

  Scenario: g_match_airportXcode_BGCX_routeXr1Var_1_3X_airportXrouteXr2Var_1_3X_cycle_selectXr1_r2X
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'BGC'})-[r1:route]->{1,3}(b:airport), (b)-[r2:route]->{1,3}(a)").select("r1","r2").by(__.unfold().map(__.inV().values("code")).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"r1":"l[VRL]","r2":"l[BGC]"}] |
      | m[{"r1":"l[VRL]","r2":"l[VSE,VRL,BGC]"}] |
      | m[{"r1":"l[VRL,BGC]","r2":"l[VRL,BGC]"}] |
      | m[{"r1":"l[VRL,VSE]","r2":"l[VRL,BGC]"}] |
      | m[{"r1":"l[VRL,VSE,CAT]","r2":"l[VSE,VRL,BGC]"}] |
      | m[{"r1":"l[VRL,VSE,VRL]","r2":"l[BGC]"}] |
      | m[{"r1":"l[VRL,VSE,VRL]","r2":"l[VSE,VRL,BGC]"}] |

  Scenario: g_match_airportXparallelRoute_1X_airportXroute_1X_cycle_selectXa_bX_byXnameX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("airport").property("name", "AAA").as("a").
        addV("airport").property("name", "BBB").as("b").
        addV("airport").property("name", "CCC").as("c").
        addE("route").from("a").to("b").
        addE("route").from("b").to("a").
        addE("route").from("b").to("a").
        addE("route").from("b").to("c")
      """
    And the traversal of
      """
      g.match("MATCH (a:airport)-[:route]->{1}(b:airport), (b)-[:route]->{1}(a)").select("a","b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"AAA","b":"BBB"}] |
      | m[{"a":"AAA","b":"BBB"}] |
      | m[{"a":"BBB","b":"AAA"}] |
      | m[{"a":"BBB","b":"AAA"}] |

  Scenario: g_match_anyXrVar_1_3X_selfLoop_selectXrX
    Given the sink graph
    And the traversal of
      """
      g.match("MATCH (a)-[r]->{1,3}(a)").select("r")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[e[loop-self->loop]] |
      | l[e[a-link->a]] |

  Scenario: g_match_airportXcode_ATTX_routeXrVar_1_3X_cycle_selectXrX
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'ATT'})-[r:route]->{1,3}(a)").select("r").by(__.unfold().map(__.inV().values("code")).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[BET,ATT] |
      | l[KUK,NUP,ATT] |

  Scenario: g_match_airportXcode_ATTX_route_airportXcode_KUKX_routeXrVar_1_3X_airport_route_triangle_selectXc_rX
    Given the airroutes graph
    And the traversal of
      """
      g.match("MATCH (a:airport {code: 'ATT'})-[:route]->(b:airport {code: 'KUK'})-[r:route]->{1,3}(c:airport)-[:route]->(a)").select("c","r").by("code").by(__.unfold().map(__.inV().values("code")).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"c":"NUP","r":"l[NUP]"}] |
      | m[{"c":"BET","r":"l[NUP,BET]"}] |
      | m[{"c":"BET","r":"l[NUP,ATT,BET]"}] |

  Scenario: g_match_messageXundirectedLinkXrVarX_selfLoop_selectXa_r_bX_byXnameX_by_byXnameX
    Given the sink graph
    And the traversal of
      """
      g.match("MATCH (a:message)-[r:link]-(b:message)").select("a","r","b").by("name").by().by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"a","r":"e[a-link->b]","b":"b"}] |
      | m[{"a":"b","r":"e[a-link->b]","b":"a"}] |
      | m[{"a":"a","r":"e[a-link->a]","b":"a"}] |

  Scenario: g_match_messageXundirectedLinkXrVar_1_2X_selfLoop_selectXa_b_rX_byXnameX_byXnameX_by
    Given the sink graph
    And the traversal of
      """
      g.match("MATCH (a:message)-[r:link]-{1,2}(b:message)").select("a","b","r").by("name").by("name").by()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"a","b":"b","r":"l[e[a-link->b]]"}] |
      | m[{"a":"b","b":"a","r":"l[e[a-link->b]]"}] |
      | m[{"a":"a","b":"a","r":"l[e[a-link->a]]"}] |
      | m[{"a":"a","b":"b","r":"l[e[a-link->a],e[a-link->b]]"}] |
      | m[{"a":"b","b":"a","r":"l[e[a-link->b],e[a-link->a]]"}] |

  Scenario: g_match_personXknowsXrVar_1_2X_personXcreatedXrVarX_software_selectXrX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[r:knows]->{1,2}(b:person), (b)-[r:created]->(s:software)").select("r")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "a quantified edge variable cannot be reused"

  Scenario: g_match_personXknows_1_2147483647X_person_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->{1,2147483647}(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "exceeds the maximum supported value"
