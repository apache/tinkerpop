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

@StepClassIntegrated
Feature: Step - paths

  @GraphComputerVerificationReferenceOnly @InsertionOrderingRequired
  Scenario: g_V_shortestpath
    Given the modern graph
    And the traversal of
      """
      g.V().as("v").both().as("v").
        project("src", "tgt", "p").
          by(__.select(first, "v")).
          by(__.select(last, "v")).
          by(__.select(Pop.all, "v")).as("triple").
        group("x").
          by(__.select("src", "tgt")).
          by(__.select("p").fold()).select("tgt").barrier().
        repeat(__.both().as("v").
                project("src", "tgt", "p").
                  by(__.select(first, "v")).
                  by(__.select(last, "v")).
                  by(__.select(Pop.all, "v")).as("t").
                filter(__.select(Pop.all, "p").count(local).as("l").
                       select(Pop.last, "t").select(Pop.all, "p").dedup(Scope.local).count(Scope.local).where(P.eq("l"))).
                where("src", P.neq("tgt")).
                select(Pop.last, "t").
                not(__.select(Pop.all, "p").as("p").count(local).as("l").
                    select(Pop.all, "x").unfold().filter(select(keys).where(P.eq("t")).by(__.select("src", "tgt"))).
                    filter(__.select(Column.values).unfold().or(__.count(Scope.local).where(P.lt("l")), __.where(P.eq("p"))))).
                barrier().
                group("x").
                  by(__.select("src", "tgt")).
                  by(__.select(Pop.all, "p").fold()).select("tgt").barrier()).
        cap("x").select(Column.values).unfold().unfold().map(__.unfold().values("name").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[josh,marko,vadas]|
      | l[ripple,josh,lop]|
      | l[josh,lop]|
      | l[peter,lop,marko]|
      | l[ripple,josh,marko]|
      | l[josh,marko]|
      | l[marko,lop]|
      | l[lop,marko]|
      | l[josh,lop,peter]|
      | l[peter,lop,josh]|
      | l[vadas,marko]|
      | l[ripple,josh]|
      | l[josh,ripple]|
      | l[peter,lop]|
      | l[vadas,marko,josh]|
      | l[lop,josh,ripple]|
      | l[marko,josh]|
      | l[lop,marko,vadas]|
      | l[marko,josh,ripple]|
      | l[marko,vadas]|
      | l[vadas,marko,lop]|
      | l[lop,peter]|
      | l[lop,josh]|
      | l[marko,lop,peter]|

  @GraphComputerVerificationStarGraphExceeded @WithSeedStrategy @InsertionOrderingRequired
  Scenario: g_V_playlist_paths
    Given the grateful graph
    And the traversal of
      """
      g.withStrategies(SeedStrategy(seed: 99999)).V().has("name", "Bob_Dylan").in("sungBy").as("a").
        repeat(__.out().order().by(Order.shuffle).simplePath().from("a")).
          until(__.out("writtenBy").has("name", "Johnny_Cash")).
        limit(1).as("b").
        repeat(__.out().order().by(Order.shuffle).as("c").simplePath().from("b").to("c")).
          until(__.out("sungBy").has("name", "Grateful_Dead")).
        limit(1).
        path().from("a").unfold().
        project("song", "artists").
          by("name").
          by(__.coalesce(__.out("sungBy", "writtenBy").dedup().values("name"),
                         __.constant("Unknown")).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"song":"I WANT YOU","artists":["Bob_Dylan"]}] |
      | m[{"song":"BABY BLUE","artists":["Unknown"]}] |
      | m[{"song":"CANDYMAN","artists":["Garcia","Hunter"]}] |
      | m[{"song":"BIG RIVER","artists":["Weir","Johnny_Cash"]}] |
      | m[{"song":"TERRAPIN STATION","artists":["Garcia","Hunter"]}] |
      | m[{"song":"DRUMS","artists":["Grateful_Dead"]}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_weightedShortestPathsBetweenAllPairs
    Given the modern graph
    And the intent
      """
      For every pair of vertices, find the path between them with the lowest total edge weight, following edges in
      either direction. Return each pair once, as a map of the two vertex names ("from" and "to"), the path as a list of
      alternating vertex names and edge weights ("path"), and the total weight ("weight").
      """
    And the traversal of
      """
      g.withSack(0.0).V().as("from").repeat(bothE().sack(Operator.sum).by("weight").otherV().where(P.neq("from")).as("to").group("m").by(select("from","to")).by(sack().min()).filter(project("s","x").by(sack()).by(select("m").select(select("from","to"))).where("s", P.eq("x"))).group("p").by(select("from", "to")).by(map(union(path().by("name").by("weight"), sack()).fold())).barrier()).cap("p").unfold().order().by(select(Column.keys).select("from").id()).by(select(Column.keys).select("to").id()).barrier().dedup().by(select(Column.keys).select(Column.values).order(Scope.local).by(T.id)).project("from","to","path","weight").by(select(Column.keys).select("from").values("name")).by(select(Column.keys).select("to").values("name")).by(select(Column.values).unfold().limit(1).unfold().fold()).by(select(Column.values).unfold().tail(1))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"from":"marko","to":"vadas","path":"l[marko,d[0.5].d,vadas]","weight":"d[0.5].d"}] |
      | m[{"from":"marko","to":"lop","path":"l[marko,d[0.4].d,lop]","weight":"d[0.4].d"}] |
      | m[{"from":"marko","to":"josh","path":"l[marko,d[0.4].d,lop,d[0.4].d,josh]","weight":"d[0.8].d"}] |
      | m[{"from":"marko","to":"ripple","path":"l[marko,d[0.4].d,lop,d[0.4].d,josh,d[1.0].d,ripple]","weight":"d[1.8].d"}] |
      | m[{"from":"marko","to":"peter","path":"l[marko,d[0.4].d,lop,d[0.2].d,peter]","weight":"d[0.6000000000000001].d"}] |
      | m[{"from":"vadas","to":"lop","path":"l[vadas,d[0.5].d,marko,d[0.4].d,lop]","weight":"d[0.9].d"}] |
      | m[{"from":"vadas","to":"josh","path":"l[vadas,d[0.5].d,marko,d[0.4].d,lop,d[0.4].d,josh]","weight":"d[1.3].d"}] |
      | m[{"from":"vadas","to":"ripple","path":"l[vadas,d[0.5].d,marko,d[0.4].d,lop,d[0.4].d,josh,d[1.0].d,ripple]","weight":"d[2.3].d"}] |
      | m[{"from":"vadas","to":"peter","path":"l[vadas,d[0.5].d,marko,d[0.4].d,lop,d[0.2].d,peter]","weight":"d[1.1].d"}] |
      | m[{"from":"lop","to":"josh","path":"l[lop,d[0.4].d,josh]","weight":"d[0.4].d"}] |
      | m[{"from":"lop","to":"ripple","path":"l[lop,d[0.4].d,josh,d[1.0].d,ripple]","weight":"d[1.4].d"}] |
      | m[{"from":"lop","to":"peter","path":"l[lop,d[0.2].d,peter]","weight":"d[0.2].d"}] |
      | m[{"from":"josh","to":"ripple","path":"l[josh,d[1.0].d,ripple]","weight":"d[1.0].d"}] |
      | m[{"from":"josh","to":"peter","path":"l[josh,d[0.4].d,lop,d[0.2].d,peter]","weight":"d[0.6000000000000001].d"}] |
      | m[{"from":"ripple","to":"peter","path":"l[ripple,d[1.0].d,josh,d[0.4].d,lop,d[0.2].d,peter]","weight":"d[1.5999999999999999].d"}] |

  Scenario: g_cyclesInEitherDirection
    Given the modern graph
    And the intent
      """
      Find the cycles in the graph, following edges in either direction. Return each cycle once, as the names of the
      vertices on it in alphabetical order.
      """
    And the traversal of
      """
      g.V().as("a").repeat(both().simplePath()).emit(loops().is(P.gt(1))).both().where(P.eq("a")).path().dedup().by(unfold().order().by(T.id).dedup().fold()).map(unfold().dedup().values("name").order().fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[josh,lop,marko] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_pathsFromMarkoToJavaWithMeanWeight
    Given the modern graph
    And the intent
      """
      For each path from marko that follows outgoing edges until it reaches software written in java, return the names
      of the vertices along it ("names") and the mean weight of its edges ("meanWeight").
      """
    And the traversal of
      """
      g.V().has("name","marko").as("v").repeat(outE().as("e").inV().as("v")).until(has("lang","java")).project("names","meanWeight").by(select(Pop.all,"v").unfold().values("name").fold()).by(select(Pop.all,"e").unfold().values("weight").mean())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"names":"l[marko,lop]","meanWeight":"d[0.4].d"}] |
      | m[{"names":"l[marko,josh,ripple]","meanWeight":"d[1.0].d"}] |
      | m[{"names":"l[marko,josh,lop]","meanWeight":"d[0.7].d"}] |

  Scenario: g_fewestFlightsFromAUSToYPOAvoidingYYZ
    Given the airroutes graph
    And the intent
      """
      What is the fewest number of flights needed to get from AUS to YPO without passing through YYZ?
      """
    And the traversal of
      """
      g.V().has("code","AUS").repeat(out().has("code",P.without("YYZ")).dedup()).until(has("code","YPO")).path().count(Scope.local).math("_ - 1")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[8.0].d |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_routesAmongLondonMunichParisAirports
    Given the airroutes graph
    And the intent
      """
      Which direct routes run between airports in the cities named London, Munich and Paris? Return each as a path of
      the two airport codes.
      """
    And the traversal of
      """
      g.V().has("city",P.within("London","Munich","Paris")).aggregate("a").out().where(P.within("a")).path().by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[LTN,CDG] |
      | p[LTN,MUC] |
      | p[LTN,ORY] |
      | p[LHR,CDG] |
      | p[LHR,MUC] |
      | p[LHR,ORY] |
      | p[LGW,MUC] |
      | p[LGW,CDG] |
      | p[CDG,LHR] |
      | p[CDG,LGW] |
      | p[CDG,LCY] |
      | p[CDG,MUC] |
      | p[CDG,LTN] |
      | p[MUC,ORY] |
      | p[MUC,STN] |
      | p[MUC,LCY] |
      | p[MUC,CDG] |
      | p[MUC,LHR] |
      | p[MUC,LGW] |
      | p[MUC,LTN] |
      | p[LCY,MUC] |
      | p[LCY,ORY] |
      | p[LCY,CDG] |
      | p[STN,MUC] |
      | p[ORY,LHR] |
      | p[ORY,LCY] |
      | p[ORY,MUC] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_oneStopRoutesAUSToLHRWithDistances
    Given the airroutes graph
    And the intent
      """
      Which journeys with exactly one stop go from AUS to LHR? Return each as a path of the airport codes with the
      distance of each flight between them.
      """
    And the traversal of
      """
      g.V().has("airport","code","AUS").outE().inV().outE().inV().has("code","LHR").path().by("code").by("dist")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[AUS,d[1030].i,CLT,d[3980].i,LHR] |
      | p[AUS,d[1284].i,NAS,d[4332].i,LHR] |
      | p[AUS,d[809].i,ATL,d[4198].i,LHR] |
      | p[AUS,d[755].i,BNA,d[4168].i,LHR] |
      | p[AUS,d[1690].i,BOS,d[3254].i,LHR] |
      | p[AUS,d[1339].i,BWI,d[3622].i,LHR] |
      | p[AUS,d[190].i,DFW,d[4736].i,LHR] |
      | p[AUS,d[1230].i,LAX,d[5439].i,LHR] |
      | p[AUS,d[1101].i,MIA,d[4414].i,LHR] |
      | p[AUS,d[1040].i,MSP,d[4001].i,LHR] |
      | p[AUS,d[973].i,ORD,d[3939].i,LHR] |
      | p[AUS,d[866].i,PHX,d[5255].i,LHR] |
      | p[AUS,d[1294].i,IAD,d[3665].i,LHR] |
      | p[AUS,d[142].i,IAH,d[4820].i,LHR] |
      | p[AUS,d[1520].i,JFK,d[3440].i,LHR] |
      | p[AUS,d[1160].i,SAN,d[5469].i,LHR] |
      | p[AUS,d[1080].i,SLC,d[4850].i,LHR] |
      | p[AUS,d[1080].i,LAS,d[5213].i,LHR] |
      | p[AUS,d[1159].i,RDU,d[3860].i,LHR] |
      | p[AUS,d[1768].i,SEA,d[4783].i,LHR] |
      | p[AUS,d[1500].i,SFO,d[5350].i,LHR] |
      | p[AUS,d[1476].i,SJC,d[5352].i,LHR] |
      | p[AUS,d[768].i,DEN,d[4655].i,LHR] |
      | p[AUS,d[444].i,MSY,d[4616].i,LHR] |
      | p[AUS,d[1500].i,EWR,d[3453].i,LHR] |
      | p[AUS,d[1430].i,PHL,d[3533].i,LHR] |
      | p[AUS,d[1140].i,DTW,d[3753].i,LHR] |
      | p[AUS,d[1357].i,YYZ,d[3544].i,LHR] |
      | p[AUS,d[5294].i,FRA,d[406].i,LHR] |
      | p[AUS,d[5074].i,AMS,d[230].i,LHR] |
      | p[AUS,d[1671].i,YYC,d[4356].i,LHR] |
      | p[AUS,d[748].i,MEX,d[5529].i,LHR] |
      | p[AUS,d[1209].i,PIT,d[3707].i,LHR] |
      | p[AUS,d[1712].i,PDX,d[4896].i,LHR] |
      | p[AUS,d[1053].i,CHS,d[4053].i,LHR] |
      | p[AUS,d[1869].i,YVR,d[4706].i,LHR] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_routesFromAUSToLASViaTexasHubs
    Given the airroutes graph
    And the intent
      """
      Find routes between Austin (AUS) and Las Vegas (LAS) that have a plane change in Dallas (DFW, DAL), Houston (IAH,
      HOU) or San Antonio (SAT), returning the airport codes along each route.
      """
    And the traversal of
      """
      g.V().has("airport","code","AUS").out().has("code",P.within("DFW","DAL","IAH","HOU","SAT")).out().has("code","LAS").path().by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[AUS,DAL,LAS] |
      | p[AUS,DFW,LAS] |
      | p[AUS,IAH,LAS] |
      | p[AUS,SAT,LAS] |
      | p[AUS,HOU,LAS] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_oneStopRoutesFromAUSToSYD
    Given the airroutes graph
    And the intent
      """
      Find the routes from Austin (AUS) to Sydney (SYD) that make exactly one stop, returning the airport codes along
      each route.
      """
    And the traversal of
      """
      g.V().has("code","AUS").repeat(__.out()).times(2).has("code","SYD").path().by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[AUS,DFW,SYD] |
      | p[AUS,IAH,SYD] |
      | p[AUS,LAX,SYD] |
      | p[AUS,SFO,SYD] |
      | p[AUS,HNL,SYD] |
      | p[AUS,YVR,SYD] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_oneStopAUSToSYDAvoidingDFWAndLAX
    Given the airroutes graph
    And the intent
      """
      Which journeys with exactly one stop go from AUS to SYD without stopping at DFW or LAX? Return each as a path of
      airport codes.
      """
    And the traversal of
      """
      g.V().has("airport","code","AUS").out().and(has("code",P.neq("DFW")),has("code",P.neq("LAX"))).out().has("code","SYD").path().by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[AUS,IAH,SYD] |
      | p[AUS,SFO,SYD] |
      | p[AUS,HNL,SYD] |
      | p[AUS,YVR,SYD] |

  Scenario: g_stopsBetweenBNEAndAUS
    Given the airroutes graph
    And the intent
      """
      At which airports can a traveller change planes to get from BNE to AUS with exactly one stop? Return their codes.
      """
    And the traversal of
      """
      g.V().has("code","BNE").out().as("stop").out().has("code","AUS").select("stop").values("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | YVR |
      | LAX |
      | HNL |
      | SFO |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_oneStopJourneysFromLondonViaChosenCities
    Given the airroutes graph
    And the intent
      """
      From the airports in London, England (region GB-ENG), find the journeys with one stop that go via Berlin to
      Lisbon, via Paris to Barcelona, or via Edinburgh to Rome. Return each as a path of the airport codes with the
      distance of each flight between them.
      """
    And the traversal of
      """
      g.V().has("city","London").has("region","GB-ENG").union(outE().inV().has("city","Berlin").outE("route").inV().has("city","Lisbon").path().by("code").by("dist").by("code").by("dist"),outE().inV().has("city","Paris").outE("route").inV().has("city","Barcelona").path().by("code").by("dist").by("code").by("dist"),outE().inV().has("city","Edinburgh").outE("route").inV().has("city","Rome").path().by("code").by("dist").by("code").by("dist"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[LTN,d[589].i,BER,d[1432].i,LIS] |
      | p[LTN,d[236].i,CDG,d[533].i,BCN] |
      | p[LTN,d[248].i,ORY,d[513].i,BCN] |
      | p[LHR,d[598].i,BER,d[1432].i,LIS] |
      | p[LHR,d[216].i,CDG,d[533].i,BCN] |
      | p[LHR,d[227].i,ORY,d[513].i,BCN] |
      | p[LGW,d[592].i,BER,d[1432].i,LIS] |
      | p[LGW,d[191].i,CDG,d[533].i,BCN] |
      | p[LCY,d[217].i,ORY,d[513].i,BCN] |
      | p[LCY,d[204].i,CDG,d[533].i,BCN] |
      | p[STN,d[564].i,BER,d[1432].i,LIS] |

  Scenario: g_accountsInTransferRings
    Given the empty graph
    And the intent
      """
      Which accounts are part of a ring of transfers, where money an account sends comes back to it within three
      transfers? Return the holders.
      """
    And the graph initializer of
      """
      g.addV("account").property("holder","Ana Ruiz").as("a").
        addV("account").property("holder","Ben Okafor").as("b").
        addV("account").property("holder","Chen Wei").as("c").
        addV("account").property("holder","Dara Novak").as("d").
        addV("account").property("holder","Eli Haddad").as("e").
        addV("account").property("holder","Farah Kaur").as("f").
        addE("transfer").from("a").to("b").property("amount",500m).
        addE("transfer").from("b").to("c").property("amount",480m).
        addE("transfer").from("c").to("a").property("amount",470m).
        addE("transfer").from("d").to("e").property("amount",90m).
        addE("transfer").from("e").to("d").property("amount",85m).
        addE("transfer").from("f").to("a").property("amount",60m).
        addE("transfer").from("b").to("d").property("amount",15m)
      """
    And the traversal of
      """
      g.V().hasLabel("account").as("a").where(__.repeat(__.out("transfer").simplePath()).emit().times(2).out("transfer").where(P.eq("a"))).values("holder")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ana Ruiz |
      | Ben Okafor |
      | Chen Wei |
      | Dara Novak |
      | Eli Haddad |

  @DataDouble
  Scenario: g_uplinkLatencyToGatewayFromTH200
    Given the empty graph
    And the intent
      """
      Devices send their data along uplinks to other devices until it reaches a gateway, which is a device with no
      uplink of its own. What is the total latency of the uplinks the data of the TH-200 device travels along to reach
      its gateway?
      """
    And the graph initializer of
      """
      g.addV("device").property("model","TH-200").as("d1").
        addV("device").property("model","RP-1").as("d2").
        addV("device").property("model","GW-10").as("g1").
        addV("device").property("model","TH-300").as("d3").
        addE("uplinksTo").from("d1").to("d2").property("latency",1.5d).
        addE("uplinksTo").from("d2").to("g1").property("latency",2.25d).
        addE("uplinksTo").from("d3").to("g1").property("latency",4.0d)
      """
    And the traversal of
      """
      g.withSack(0.0d).V().has("device","model","TH-200").repeat(__.outE("uplinksTo").sack(Operator.sum).by("latency").inV()).until(__.not(__.out("uplinksTo"))).sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3.75].d |

  Scenario: g_chainOfCommandFromMei
    Given the empty graph
    And the intent
      """
      Follow the chain of command from Mei Tanaka up to the head of the organization, the one employee who reports to no
      one. Return it as a path of names, starting with Mei.
      """
    And the graph initializer of
      """
      g.addV("employee").property("name","Grace Hopkins").as("grace").
        addV("employee").property("name","Omar Said").as("omar").
        addV("employee").property("name","Ines Duarte").as("ines").
        addV("employee").property("name","Kofi Mensah").as("kofi").
        addV("employee").property("name","Lena Vogel").as("lena").
        addV("employee").property("name","Raj Patel").as("raj").
        addV("employee").property("name","Mei Tanaka").as("mei").
        addE("reportsTo").from("omar").to("grace").
        addE("reportsTo").from("lena").to("grace").
        addE("reportsTo").from("ines").to("omar").
        addE("reportsTo").from("kofi").to("omar").
        addE("reportsTo").from("raj").to("lena").
        addE("reportsTo").from("mei").to("raj")
      """
    And the traversal of
      """
      g.V().has("employee","name","Mei Tanaka").repeat(__.out("reportsTo")).until(__.not(__.out("reportsTo"))).path().by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[Mei Tanaka,Raj Patel,Lena Vogel,Grace Hopkins] |
