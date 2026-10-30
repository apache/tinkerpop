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
Feature: Step - ranking

  Scenario: g_tenAirportsWithMostIncomingRoutes
    Given the airroutes graph
    And the intent
      """
      Which ten airports have the most incoming routes? Return a map for each with its code ("a") and its number of
      incoming routes ("b").
      """
    And the traversal of
      """
      g.V().hasLabel("airport").order().by(inE("route").count(),Order.desc).limit(10).project("a","b").by("code").by(inE("route").count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"FRA","b":"d[310].l"}] |
      | m[{"a":"IST","b":"d[309].l"}] |
      | m[{"a":"CDG","b":"d[294].l"}] |
      | m[{"a":"AMS","b":"d[285].l"}] |
      | m[{"a":"MUC","b":"d[271].l"}] |
      | m[{"a":"ORD","b":"d[264].l"}] |
      | m[{"a":"DFW","b":"d[253].l"}] |
      | m[{"a":"PEK","b":"d[249].l"}] |
      | m[{"a":"DXB","b":"d[248].l"}] |
      | m[{"a":"ATL","b":"d[242].l"}] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_longestRoutesIncludingTies
    Given the airroutes graph
    And the intent
      """
      Which route is the longest in the graph? Include every route that ties for the longest distance, and return a map
      for each with the code of the departure airport ("from"), the code of the arrival airport ("to") and the distance
      ("dist").
      """
    And the traversal of
      """
      g.E().hasLabel("route").order().by("dist",Order.desc).local(aggregate("d").by("dist")).filter(values("dist").as("cd").select("d").by(limit(Scope.local,1).unfold()).as("md").where("cd",P.eq("md"))).project("from","to","dist").by(outV().values("code")).by(inV().values("code")).by("dist")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"from":"SIN","to":"JFK","dist":"d[9526].i"}] |
      | m[{"from":"JFK","to":"SIN","dist":"d[9526].i"}] |

  Scenario: g_busiestSingleRunwayAirportsAndLGW
    Given the airroutes graph
    And the intent
      """
      Among the airports with a single runway, together with LGW, which nine have the most outgoing routes? Return a map
      for each with its code ("apt"), its city ("city") and its number of outgoing routes ("routes").
      """
    And the traversal of
      """
      g.V().or(has("airport","runways",1),has("code","LGW")).order().by(out().count(),Order.desc).limit(9).project("apt","city","routes").by("code").by("city").by(out().count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"apt":"LGW","city":"London","routes":"d[232].l"}] |
      | m[{"apt":"STN","city":"London","routes":"d[211].l"}] |
      | m[{"apt":"LIS","city":"Lisbon","routes":"d[139].l"}] |
      | m[{"apt":"CTU","city":"Chengdu","routes":"d[139].l"}] |
      | m[{"apt":"LTN","city":"London","routes":"d[130].l"}] |
      | m[{"apt":"BHX","city":"Birmingham","routes":"d[130].l"}] |
      | m[{"apt":"SZX","city":"Shenzhen","routes":"d[129].l"}] |
      | m[{"apt":"CKG","city":"Chongqing","routes":"d[122].l"}] |
      | m[{"apt":"STR","city":"Stuttgart","routes":"d[121].l"}] |

  Scenario: g_mostOutgoingRoutesFromAnyAirport
    Given the airroutes graph
    And the intent
      """
      What is the largest number of outgoing routes that any one airport has?
      """
    And the traversal of
      """
      g.V().hasLabel("airport").local(out("route").count()).max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[310].l |

  Scenario: g_AUSDestinationsWithMostRunways
    Given the airroutes graph
    And the intent
      """
      Which six airports that AUS flies to have the most runways? Return a map for each with its code ("ap") and its
      number of runways ("rw").
      """
    And the traversal of
      """
      g.V().has("code","AUS").out().project("ap","rw").by("code").by("runways").order().by(select("rw"),Order.desc).limit(6)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ap":"DFW","rw":"d[7].i"}] |
      | m[{"ap":"ORD","rw":"d[7].i"}] |
      | m[{"ap":"BOS","rw":"d[6].i"}] |
      | m[{"ap":"DEN","rw":"d[6].i"}] |
      | m[{"ap":"DTW","rw":"d[6].i"}] |
      | m[{"ap":"AMS","rw":"d[6].i"}] |

  @DataBigDecimal
  Scenario: g_largestTransfer
    Given the empty graph
    And the intent
      """
      Which account made the single largest transfer, and how much was it? Return a map with the holder of the sending
      account ("holder") and the amount ("amount").
      """
    And the graph initializer of
      """
      g.addV("account").property("holder","Ana Ruiz").as("a").
        addV("account").property("holder","Ben Okafor").as("b").
        addV("account").property("holder","Chen Wei").as("c").
        addE("transfer").from("a").to("b").property("amount",250m).
        addE("transfer").from("b").to("c").property("amount",1250.5m).
        addE("transfer").from("c").to("a").property("amount",980m).
        addE("transfer").from("a").to("c").property("amount",1250.49m)
      """
    And the traversal of
      """
      g.E().hasLabel("transfer").order().by("amount",Order.desc).limit(1).project("holder","amount").by(__.outV().values("holder")).by("amount")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"holder":"Ben Okafor","amount":"d[1250.5].m"}] |

  @AllowDateTimePropertyValues @DataDateTime @DataFloat
  Scenario: g_latestReadingOfEachTemperatureSensor
    Given the empty graph
    And the intent
      """
      What is the most recent reading of each temperature sensor? Return a map for each sensor with the model of its
      device ("model") and the value of that reading ("value").
      """
    And the graph initializer of
      """
      g.addV("device").property("model","TH-200").as("d1").
        addV("device").property("model","TH-300").as("d2").
        addV("sensor").property("kind","temperature").as("t1").
        addV("sensor").property("kind","temperature").as("t2").
        addV("sensor").property("kind","humidity").as("h1").
        addV("reading").property("value",20.5f).property("at",datetime("2024-05-01T08:00:00Z")).as("r1").
        addV("reading").property("value",22.0f).property("at",datetime("2024-05-01T12:00:00Z")).as("r2").
        addV("reading").property("value",21.25f).property("at",datetime("2024-05-01T10:00:00Z")).as("r3").
        addV("reading").property("value",18.5f).property("at",datetime("2024-05-02T07:30:00Z")).as("r4").
        addV("reading").property("value",17.75f).property("at",datetime("2024-04-30T23:00:00Z")).as("r5").
        addV("reading").property("value",61.0f).property("at",datetime("2024-05-03T00:00:00Z")).as("r6").
        addE("has").from("d1").to("t1").addE("has").from("d2").to("t2").addE("has").from("d2").to("h1").
        addE("recorded").from("t1").to("r1").addE("recorded").from("t1").to("r2").addE("recorded").from("t1").to("r3").
        addE("recorded").from("t2").to("r4").addE("recorded").from("t2").to("r5").addE("recorded").from("h1").to("r6")
      """
    And the traversal of
      """
      g.V().hasLabel("sensor").has("kind","temperature").project("model","value").by(__.in("has").values("model")).by(__.out("recorded").order().by("at",Order.desc).limit(1).values("value"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"model":"TH-200","value":"d[22.0].f"}] |
      | m[{"model":"TH-300","value":"d[18.5].f"}] |

  @DataShort
  Scenario: g_mostSensorErrorsPerSite
    Given the empty graph
    And the intent
      """
      What is the highest error count logged by any sensor at each site? Return a map from site name to that count.
      """
    And the graph initializer of
      """
      g.addV("site").property("name","North Plant").as("n").
        addV("site").property("name","South Plant").as("s").
        addV("device").property("model","TH-200").as("d1").
        addV("device").property("model","PR-5").as("d2").
        addV("device").property("model","TH-300").as("d3").
        addV("sensor").property("kind","temperature").property("errors",12s).as("s1").
        addV("sensor").property("kind","pressure").property("errors",340s).as("s2").
        addV("sensor").property("kind","humidity").property("errors",0s).as("s3").
        addV("sensor").property("kind","temperature").property("errors",57s).as("s4").
        addE("hosts").from("n").to("d1").addE("hosts").from("n").to("d2").addE("hosts").from("s").to("d3").
        addE("has").from("d1").to("s1").addE("has").from("d2").to("s2").addE("has").from("d3").to("s3").addE("has").from("d3").to("s4")
      """
    And the traversal of
      """
      g.V().hasLabel("site").group().by("name").by(__.out("hosts").out("has").values("errors").max())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"North Plant":"d[340].s","South Plant":"d[57].s"}] |
