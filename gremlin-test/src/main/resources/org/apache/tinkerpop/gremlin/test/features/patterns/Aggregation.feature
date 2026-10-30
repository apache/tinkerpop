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
Feature: Step - aggregation

  Scenario: g_totalLongestRunwayLengthAndRunwayCount
    Given the airroutes graph
    And the intent
      """
      Add up the lengths of the longest runway at every airport and the number of runways at every airport, grouped by
      property key.
      """
    And the traversal of
      """
      g.V().hasLabel("airport").properties("runways","longest").group().by(T.key).by(__.value().sum())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"longest":"d[26436124].i","runways":"d[4980].i"}] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_runwayRatioNewMexicoToArizona
    Given the airroutes graph
    And the intent
      """
      Divide the total number of runways at airports in New Mexico (region US-NM) by the total at airports in Arizona
      (region US-AZ). What is the result?
      """
    And the traversal of
      """
      g.V().has("region","US-NM").group().by("code").by(values("runways")).select(Column.values).unfold().sum().aggregate("a").V().has("region","US-AZ").group().by("code").by(values("runways")).select(Column.values).unfold().sum().aggregate("b").project("first","second").by(select("a").unfold()).by(select("b").unfold()).math("first / second")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1.0384615384615385].d |

  Scenario: g_routeCountFromDFW
    Given the airroutes graph
    And the intent
      """
      How many routes leave DFW?
      """
    And the traversal of
      """
      g.V().has("code","DFW").project("dfw","route_count").by().by(outE().count()).select("route_count")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[253].l |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_airportCountPerContinent
    Given the airroutes graph
    And the intent
      """
      How many airports does each continent contain? Return a map for each with the continent's description
      ("continent") and its number of airports ("airports").
      """
    And the traversal of
      """
      g.V().hasLabel("continent").project("continent","airports").by("desc").by(out("contains").hasLabel("airport").count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"continent":"Antarctica","airports":"d[0].l"}] |
      | m[{"continent":"Europe","airports":"d[605].l"}] |
      | m[{"continent":"Africa","airports":"d[321].l"}] |
      | m[{"continent":"North America","airports":"d[989].l"}] |
      | m[{"continent":"South America","airports":"d[313].l"}] |
      | m[{"continent":"Asia","airports":"d[971].l"}] |
      | m[{"continent":"Oceania","airports":"d[305].l"}] |

  Scenario: g_routeDistanceFromLondonMunichParisAndAmongThem
    Given the airroutes graph
    And the intent
      """
      For the airports in the cities named London, Munich and Paris, what is the total distance of all routes leaving
      them, and what is the total distance of just the routes that go from one of those airports to another? Return the
      two totals.
      """
    And the traversal of
      """
      g.V().has("city",P.within("London","Munich","Paris")).aggregate("a").outE().union(values("dist").sum(),filter(inV().where(P.within("a"))).values("dist").sum())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[2739039].i |
      | d[10282].i |

  Scenario: g_airportCountsByRunwayBucket
    Given the airroutes graph
    And the intent
      """
      How many airports have four runways, how many have two or fewer, how many have six or more, and how many airports
      are there in total? Return the counts in one map keyed "four", "low", "high" and "total".
      """
    And the traversal of
      """
      g.V().hasLabel("airport").choose(has("runways",4),groupCount("a").by(constant("four"))).choose(has("runways",P.lte(2)),groupCount("a").by(constant("low"))).choose(has("runways",P.gte(6)),groupCount("a").by(constant("high"))).groupCount("a").by(constant("total")).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"four":"d[53].l","low":"d[3204].l","high":"d[6].l","total":"d[3504].l"}] |

  Scenario: g_airportAndRouteCounts
    Given the airroutes graph
    And the intent
      """
      How many airports are there, and how many routes leave them in total? Return the two counts as one list, the
      airport count first.
      """
    And the traversal of
      """
      g.V().hasLabel("airport").union(count(),out("route").count()).fold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[3504].l,d[50637].l] |

  Scenario: g_airportsWithMoreThan400Routes
    Given the airroutes graph
    And the intent
      """
      Which airports have more than 400 routes, counting incoming and outgoing together? Return their codes.
      """
    And the traversal of
      """
      g.V().hasLabel("airport").where(both("route").count().is(P.gt(400))).values("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ATL |
      | DFW |
      | JFK |
      | LAX |
      | ORD |
      | DEN |
      | EWR |
      | LHR |
      | LGW |
      | CDG |
      | FRA |
      | DXB |
      | PEK |
      | PVG |
      | FCO |
      | AMS |
      | BCN |
      | MAD |
      | VIE |
      | MUC |
      | MAN |
      | STN |
      | BER |
      | DME |
      | IST |

  Scenario: g_routesToUSPerEuropeanAirport
    Given the airroutes graph
    And the intent
      """
      For each European airport (continent code EU) with routes to the United States, how many of its routes go to US
      airports? Return one map from airport code to count.
      """
    And the traversal of
      """
      g.V().has("continent","code","EU").out().as("a").out().has("country","US").select("a").groupCount().by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"TER":"d[1].l","RIX":"d[1].l","BRS":"d[1].l","MXP":"d[6].l","PRG":"d[2].l","BRU":"d[8].l","CDG":"d[25].l","EDI":"d[9].l","STN":"d[4].l","BFS":"d[3].l","OPO":"d[2].l","STR":"d[2].l","ZRH":"d[15].l","DSA":"d[1].l","DBV":"d[1].l","MUC":"d[18].l","TFS":"d[1].l","AMS":"d[22].l","BGO":"d[4].l","BCN":"d[12].l","KEF":"d[26].l","NCE":"d[2].l","MAD":"d[13].l","FCO":"d[13].l","GVA":"d[3].l","VCE":"d[3].l","MAN":"d[14].l","VKO":"d[3].l","ARN":"d[9].l","LGW":"d[14].l","KRK":"d[1].l","BLQ":"d[1].l","ORK":"d[1].l","WAW":"d[5].l","SVO":"d[4].l","DUB":"d[18].l","BUD":"d[3].l","SNN":"d[5].l","BHX":"d[2].l","ORY":"d[4].l","LHR":"d[30].l","KBP":"d[1].l","PDL":"d[3].l","DME":"d[1].l","DUS":"d[11].l","BEG":"d[1].l","OSL":"d[8].l","AGP":"d[1].l","CGN":"d[6].l","IST":"d[11].l","PMI":"d[1].l","HEL":"d[6].l","BER":"d[7].l","VIE":"d[6].l","FRA":"d[28].l","ATH":"d[5].l","HAM":"d[1].l","LIS":"d[9].l","CPH":"d[11].l","NAP":"d[1].l","RZE":"d[1].l","GLA":"d[4].l"}] |

  Scenario: g_totalRunwaysOneFlightFromAUS
    Given the airroutes graph
    And the intent
      """
      What is the total number of runways at all the airports AUS has a direct flight to?
      """
    And the traversal of
      """
      g.V().has("code","AUS").sack(Operator.assign).by(constant(0)).out().sack(Operator.sum).by("runways").sack().sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[306].i |

  Scenario: g_totalLengthOfCityNamesAUSFliesTo
    Given the airroutes graph
    And the intent
      """
      Add up the number of characters in the city name of every airport AUS has a route to. What is the total?
      """
    And the traversal of
      """
      g.V().has("code","AUS").out("route").values("city").length().sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[891].i |

  Scenario: g_airportsWith95To100Routes
    Given the airroutes graph
    And the intent
      """
      Which airports have between 95 and 100 outgoing routes? Return a single map from each airport's code to its number
      of outgoing routes.
      """
    And the traversal of
      """
      g.V().hasLabel("airport").where(__.out().count().is(P.between(95,101))).group().by("code").by(__.out().count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"RIX":"d[96].l","BRS":"d[100].l","DCA":"d[96].l","KBP":"d[100].l","VCE":"d[99].l","GRU":"d[97].l","AYT":"d[100].l","SFB":"d[95].l","AUS":"d[98].l","OPO":"d[95].l"}] |

  Scenario: g_airportCountsByElevationCategory
    Given the airroutes graph
    And the intent
      """
      Count the airports by elevation, categorizing any airport above 5,000 feet as "high", above 3,000 feet as "medium"
      and all others as "low".
      """
    And the traversal of
      """
      g.V().hasLabel("airport").groupCount().by(__.choose(__.values("elev")).option(P.gt(5000),__.constant("high")).option(P.gt(3000),__.constant("medium")).option(Pick.none,__.constant("low")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"high":"d[163].l","medium":"d[214].l","low":"d[3127].l"}] |

  Scenario: g_totalRoutesInAndOutOfAUS
    Given the airroutes graph
    And the intent
      """
      Count the routes into Austin (AUS) and the routes out of it separately, then add the two counts with a math
      expression.
      """
    And the traversal of
      """
      g.V().has("code","AUS").project("in","out").by(__.in("route").count()).by(__.out("route").count()).math("in + out")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[196.0].d |

  Scenario: g_citiesWithMoreThanThreeAirports
    Given the airroutes graph
    And the intent
      """
      Which city names are shared by more than three airports? Return the city names.
      """
    And the traversal of
      """
      g.V().groupCount().by("city").unfold().filter(select(Column.values).is(P.gt(3))).select(Column.keys)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | San Jose |
      | London |
      | Melbourne |
      | Santa Rosa |

  Scenario: g_averageRoutesPerAirport
    Given the airroutes graph
    And the intent
      """
      On average, how many routes, counting both incoming and outgoing, does an airport have?
      """
    And the traversal of
      """
      g.V().hasLabel("airport").local(both("route").count()).mean()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[28.902397260273972].d |

  Scenario: g_standardDeviationOfRunwayCounts
    Given the airroutes graph
    And the intent
      """
      What is the standard deviation of the number of runways across all airports, rounded to three decimal places? Use
      the population standard deviation.
      """
    And the traversal of
      """
      g.V().hasLabel("airport").values("runways").fold().as("runways").mean(Scope.local).as("mean").select("runways").unfold().math("(_-mean)^2").mean().math("floor(sqrt(_) * 1000 + 0.5) / 1000")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.743].d |

  Scenario: g_airportsWithThreeRunways
    Given the airroutes graph
    And the intent
      """
      How many airports have exactly three runways?
      """
    And the traversal of
      """
      g.V().has("runways",P.eq(3)).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[227].l |

  @DataBigDecimal
  Scenario: g_totalPaidToGroceryMerchants
    Given the empty graph
    And the intent
      """
      What is the total amount paid to grocery merchants?
      """
    And the graph initializer of
      """
      g.addV("account").property("holder","Ana Ruiz").as("a").
        addV("account").property("holder","Ben Okafor").as("b").
        addV("merchant").property("name","FreshMart").property("category","grocery").as("m1").
        addV("merchant").property("name","Green Basket").property("category","grocery").as("m2").
        addV("merchant").property("name","FuelUp").property("category","fuel").as("m3").
        addE("payment").from("a").to("m1").property("amount",12.1m).
        addE("payment").from("b").to("m1").property("amount",0.2m).
        addE("payment").from("b").to("m2").property("amount",7.75m).
        addE("payment").from("a").to("m3").property("amount",40m)
      """
    And the traversal of
      """
      g.V().hasLabel("merchant").has("category","grocery").inE("payment").values("amount").sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[20.05].m |

  @DataBigDecimal
  Scenario: g_netTransferFlowPerAccount
    Given the empty graph
    And the intent
      """
      For each account, how much more has it received by transfer than it has sent? Return a map for each account with
      its holder ("holder") and that amount ("net"), negative where it has sent more than it received.
      """
    And the graph initializer of
      """
      g.addV("account").property("holder","Ana Ruiz").as("a").
        addV("account").property("holder","Ben Okafor").as("b").
        addV("account").property("holder","Chen Wei").as("c").
        addE("transfer").from("a").to("b").property("amount",100.1m).
        addE("transfer").from("b").to("c").property("amount",30.2m).
        addE("transfer").from("c").to("a").property("amount",0.3m).
        addE("transfer").from("b").to("a").property("amount",0.1m)
      """
    And the traversal of
      """
      g.withSack(0m).V().hasLabel("account").sack(Operator.assign).by(__.inE("transfer").values("amount").sum()).sack(Operator.minus).by(__.outE("transfer").values("amount").sum()).project("holder","net").by("holder").by(__.sack())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"holder":"Ana Ruiz","net":"d[-99.7].m"}] |
      | m[{"holder":"Ben Okafor","net":"d[69.8].m"}] |
      | m[{"holder":"Chen Wei","net":"d[29.9].m"}] |

  @DataFloat
  Scenario: g_averageTemperaturePerSite
    Given the empty graph
    And the intent
      """
      What is the average of the temperature readings at each site? Return a map from site name to average.
      """
    And the graph initializer of
      """
      g.addV("site").property("name","North Plant").as("n").
        addV("site").property("name","South Plant").as("s").
        addV("device").property("model","TH-200").as("d1").
        addV("device").property("model","TH-201").as("d2").
        addV("device").property("model","TH-300").as("d3").
        addV("sensor").property("kind","temperature").property("unit","C").as("t1").
        addV("sensor").property("kind","humidity").property("unit","%").as("h1").
        addV("sensor").property("kind","temperature").property("unit","C").as("t2").
        addV("sensor").property("kind","temperature").property("unit","C").as("t3").
        addV("reading").property("value",20.5f).as("r1").
        addV("reading").property("value",21.75f).as("r2").
        addV("reading").property("value",55.0f).as("r3").
        addV("reading").property("value",19.25f).as("r4").
        addV("reading").property("value",24.0f).as("r5").
        addV("reading").property("value",23.5f).as("r6").
        addE("hosts").from("n").to("d1").addE("hosts").from("n").to("d2").addE("hosts").from("s").to("d3").
        addE("has").from("d1").to("t1").addE("has").from("d1").to("h1").addE("has").from("d2").to("t2").addE("has").from("d3").to("t3").
        addE("recorded").from("t1").to("r1").addE("recorded").from("t1").to("r2").addE("recorded").from("h1").to("r3").
        addE("recorded").from("t2").to("r4").addE("recorded").from("t3").to("r5").addE("recorded").from("t3").to("r6")
      """
    And the traversal of
      """
      g.V().hasLabel("site").group().by("name").by(__.out("hosts").out("has").has("kind","temperature").out("recorded").values("value").mean())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"North Plant":"d[20.5].d","South Plant":"d[23.75].d"}] |

  @SupportsBinary @DataBinary
  Scenario: g_firmwareBuildsPerModel
    Given the empty graph
    And the intent
      """
      How many different firmware builds are installed across the devices of each model? Return a map from model to
      count.
      """
    And the graph initializer of
      """
      g.addV("device").property("model","TH-200").property("firmware",Binary("AQID")).
        addV("device").property("model","TH-200").property("firmware",Binary("AQID")).
        addV("device").property("model","TH-200").property("firmware",Binary("AQIE")).
        addV("device").property("model","GW-10").property("firmware",Binary("CgsM")).
        addV("device").property("model","GW-10").property("firmware",Binary("CgsM"))
      """
    And the traversal of
      """
      g.V().hasLabel("device").group().by("model").by(__.values("firmware").dedup().count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"TH-200":"d[2].l","GW-10":"d[1].l"}] |

  @AllowMapPropertyValues @DataMap
  Scenario: g_employeeCountPerCity
    Given the empty graph
    And the intent
      """
      How many employees live in each city? Return a map from city to count.
      """
    And the graph initializer of
      """
      g.addV("employee").property("name","Ines Duarte").property("address",["street":"12 Rua Augusta","city":"Lisbon","postcode":"1100-053"]).
        addV("employee").property("name","Kofi Mensah").property("address",["street":"4 Ring Road","city":"Accra","postcode":"GA-184"]).
        addV("employee").property("name","Raj Patel").property("address",["street":"9 Avenida da Liberdade","city":"Lisbon","postcode":"1250-096"]).
        addV("employee").property("name","Mei Tanaka").property("address",["street":"2 Calçada do Combro","city":"Lisbon","postcode":"1200-115"])
      """
    And the traversal of
      """
      g.V().hasLabel("employee").groupCount().by(__.values("address").select("city"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"Lisbon":"d[3].l","Accra":"d[1].l"}] |

  @DataDouble
  Scenario: g_totalAllocationPerProject
    Given the empty graph
    And the intent
      """
      What is the total allocation on each project, adding up the share of their time each employee spends on it? Return
      a map from project name to total.
      """
    And the graph initializer of
      """
      g.addV("project").property("name","Atlas").as("atlas").
        addV("project").property("name","Beacon").as("beacon").
        addV("employee").property("name","Ines Duarte").as("ines").
        addV("employee").property("name","Raj Patel").as("raj").
        addV("employee").property("name","Kofi Mensah").as("kofi").
        addE("worksOn").from("ines").to("atlas").property("allocation",1.0d).
        addE("worksOn").from("raj").to("atlas").property("allocation",0.5d).
        addE("worksOn").from("raj").to("beacon").property("allocation",0.25d).
        addE("worksOn").from("kofi").to("beacon").property("allocation",0.5d)
      """
    And the traversal of
      """
      g.V().hasLabel("project").group().by("name").by(__.inE("worksOn").values("allocation").sum())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"Atlas":"d[1.5].d","Beacon":"d[0.75].d"}] |

  Scenario: g_contactCountPerAgeBand
    Given the empty graph
    And the intent
      """
      Ages were imported partly as numbers and partly as text. How many contacts are in each age band, "under 30", "30
      to 49" and "50 and over"? Return a map from band to count.
      """
    And the graph initializer of
      """
      g.addV("contact").property("name","Alba Reyes").property("age",34).
        addV("contact").property("name","Bram de Vries").property("age","41").
        addV("contact").property("name","Chiara Russo").property("age",28).
        addV("contact").property("name","Esi Owusu").property("age","52").
        addV("contact").property("name","Farid Haddad").property("age","30").
        addV("contact").property("name","Greta Lind").property("age",49)
      """
    And the traversal of
      """
      g.V().hasLabel("contact").groupCount().by(__.values("age").asNumber().choose(__.is(P.lt(30)),__.constant("under 30"),__.choose(__.is(P.lt(50)),__.constant("30 to 49"),__.constant("50 and over"))))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"under 30":"d[1].l","30 to 49":"d[4].l","50 and over":"d[1].l"}] |

  @DataBigDecimal
  Scenario: g_totalScoreOfAllContacts
    Given the empty graph
    And the intent
      """
      Scores were imported as decimal text. What is the exact total of the scores of all contacts?
      """
    And the graph initializer of
      """
      g.addV("contact").property("name","Alba Reyes").property("score","12.50").
        addV("contact").property("name","Bram de Vries").property("score","7.25").
        addV("contact").property("name","Chiara Russo").property("score","0.10").
        addV("contact").property("name","Esi Owusu").property("score","0.20")
      """
    And the traversal of
      """
      g.V().hasLabel("contact").values("score").asNumber(GType.BIGDECIMAL).sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[20.05].m |
