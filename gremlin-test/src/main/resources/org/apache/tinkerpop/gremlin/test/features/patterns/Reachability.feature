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
Feature: Step - reachability

  Scenario: g_countriesReachableFromIrelandExcludingDomestic
    Given the airroutes graph
    And the intent
      """
      Find all the countries that can be flown to from airports in Ireland, excluding routes between airports within
      Ireland, as a sorted list.
      """
    And the traversal of
      """
      g.V().has("airport","country","IE").aggregate("ireland").out().where(P.without("ireland")).values("country").dedup().fold().order(Scope.local)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | l[AE,AT,BE,BG,CA,CH,CN,CY,CZ,DE,DK,ES,ET,FI,FR,GR,HK,HR,HU,IM,IS,IT,JE,LT,LU,LV,MA,MD,MT,NL,NO,PL,PT,QA,RO,RU,SE,SK,TR,UA,UK,US] |

  Scenario: g_airportsTwoFlightsFromLondonEngland
    Given the airroutes graph
    And the intent
      """
      How many distinct airports can be reached from the airports in London, England (region GB-ENG) with exactly two
      flights, not counting the London airports themselves?
      """
    And the traversal of
      """
      g.V().has("region","GB-ENG").has("city","London").aggregate("lon").out().out().dedup().where(P.without("lon")).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[2353].l |

  Scenario: g_airportsOnlyReachableFromAUSWithOneStop
    Given the airroutes graph
    And the intent
      """
      How many airports can be reached from Austin (AUS) with one stop but not with a nonstop flight? Count Austin
      itself, since a one-stop trip can return there.
      """
    And the traversal of
      """
      g.V().has("code","AUS").out().aggregate("nonstop").out().where(P.without("nonstop")).dedup().count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[946].l |

  Scenario: g_directAndTwoFlightJourneysFromAUS
    Given the airroutes graph
    And the intent
      """
      How many routes leave AUS, and how many journeys of exactly two flights start at AUS? Count every two-flight
      journey, including ones that return to AUS. Return a map with the first count under "a" and the second under "b".
      """
    And the traversal of
      """
      g.V().has("code","AUS").sideEffect(out().count().aggregate("a")).out().out().count().as("b").select("a","b").by(unfold()).by()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"d[98].l","b":"d[8354].l"}] |

  Scenario: g_virginiaAirportsReachableFromVirginia
    Given the airroutes graph
    And the intent
      """
      Which airports in Virginia (region US-VA) can be reached on a direct flight from an airport in Virginia? Return
      their codes.
      """
    And the traversal of
      """
      g.V().has("airport","region","US-VA").aggregate("x").by("code").out("route").values("code").fold().intersect(select("x")).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ORF |
      | ROA |
      | CHO |
      | RIC |
      | IAD |
      | SHD |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_cityOfSYDIfAUSFliesThereElseAustin
    Given the airroutes graph
    And the intent
      """
      If AUS has a direct flight to SYD, return the city of SYD; otherwise return the city of AUS.
      """
    And the traversal of
      """
      g.V().has("code","AUS").optional(out().has("code","SYD")).values("city")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Austin |

  Scenario: g_southAmericanAirportsWithDirectFlightsToMIA
    Given the airroutes graph
    And the intent
      """
      Which airports in South America (continent code SA) have a direct flight to MIA? Return a map for each with its
      code ("x") and the code MIA ("y").
      """
    And the traversal of
      """
      g.V().has("continent","code","SA").out().as("x").out().as("y").has("code","MIA").select("x","y").by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"x":"GYE","y":"MIA"}] |
      | m[{"x":"CCS","y":"MIA"}] |
      | m[{"x":"PBM","y":"MIA"}] |
      | m[{"x":"UIO","y":"MIA"}] |
      | m[{"x":"BSB","y":"MIA"}] |
      | m[{"x":"BEL","y":"MIA"}] |
      | m[{"x":"CNF","y":"MIA"}] |
      | m[{"x":"COR","y":"MIA"}] |
      | m[{"x":"REC","y":"MIA"}] |
      | m[{"x":"MAO","y":"MIA"}] |
      | m[{"x":"ASU","y":"MIA"}] |
      | m[{"x":"SSA","y":"MIA"}] |
      | m[{"x":"LPB","y":"MIA"}] |
      | m[{"x":"MDE","y":"MIA"}] |
      | m[{"x":"MAR","y":"MIA"}] |
      | m[{"x":"BLA","y":"MIA"}] |
      | m[{"x":"BAQ","y":"MIA"}] |
      | m[{"x":"CLO","y":"MIA"}] |
      | m[{"x":"CTG","y":"MIA"}] |
      | m[{"x":"BOG","y":"MIA"}] |
      | m[{"x":"GEO","y":"MIA"}] |
      | m[{"x":"FOR","y":"MIA"}] |
      | m[{"x":"MVD","y":"MIA"}] |
      | m[{"x":"VVI","y":"MIA"}] |
      | m[{"x":"SMR","y":"MIA"}] |
      | m[{"x":"EZE","y":"MIA"}] |
      | m[{"x":"GRU","y":"MIA"}] |
      | m[{"x":"SCL","y":"MIA"}] |
      | m[{"x":"POA","y":"MIA"}] |
      | m[{"x":"LIM","y":"MIA"}] |
      | m[{"x":"GIG","y":"MIA"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_flightsIntoAUSFromUKAndOutToMexico
    Given the airroutes graph
    And the intent
      """
      Which airports in the UK (country code UK) have a flight into AUS, and which airports in Mexico (country code MX)
      does AUS fly to? Return each as a path of AUS and the other airport's code.
      """
    And the traversal of
      """
      g.V().has("code","AUS").union(__.in().has("country","UK"),out().has("country","MX")).path().by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[AUS,LGW] |
      | p[AUS,LHR] |
      | p[AUS,CUN] |
      | p[AUS,GDL] |
      | p[AUS,SJD] |
      | p[AUS,PVR] |
      | p[AUS,MEX] |
      | p[AUS,CZM] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_nonStopCitiesFromHawaii
    Given the airroutes graph
    And the intent
      """
      For every airport in Hawaii (region US-HI), list the cities it flies to non-stop, each as a path of the origin
      city and the destination city.
      """
    And the traversal of
      """
      g.V().has("airport","region","US-HI").out().path().by("city")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[Kamuela,Kahului] |
      | p[Hana,Kahului] |
      | p[Lihue,Honolulu] |
      | p[Lihue,Oakland] |
      | p[Lihue,San Diego] |
      | p[Lihue,Denver] |
      | p[Lihue,Los Angeles] |
      | p[Lihue,Kailua/Kona] |
      | p[Lihue,Phoenix] |
      | p[Lihue,Vancouver] |
      | p[Lihue,Kahului] |
      | p[Lihue,San Jose] |
      | p[Lihue,Seattle] |
      | p[Lihue,San Francisco] |
      | p[Hilo,Kahului] |
      | p[Hilo,Honolulu] |
      | p[Hilo,Los Angeles] |
      | p[Honolulu,Portland] |
      | p[Honolulu,Kahului] |
      | p[Honolulu,Tokyo] |
      | p[Honolulu,Seoul] |
      | p[Honolulu,Fukuoka] |
      | p[Honolulu,Osaka] |
      | p[Honolulu,Hagta] |
      | p[Honolulu,Taipei] |
      | p[Honolulu,Kuala Lumpur] |
      | p[Honolulu,Oakland] |
      | p[Honolulu,Brisbane] |
      | p[Honolulu,San Diego] |
      | p[Honolulu,Shanghai] |
      | p[Honolulu,Long Beach] |
      | p[Honolulu,Las Vegas] |
      | p[Honolulu,Denver] |
      | p[Honolulu,Newark] |
      | p[Honolulu,Tokoname] |
      | p[Honolulu,Papeete] |
      | p[Honolulu,Nadi] |
      | p[Honolulu,Hilo] |
      | p[Honolulu,Apia] |
      | p[Honolulu,Pago Pago] |
      | p[Honolulu,Lanai City] |
      | p[Honolulu,Kailua/Kona] |
      | p[Honolulu,Sacramento] |
      | p[Honolulu,Lihue] |
      | p[Honolulu,Sapporo] |
      | p[Honolulu,Kaunakakai] |
      | p[Honolulu,Bellingham] |
      | p[Honolulu,Banana] |
      | p[Honolulu,Lahaina] |
      | p[Honolulu,Majuro Atoll] |
      | p[Honolulu,Atlanta] |
      | p[Honolulu,Anchorage] |
      | p[Honolulu,Austin] |
      | p[Honolulu,Boston] |
      | p[Honolulu,Dallas] |
      | p[Honolulu,Tokyo] |
      | p[Honolulu,Sydney] |
      | p[Honolulu,Toronto] |
      | p[Honolulu,Vancouver] |
      | p[Honolulu,Auckland] |
      | p[Honolulu,Beijing] |
      | p[Honolulu,Singapore] |
      | p[Honolulu,Melbourne] |
      | p[Honolulu,Minneapolis] |
      | p[Honolulu,Chicago] |
      | p[Honolulu,Phoenix] |
      | p[Honolulu,Seattle] |
      | p[Honolulu,San Francisco] |
      | p[Honolulu,San Jose] |
      | p[Honolulu,Washington D.C.] |
      | p[Honolulu,Houston] |
      | p[Honolulu,New York] |
      | p[Honolulu,Los Angeles] |
      | p[Lanai City,Honolulu] |
      | p[Lanai City,Kahului] |
      | p[Kailua/Kona,Lihue] |
      | p[Kailua/Kona,San Francisco] |
      | p[Kailua/Kona,Lanai City] |
      | p[Kailua/Kona,San Jose] |
      | p[Kailua/Kona,Lahaina] |
      | p[Kailua/Kona,San Diego] |
      | p[Kailua/Kona,Bellingham] |
      | p[Kailua/Kona,Denver] |
      | p[Kailua/Kona,Honolulu] |
      | p[Kailua/Kona,Oakland] |
      | p[Kailua/Kona,Vancouver] |
      | p[Kailua/Kona,Dallas] |
      | p[Kailua/Kona,Tokyo] |
      | p[Kailua/Kona,Los Angeles] |
      | p[Kailua/Kona,Kahului] |
      | p[Kailua/Kona,Sacramento] |
      | p[Kailua/Kona,Phoenix] |
      | p[Kailua/Kona,Seattle] |
      | p[Lahaina,Honolulu] |
      | p[Lahaina,Kahului] |
      | p[Lahaina,Kailua/Kona] |
      | p[Lahaina,Kaunakakai] |
      | p[Kaunakakai,Honolulu] |
      | p[Kaunakakai,Kahului] |
      | p[Kaunakakai,Lahaina] |
      | p[Kahului,Kaunakakai] |
      | p[Kahului,Hana] |
      | p[Kahului,Kamuela] |
      | p[Kahului,Los Angeles] |
      | p[Kahului,Vancouver] |
      | p[Kahului,Chicago] |
      | p[Kahului,Portland] |
      | p[Kahului,Sacramento] |
      | p[Kahului,Phoenix] |
      | p[Kahului,Lihue] |
      | p[Kahului,Hilo] |
      | p[Kahului,Lanai City] |
      | p[Kahului,Kailua/Kona] |
      | p[Kahului,Lahaina] |
      | p[Kahului,Seattle] |
      | p[Kahului,San Francisco] |
      | p[Kahului,San Jose] |
      | p[Kahului,San Diego] |
      | p[Kahului,Salt Lake City] |
      | p[Kahului,Denver] |
      | p[Kahului,Honolulu] |
      | p[Kahului,Dallas] |
      | p[Kahului,Oakland] |

  Scenario: g_accountsReachedByMoneyFromFarah
    Given the empty graph
    And the intent
      """
      Which accounts can money sent by Farah Kaur reach through one or more transfers? Return the holders.
      """
    And the graph initializer of
      """
      g.addV("account").property("holder","Ana Ruiz").as("a").
        addV("account").property("holder","Ben Okafor").as("b").
        addV("account").property("holder","Chen Wei").as("c").
        addV("account").property("holder","Dara Novak").as("d").
        addV("account").property("holder","Eli Haddad").as("e").
        addV("account").property("holder","Farah Kaur").as("f").
        addE("transfer").from("f").to("a").property("amount",60m).
        addE("transfer").from("a").to("b").property("amount",500m).
        addE("transfer").from("b").to("c").property("amount",480m).
        addE("transfer").from("c").to("a").property("amount",470m).
        addE("transfer").from("d").to("e").property("amount",90m).
        addE("transfer").from("d").to("f").property("amount",20m)
      """
    And the traversal of
      """
      g.V().has("account","holder","Farah Kaur").repeat(__.out("transfer").simplePath()).emit().values("holder").dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ana Ruiz |
      | Ben Okafor |
      | Chen Wei |

  Scenario: g_gatewayOfEachDevice
    Given the empty graph
    And the intent
      """
      Devices send their data along uplinks to other devices until it reaches a gateway, which is a device with no
      uplink of its own. For each device that is not a gateway, which gateway does its data reach? Return a map for each
      with the model of the device ("device") and the model of the gateway ("gateway").
      """
    And the graph initializer of
      """
      g.addV("device").property("model","TH-200").as("d1").
        addV("device").property("model","RP-1").as("d2").
        addV("device").property("model","GW-10").as("g1").
        addV("device").property("model","TH-300").as("d3").
        addV("device").property("model","PR-5").as("d4").
        addV("device").property("model","GW-20").as("g2").
        addE("uplinksTo").from("d1").to("d2").property("latency",1.5d).
        addE("uplinksTo").from("d2").to("g1").property("latency",2.25d).
        addE("uplinksTo").from("d3").to("g1").property("latency",4.0d).
        addE("uplinksTo").from("d4").to("g2").property("latency",0.75d)
      """
    And the traversal of
      """
      g.V().hasLabel("device").where(__.out("uplinksTo")).project("device","gateway").by("model").by(__.repeat(__.out("uplinksTo")).until(__.not(__.out("uplinksTo"))).values("model"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"device":"TH-200","gateway":"GW-10"}] |
      | m[{"device":"RP-1","gateway":"GW-10"}] |
      | m[{"device":"TH-300","gateway":"GW-10"}] |
      | m[{"device":"PR-5","gateway":"GW-20"}] |

  Scenario: g_everyoneWhoWorksUnderLena
    Given the empty graph
    And the intent
      """
      Who works under Lena Vogel, either reporting to her directly or through other managers? Return their names.
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
      g.V().has("employee","name","Lena Vogel").repeat(__.in("reportsTo")).emit().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Raj Patel |
      | Mei Tanaka |
