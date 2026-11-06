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
Feature: Step - lookup

  Scenario: g_codeAndRegionOfAirportsInTXLAAZOK
    Given the airroutes graph
    And the intent
      """
      List the airports in Texas, Louisiana, Arizona and Oklahoma (regions US-TX, US-LA, US-AZ and US-OK), each as a map
      of its code and its region.
      """
    And the traversal of
      """
      g.V().hasLabel("airport").has("region",P.within("US-TX","US-LA","US-AZ","US-OK")).valueMap("code","region").by(unfold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"code":"AUS","region":"US-TX"}] |
      | m[{"code":"DFW","region":"US-TX"}] |
      | m[{"code":"AZA","region":"US-AZ"}] |
      | m[{"code":"SHV","region":"US-LA"}] |
      | m[{"code":"LBB","region":"US-TX"}] |
      | m[{"code":"HRL","region":"US-TX"}] |
      | m[{"code":"MAF","region":"US-TX"}] |
      | m[{"code":"VCT","region":"US-TX"}] |
      | m[{"code":"SOW","region":"US-AZ"}] |
      | m[{"code":"FLG","region":"US-AZ"}] |
      | m[{"code":"CRP","region":"US-TX"}] |
      | m[{"code":"ABI","region":"US-TX"}] |
      | m[{"code":"ACT","region":"US-TX"}] |
      | m[{"code":"CLL","region":"US-TX"}] |
      | m[{"code":"IAH","region":"US-TX"}] |
      | m[{"code":"BPT","region":"US-TX"}] |
      | m[{"code":"AEX","region":"US-LA"}] |
      | m[{"code":"AMA","region":"US-TX"}] |
      | m[{"code":"PHX","region":"US-AZ"}] |
      | m[{"code":"BRO","region":"US-TX"}] |
      | m[{"code":"BTR","region":"US-LA"}] |
      | m[{"code":"SAT","region":"US-TX"}] |
      | m[{"code":"MSY","region":"US-LA"}] |
      | m[{"code":"HOU","region":"US-TX"}] |
      | m[{"code":"ELP","region":"US-TX"}] |
      | m[{"code":"TUS","region":"US-AZ"}] |
      | m[{"code":"GGG","region":"US-TX"}] |
      | m[{"code":"GRK","region":"US-TX"}] |
      | m[{"code":"LAW","region":"US-OK"}] |
      | m[{"code":"LCH","region":"US-LA"}] |
      | m[{"code":"LFT","region":"US-LA"}] |
      | m[{"code":"LRD","region":"US-TX"}] |
      | m[{"code":"MFE","region":"US-TX"}] |
      | m[{"code":"MLU","region":"US-LA"}] |
      | m[{"code":"SJT","region":"US-TX"}] |
      | m[{"code":"SPS","region":"US-TX"}] |
      | m[{"code":"TYR","region":"US-TX"}] |
      | m[{"code":"SWO","region":"US-OK"}] |
      | m[{"code":"AFW","region":"US-TX"}] |
      | m[{"code":"IFP","region":"US-AZ"}] |
      | m[{"code":"GCN","region":"US-AZ"}] |
      | m[{"code":"DRT","region":"US-TX"}] |
      | m[{"code":"IGM","region":"US-AZ"}] |
      | m[{"code":"YUM","region":"US-AZ"}] |
      | m[{"code":"PRC","region":"US-AZ"}] |
      | m[{"code":"PGA","region":"US-AZ"}] |
      | m[{"code":"OKC","region":"US-OK"}] |
      | m[{"code":"DAL","region":"US-TX"}] |
      | m[{"code":"TUL","region":"US-OK"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_routesLongerThan8000MilesEachPairOnce
    Given the airroutes graph
    And the intent
      """
      Which routes are longer than 8,000 miles? List each pair of airports once, as a path of the airport code that
      comes first alphabetically, the distance and the other airport code.
      """
    And the traversal of
      """
      g.V().as("a").outE().has("dist",P.gt(8000)).inV().as("b").filter(select("a","b").by("code").where("a",P.lt("b"))).path().by("code").by("dist")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[ATL,d[8434].i,JNB] |
      | p[DFW,d[8105].i,HKG] |
      | p[DFW,d[8574].i,SYD] |
      | p[DFW,d[8022].i,DXB] |
      | p[JED,d[8314].i,LAX] |
      | p[CAN,d[8754].i,MEX] |
      | p[IAH,d[8591].i,SYD] |
      | p[JFK,d[8504].i,MNL] |
      | p[JFK,d[9526].i,SIN] |
      | p[LAX,d[8756].i,SIN] |
      | p[LAX,d[8246].i,RUH] |
      | p[SEA,d[8059].i,SIN] |
      | p[SFO,d[8433].i,SIN] |
      | p[EWR,d[8047].i,HKG] |
      | p[EWR,d[9523].i,SIN] |
      | p[LHR,d[9009].i,PER] |
      | p[MEL,d[8197].i,YVR] |
      | p[DXB,d[8150].i,IAH] |
      | p[DXB,d[8321].i,LAX] |
      | p[DXB,d[8085].i,SFO] |
      | p[HKG,d[8054].i,JFK] |
      | p[HKG,d[8135].i,IAD] |
      | p[AKL,d[9025].i,DOH] |
      | p[AKL,d[8186].i,ORD] |
      | p[AKL,d[8818].i,DXB] |
      | p[PEK,d[8884].i,PTY] |
      | p[DOH,d[8030].i,IAH] |
      | p[DOH,d[8287].i,LAX] |
      | p[SCL,d[8208].i,TLV] |
      | p[AUH,d[8053].i,DFW] |
      | p[AUH,d[8139].i,SFO] |
      | p[AUH,d[8372].i,LAX] |

  Scenario: g_codeAndRunwaysOfAirportsWithSixOrMore
    Given the airroutes graph
    And the intent
      """
      For each airport with six or more runways, return a two-item list of its code and its number of runways.
      """
    And the traversal of
      """
      g.V().has("airport","runways",P.gte(6)).local(union(values("code"),values("runways")).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[BOS,d[6].i] |
      | l[DFW,d[7].i] |
      | l[ORD,d[7].i] |
      | l[DEN,d[6].i] |
      | l[DTW,d[6].i] |
      | l[AMS,d[6].i] |

  Scenario: g_airportsSouthOf50SOrNorthOf77N
    Given the airroutes graph
    And the intent
      """
      Which airports lie south of latitude -50 or north of latitude 77? Return each as a map of its city and its
      latitude.
      """
    And the traversal of
      """
      g.V().has("lat",P.outside(-50,77)).valueMap("city","lat").by(unfold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"city":"Rio Grande","lat":"d[-53.7777].d"}] |
      | m[{"city":"Rio Gallegos","lat":"d[-51.6089].d"}] |
      | m[{"city":"Longyearbyen","lat":"d[78.2461013793945].d"}] |
      | m[{"city":"Qaanaaq","lat":"d[77.4886016846].d"}] |
      | m[{"city":"Puerto Natales","lat":"d[-51.671501159668].d"}] |
      | m[{"city":"Ushuahia","lat":"d[-54.8433].d"}] |
      | m[{"city":"El Calafate","lat":"d[-50.2803001404].d"}] |
      | m[{"city":"Stanley","lat":"d[-51.6856994628906].d"}] |
      | m[{"city":"Mount Pleasant","lat":"d[-51.8227996826172].d"}] |
      | m[{"city":"Punta Arenas","lat":"d[-53.0026016235352].d"}] |

  Scenario: g_airportsNearThePrimeMeridian
    Given the airroutes graph
    And the intent
      """
      Which airports have a longitude from -0.1 up to but not including 0.1? Return each as a map of its code and its
      longitude.
      """
    And the traversal of
      """
      g.V().hasLabel("airport").has("lon",P.between(-0.1,0.1)).valueMap("code","lon").by(unfold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"code":"LCY","lon":"d[0.055278].d"}] |
      | m[{"code":"LDE","lon":"d[-0.006438999902457].d"}] |
      | m[{"code":"CDT","lon":"d[0.0261109992862].d"}] |
      | m[{"code":"LEH","lon":"d[0.0880559980869293].d"}] |

  Scenario: g_distanceOfRouteFromDFWToMIA
    Given the airroutes graph
    And the intent
      """
      What is the distance of the route from Dallas (DFW) to Miami (MIA)?
      """
    And the traversal of
      """
      g.V().has("code","MIA").inE().as("e").outV().has("code","DFW").select("e").values("dist")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1120].i |

  Scenario: g_citiesStartingWithDal
    Given the airroutes graph
    And the intent
      """
      What are the names of the cities with airports, where the city name starts with "Dal", without duplicates?
      """
    And the traversal of
      """
      g.V().hasLabel("airport").has("city",TextP.startingWith("Dal")).values("city").dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Dallas |
      | Dalaman |
      | Dalian |
      | Dalcahue |
      | Dalat |
      | Dalanzadgad |

  Scenario: g_longestRunwayAtDFWInFeetAndMeters
    Given the airroutes graph
    And the intent
      """
      What is the length of the longest runway at Dallas (DFW) in both feet and meters? Return a map with the key
      "Longest runway at" holding the airport code, "feet" holding the length in feet and "meters" holding it in meters.
      """
    And the traversal of
      """
      g.V().has("code","DFW").project("Longest runway at","feet","meters").by("code").by("longest").by(__.values("longest").math("_ * 0.3048"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"Longest runway at":"DFW","feet":"d[13401].i","meters":"d[4084.6248].d"}] |

  Scenario: g_citiesWithoutVowelsButWithYOrH
    Given the airroutes graph
    And the intent
      """
      Which airport cities have none of the lowercase letters a, e, i, o or u in their name, but do contain a "y" or an
      "h"? Return each city name once.
      """
    And the traversal of
      """
      g.V().hasLabel("airport").has("city",TextP.notContaining("e")).has("city",TextP.notContaining("a")).has("city",TextP.notContaining("i")).has("city",TextP.notContaining("u")).has("city",TextP.notContaining("o")).or(has("city",TextP.containing("y")),has("city",TextP.containing("h"))).values("city").dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Osh |
      | Kyzyl |
      | Brønnøy |

  Scenario: g_virginiaAirportsAsDelimitedRows
    Given the airroutes graph
    And the intent
      """
      For each airport in Virginia (region US-VA), produce one line of text with its code, the length of its longest
      runway and its city, in that order, separated by commas.
      """
    And the traversal of
      """
      g.V().has("airport","region","US-VA").map(union(values("code"),values("longest"),values("city")).fold().conjoin(","))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | SHD,6002,Staunton/Waynesboro/Harrisonburg |
      | ORF,9001,Norfolk |
      | RIC,9003,Richmond |
      | IAD,11500,Washington D.C. |
      | ROA,6800,Roanoke |
      | CHO,6001,Charlottesville |
      | PHF,8003,Newport News |
      | LYH,5799,Lynchburg |

  Scenario: g_AUSDestinationsWithMoreThanSixOrExactlyFourRunways
    Given the airroutes graph
    And the intent
      """
      Which airports that AUS flies to have more than six runways or exactly four? Return each as a map of its code and
      its number of runways.
      """
    And the traversal of
      """
      g.V().has("airport","code","AUS").out().where(values("runways").is(P.gt(6).or(P.eq(4)))).valueMap("code","runways").by(unfold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"code":"CVG","runways":"d[4].i"}] |
      | m[{"code":"STL","runways":"d[4].i"}] |
      | m[{"code":"ABQ","runways":"d[4].i"}] |
      | m[{"code":"SFB","runways":"d[4].i"}] |
      | m[{"code":"MEM","runways":"d[4].i"}] |
      | m[{"code":"CLT","runways":"d[4].i"}] |
      | m[{"code":"BNA","runways":"d[4].i"}] |
      | m[{"code":"DFW","runways":"d[7].i"}] |
      | m[{"code":"PIE","runways":"d[4].i"}] |
      | m[{"code":"LAX","runways":"d[4].i"}] |
      | m[{"code":"MCO","runways":"d[4].i"}] |
      | m[{"code":"MIA","runways":"d[4].i"}] |
      | m[{"code":"MSP","runways":"d[4].i"}] |
      | m[{"code":"ORD","runways":"d[7].i"}] |
      | m[{"code":"IAD","runways":"d[4].i"}] |
      | m[{"code":"JFK","runways":"d[4].i"}] |
      | m[{"code":"SLC","runways":"d[4].i"}] |
      | m[{"code":"LAS","runways":"d[4].i"}] |
      | m[{"code":"SFO","runways":"d[4].i"}] |
      | m[{"code":"OAK","runways":"d[4].i"}] |
      | m[{"code":"HNL","runways":"d[4].i"}] |
      | m[{"code":"HOU","runways":"d[4].i"}] |
      | m[{"code":"PHL","runways":"d[4].i"}] |
      | m[{"code":"FRA","runways":"d[4].i"}] |
      | m[{"code":"PIT","runways":"d[4].i"}] |
      | m[{"code":"OKC","runways":"d[4].i"}] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_whichCityEachOfIADMIALAXIsIn
    Given the airroutes graph
    And the intent
      """
      For IAD, MIA and LAX, produce a sentence saying which city each airport is in, in the form "IAD is in Washington
      D.C.".
      """
    And the traversal of
      """
      g.V().has("airport","code",P.within("IAD","MIA","LAX")).as("v").values("code").concat(constant(" is in "),select("v").values("city"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | IAD is in Washington D.C. |
      | LAX is in Los Angeles |
      | MIA is in Miami |

  @DataBigInt
  Scenario: g_holderOfAccountNumber
    Given the empty graph
    And the intent
      """
      Who holds account number 40000000000000000017?
      """
    And the graph initializer of
      """
      g.addV("account").property("holder","Ana Ruiz").property("number",40000000000000000017n).
        addV("account").property("holder","Ben Okafor").property("number",40000000000000000071n)
      """
    And the traversal of
      """
      g.V().has("account","number",40000000000000000017n).values("holder")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ana Ruiz |

  @DataNull
  Scenario: g_transfersWithMemos
    Given the empty graph
    And the intent
      """
      List every transfer as a map with the holder of the sending account ("from"), the holder of the receiving account
      ("to") and the memo ("memo"), using null for a transfer that has no memo.
      """
    And the graph initializer of
      """
      g.addV("account").property("holder","Ana Ruiz").as("a").
        addV("account").property("holder","Ben Okafor").as("b").
        addV("account").property("holder","Chen Wei").as("c").
        addE("transfer").from("a").to("b").property("amount",950m).property("memo","rent").
        addE("transfer").from("b").to("c").property("amount",42.8m).
        addE("transfer").from("c").to("a").property("amount",61.25m).property("memo","dinner")
      """
    And the traversal of
      """
      g.E().hasLabel("transfer").project("from","to","memo").by(__.outV().values("holder")).by(__.inV().values("holder")).by(__.coalesce(__.values("memo"),__.constant(null)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"from":"Ana Ruiz","to":"Ben Okafor","memo":"rent"}] |
      | m[{"from":"Ben Okafor","to":"Chen Wei","memo":null}] |
      | m[{"from":"Chen Wei","to":"Ana Ruiz","memo":"dinner"}] |

  @AllowUUIDPropertyValues @DataUUID
  Scenario: g_modelOfDeviceWithSerial
    Given the empty graph
    And the intent
      """
      Which model is the device with serial number 3f2b8cae-7d4a-4eab-9c55-1a2b3c4d5eaf?
      """
    And the graph initializer of
      """
      g.addV("device").property("serial",UUID("3f2b8cae-7d4a-4eab-9c55-1a2b3c4d5eaf")).property("model","TH-200").
        addV("device").property("serial",UUID("9a1c7ea4-02b6-4f1d-8eba-6b5d4c3b2a19")).property("model","GW-10").
        addV("device").property("serial",UUID("c47d2ea0-5b18-4a6f-b2eb-0f9eed7c6b5a")).property("model","PR-5")
      """
    And the traversal of
      """
      g.V().has("device","serial",UUID("3f2b8cae-7d4a-4eab-9c55-1a2b3c4d5eaf")).values("model")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | TH-200 |

  @DataByte
  Scenario: g_sensorsReportingFaults
    Given the empty graph
    And the intent
      """
      Which sensors are reporting a fault, meaning their status code is anything other than 0? Return a map for each
      with the model of the device it belongs to ("model"), the kind of sensor ("kind") and the status code ("status").
      """
    And the graph initializer of
      """
      g.addV("device").property("model","TH-200").as("d1").
        addV("device").property("model","PR-5").as("d2").
        addV("sensor").property("kind","temperature").property("status",0b).as("s1").
        addV("sensor").property("kind","humidity").property("status",3b).as("s2").
        addV("sensor").property("kind","pressure").property("status",0b).as("s3").
        addV("sensor").property("kind","vibration").property("status",7b).as("s4").
        addE("has").from("d1").to("s1").addE("has").from("d1").to("s2").addE("has").from("d2").to("s3").addE("has").from("d2").to("s4")
      """
    And the traversal of
      """
      g.V().hasLabel("sensor").has("status",P.neq(0)).project("model","kind","status").by(__.in("has").values("model")).by("kind").by("status")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"model":"TH-200","kind":"humidity","status":"d[3].b"}] |
      | m[{"model":"PR-5","kind":"vibration","status":"d[7].b"}] |

  @MultiProperties @DataSet
  Scenario: g_employeesWhoKnowKafka
    Given the empty graph
    And the intent
      """
      Which employees list the skill "kafka" among their skills? Return their names.
      """
    And the graph initializer of
      """
      g.addV("employee").property("name","Ines Duarte").property(Cardinality.set,"skills","java").property(Cardinality.set,"skills","kafka").
        addV("employee").property("name","Kofi Mensah").property(Cardinality.set,"skills","python").property(Cardinality.set,"skills","sql").
        addV("employee").property("name","Raj Patel").property(Cardinality.set,"skills","kafka").property(Cardinality.set,"skills","kubernetes").property(Cardinality.set,"skills","go").
        addV("employee").property("name","Mei Tanaka").property(Cardinality.set,"skills","kafka-streams")
      """
    And the traversal of
      """
      g.V().hasLabel("employee").has("skills","kafka").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ines Duarte |
      | Raj Patel |

  @AllowDateTimePropertyValues @MetaProperties @MultiProperties @DataList
  Scenario: g_currentTitleOfEachEmployee
    Given the empty graph
    And the intent
      """
      What is the current title of each employee, meaning the one with no end date? Return a map for each employee with
      the name ("name") and the title ("title").
      """
    And the graph initializer of
      """
      g.addV("employee").property("name","Omar Said").property(Cardinality.list,"title","engineer","since",datetime("2018-04-01T00:00:00Z"),"until",datetime("2021-07-01T00:00:00Z")).property(Cardinality.list,"title","senior engineer","since",datetime("2021-07-01T00:00:00Z")).
        addV("employee").property("name","Lena Vogel").property(Cardinality.list,"title","analyst","since",datetime("2019-01-07T00:00:00Z"),"until",datetime("2020-12-31T00:00:00Z")).property(Cardinality.list,"title","manager","since",datetime("2021-01-01T00:00:00Z"),"until",datetime("2023-06-30T00:00:00Z")).property(Cardinality.list,"title","director","since",datetime("2023-07-01T00:00:00Z")).
        addV("employee").property("name","Kofi Mensah").property(Cardinality.list,"title","engineer","since",datetime("2022-09-12T00:00:00Z"))
      """
    And the traversal of
      """
      g.V().hasLabel("employee").project("name","title").by("name").by(__.properties("title").hasNot("until").value())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name":"Omar Said","title":"senior engineer"}] |
      | m[{"name":"Lena Vogel","title":"director"}] |
      | m[{"name":"Kofi Mensah","title":"engineer"}] |

  @SupportsChar @DataChar
  Scenario: g_employeesWithGradeA
    Given the empty graph
    And the intent
      """
      Which employees have a performance grade of A? Return their names.
      """
    And the graph initializer of
      """
      g.addV("employee").property("name","Ines Duarte").property("grade","A"c).
        addV("employee").property("name","Kofi Mensah").property("grade","B"c).
        addV("employee").property("name","Raj Patel").property("grade","A"c).
        addV("employee").property("name","Mei Tanaka").property("grade","C"c)
      """
    And the traversal of
      """
      g.V().hasLabel("employee").has("grade","A"c).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ines Duarte |
      | Raj Patel |

  @DataNumber
  Scenario: g_contactsWithAgeNotStoredAsNumber
    Given the empty graph
    And the intent
      """
      Which contacts have an age that is not stored as a number? Return their names.
      """
    And the graph initializer of
      """
      g.addV("contact").property("name","Alba Reyes").property("age",34).
        addV("contact").property("name","Bram de Vries").property("age","41").
        addV("contact").property("name","Chiara Russo").property("age",28).
        addV("contact").property("name","Dmitri Volkov").property("age","unknown").
        addV("contact").property("name","Esi Owusu").property("age",52)
      """
    And the traversal of
      """
      g.V().hasLabel("contact").not(__.has("age",P.typeOf(GType.NUMBER))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Bram de Vries |
      | Dmitri Volkov |

  @DataInt
  Scenario: g_ageOfEachContactAsNumber
    Given the empty graph
    And the intent
      """
      Ages were imported partly as numbers and partly as text. Return the age of each contact as a number, in a map with
      the name ("name") and the age ("age").
      """
    And the graph initializer of
      """
      g.addV("contact").property("name","Alba Reyes").property("age",34).
        addV("contact").property("name","Bram de Vries").property("age","41").
        addV("contact").property("name","Chiara Russo").property("age",28).
        addV("contact").property("name","Esi Owusu").property("age","52")
      """
    And the traversal of
      """
      g.V().hasLabel("contact").project("name","age").by("name").by(__.values("age").asNumber())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name":"Alba Reyes","age":"d[34].i"}] |
      | m[{"name":"Bram de Vries","age":"d[41].i"}] |
      | m[{"name":"Chiara Russo","age":"d[28].i"}] |
      | m[{"name":"Esi Owusu","age":"d[52].i"}] |

  @DataBoolean
  Scenario: g_activeContacts
    Given the empty graph
    And the intent
      """
      The active flag was imported partly as true or false and partly as the text "true" or "false". Which contacts are
      active? Return their names.
      """
    And the graph initializer of
      """
      g.addV("contact").property("name","Alba Reyes").property("active",true).
        addV("contact").property("name","Bram de Vries").property("active","true").
        addV("contact").property("name","Chiara Russo").property("active","false").
        addV("contact").property("name","Esi Owusu").property("active",false)
      """
    And the traversal of
      """
      g.V().hasLabel("contact").where(__.values("active").asBool().is(true)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Alba Reyes |
      | Bram de Vries |

  @DataLong @DataNull
  Scenario: g_contactExportWithPhoneAsText
    Given the empty graph
    And the intent
      """
      Phone numbers were imported partly as numbers and partly as text. Export each contact as a map with the name
      ("name"), the phone number as text ("phone") and the email address ("email"), using null for a contact with no
      email address.
      """
    And the graph initializer of
      """
      g.addV("contact").property("name","Alba Reyes").property("phone",4915112345678l).property("email","alba@example.com").
        addV("contact").property("name","Bram de Vries").property("phone","+31 20 555 0134").
        addV("contact").property("name","Esi Owusu").property("phone",233201234567l).property("email","esi@example.org")
      """
    And the traversal of
      """
      g.V().hasLabel("contact").project("name","phone","email").by("name").by(__.values("phone").asString()).by(__.coalesce(__.values("email"),__.constant(null)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name":"Alba Reyes","phone":"4915112345678","email":"alba@example.com"}] |
      | m[{"name":"Bram de Vries","phone":"+31 20 555 0134","email":null}] |
      | m[{"name":"Esi Owusu","phone":"233201234567","email":"esi@example.org"}] |
