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
Feature: Step - comparison

  Scenario: g_oneWayRoutesFromJFK
    Given the airroutes graph
    And the intent
      """
      Which airports can be flown to from New York (JFK) that have no route flying back to JFK? Return a map for each
      with the key "s" holding JFK's code and the key "d" holding the destination's code.
      """
    And the traversal of
      """
      g.V().has("code","JFK").as("s").out().as("d").where(__.not(__.out().as("s"))).select("s","d").by("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"s":"JFK","d":"LCY"}] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_airportsInCitiesStartingLikeIADs
    Given the airroutes graph
    And the intent
      """
      Which airports are in a city whose name begins with the same first three letters as the city of IAD? Return the
      city of each such airport, IAD included.
      """
    And the traversal of
      """
      g.V().has("airport","code","IAD").as("a").V().hasLabel("airport").where(TextP.startingWith("a")).by("city").by(values("city").substring(0,3)).values("city")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Washington D.C. |
      | Washington D.C. |
      | Waskaganish |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_DFWRoutesLongerThanIADsLongest
    Given the airroutes graph
    And the intent
      """
      Which airports does DFW fly to on a route longer than the longest route from IAD? Return their codes.
      """
    And the traversal of
      """
      g.V().has("code","IAD").outE("route").values("dist").max().as("iad").V().has("code","DFW").outE("route").as("r").where(values("dist").where(P.gt("iad"))).inV().values("code")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | SYD |

  Scenario: g_routesWithoutReturnFlight
    Given the airroutes graph
    And the traversal of
      """
      g.V().hasLabel("airport").match(__.as("s").out().as("d"),__.not(__.as("d").out().as("s"))).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[339].l |

  Scenario: g_AUSDestinationsWithSameRunwayCountAsAUS
    Given the airroutes graph
    And the intent
      """
      Which airports that AUS flies to have the same number of runways as AUS? Return each as a map of its code and its
      number of runways.
      """
    And the traversal of
      """
      g.V().has("code","AUS").as("aus").values("runways").as("ausr").select("aus").out().as("outa").values("runways").as("outr").where("ausr",P.eq("outr")).select("outa").valueMap("code","runways").by(unfold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"code":"BUR","runways":"d[2].i"}] |
      | m[{"code":"CUN","runways":"d[2].i"}] |
      | m[{"code":"GDL","runways":"d[2].i"}] |
      | m[{"code":"PNS","runways":"d[2].i"}] |
      | m[{"code":"TYS","runways":"d[2].i"}] |
      | m[{"code":"VPS","runways":"d[2].i"}] |
      | m[{"code":"NAS","runways":"d[2].i"}] |
      | m[{"code":"JAX","runways":"d[2].i"}] |
      | m[{"code":"PVD","runways":"d[2].i"}] |
      | m[{"code":"SMF","runways":"d[2].i"}] |
      | m[{"code":"BHM","runways":"d[2].i"}] |
      | m[{"code":"FLL","runways":"d[2].i"}] |
      | m[{"code":"SNA","runways":"d[2].i"}] |
      | m[{"code":"MSY","runways":"d[2].i"}] |
      | m[{"code":"BUF","runways":"d[2].i"}] |
      | m[{"code":"MEX","runways":"d[2].i"}] |
      | m[{"code":"CHS","runways":"d[2].i"}] |
      | m[{"code":"ONT","runways":"d[2].i"}] |
      | m[{"code":"LGW","runways":"d[2].i"}] |
      | m[{"code":"AMA","runways":"d[2].i"}] |
      | m[{"code":"LHR","runways":"d[2].i"}] |
      | m[{"code":"CMH","runways":"d[2].i"}] |
      | m[{"code":"DSM","runways":"d[2].i"}] |
      | m[{"code":"CZM","runways":"d[2].i"}] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_airportsInSameRegionAsNCE
    Given the airroutes graph
    And the intent
      """
      Which airports are in the same region as NCE? Return each as a list of its city, its code and its region, in that
      order.
      """
    And the traversal of
      """
      g.V().has("code","NCE").values("region").as("r").V().hasLabel("airport").as("a").values("region").where(P.eq("r")).by().local(select("a").union(values("city"),values("code"),values("region")).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[Marseille,MRS,FR-U] |
      | l[Toulon/Hyères/Le Palyvestre,TLN,FR-U] |
      | l[Avignon/Caumont,AVN,FR-U] |
      | l[Nice,NCE,FR-U] |

  @AllowDateTimePropertyValues @DataBoolean @DataDateTime
  Scenario: g_frozenAccountsCreditedSinceMarch2024
    Given the empty graph
    And the intent
      """
      Which frozen accounts have still received a transfer on or after March 1, 2024 (UTC)? Return the holders.
      """
    And the graph initializer of
      """
      g.addV("account").property("holder","Ana Ruiz").property("frozen",true).as("a").
        addV("account").property("holder","Ben Okafor").property("frozen",true).as("b").
        addV("account").property("holder","Chen Wei").property("frozen",false).as("c").
        addV("account").property("holder","Dara Novak").property("frozen",true).as("d").
        addE("transfer").from("c").to("a").property("amount",75m).property("at",datetime("2024-03-05T14:20:00Z")).
        addE("transfer").from("c").to("b").property("amount",40m).property("at",datetime("2024-02-10T08:00:00Z")).
        addE("transfer").from("a").to("c").property("amount",12.5m).property("at",datetime("2024-03-20T11:45:00Z")).
        addE("transfer").from("c").to("d").property("amount",300m).property("at",datetime("2024-03-01T00:00:00Z"))
      """
    And the traversal of
      """
      g.V().has("account","frozen",true).where(__.inE("transfer").has("at",P.gte(datetime("2024-03-01T00:00:00Z")))).values("holder")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ana Ruiz |
      | Dara Novak |

  @AllowDateTimePropertyValues @DataDateTime
  Scenario: g_accountsOpenedIn2023WithoutPayments
    Given the empty graph
    And the intent
      """
      Which accounts opened during 2023 (UTC) have never paid a merchant? Return the holders.
      """
    And the graph initializer of
      """
      g.addV("account").property("holder","Ana Ruiz").property("opened",datetime("2023-02-14T09:00:00Z")).as("a").
        addV("account").property("holder","Ben Okafor").property("opened",datetime("2023-11-30T16:30:00Z")).
        addV("account").property("holder","Chen Wei").property("opened",datetime("2022-12-31T23:30:00Z")).
        addV("account").property("holder","Dara Novak").property("opened",datetime("2024-01-01T00:00:00Z")).
        addV("merchant").property("name","FreshMart").property("category","grocery").as("m").
        addE("payment").from("a").to("m").property("amount",18.4m)
      """
    And the traversal of
      """
      g.V().hasLabel("account").has("opened",P.between(datetime("2023-01-01T00:00:00Z"),datetime("2024-01-01T00:00:00Z"))).not(__.outE("payment")).values("holder")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ben Okafor |

  @AllowUUIDPropertyValues @SupportsDuration @DataDuration @DataUUID
  Scenario: g_devicesUpMoreThan30Days
    Given the empty graph
    And the intent
      """
      Which devices have been up for more than 30 days? Return their serial numbers.
      """
    And the graph initializer of
      """
      g.addV("device").property("serial",UUID("3f2b8cae-7d4a-4eab-9c55-1a2b3c4d5eaf")).property("model","TH-200").property("uptime",Duration(2678400,0)).
        addV("device").property("serial",UUID("9a1c7ea4-02b6-4f1d-8eba-6b5d4c3b2a19")).property("model","GW-10").property("uptime",Duration(2592000,0)).
        addV("device").property("serial",UUID("c47d2ea0-5b18-4a6f-b2eb-0f9eed7c6b5a")).property("model","PR-5").property("uptime",Duration(86400,0)).
        addV("device").property("serial",UUID("5eaf7a8b-9c0d-4eaf-a2b3-c4d5eaf7a8b9")).property("model","TH-300").property("uptime",Duration(7776000,500000000))
      """
    And the traversal of
      """
      g.V().hasLabel("device").has("uptime",P.gt(Duration(2592000,0))).values("serial")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | uuid[3f2b8cae-7d4a-4eab-9c55-1a2b3c4d5eaf] |
      | uuid[5eaf7a8b-9c0d-4eaf-a2b3-c4d5eaf7a8b9] |

  @AllowDateTimePropertyValues @DataBoolean @DataDateTime
  Scenario: g_offlineDevicesInstalledBefore2023
    Given the empty graph
    And the intent
      """
      Which offline devices were installed before 2023 (UTC)? Return their models.
      """
    And the graph initializer of
      """
      g.addV("device").property("model","TH-200").property("online",false).property("installed",datetime("2021-06-15T09:00:00Z")).
        addV("device").property("model","GW-10").property("online",true).property("installed",datetime("2020-03-01T12:00:00Z")).
        addV("device").property("model","PR-5").property("online",false).property("installed",datetime("2023-01-01T00:00:00Z")).
        addV("device").property("model","TH-300").property("online",false).property("installed",datetime("2022-12-31T22:00:00Z"))
      """
    And the traversal of
      """
      g.V().hasLabel("device").has("online",false).has("installed",P.lt(datetime("2023-01-01T00:00:00Z"))).values("model")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | TH-200 |
      | TH-300 |

  @AllowDateTimePropertyValues @MetaProperties @MultiProperties @DataDateTime @DataList
  Scenario: g_managersAtAnyTimeIn2022
    Given the empty graph
    And the intent
      """
      Who held the title "manager" at any time during 2022 (UTC)? Return their names.
      """
    And the graph initializer of
      """
      g.addV("employee").property("name","Grace Hopkins").property(Cardinality.list,"title","manager","since",datetime("2016-02-01T00:00:00Z"),"until",datetime("2021-12-31T00:00:00Z")).property(Cardinality.list,"title","director","since",datetime("2022-01-01T00:00:00Z")).
        addV("employee").property("name","Lena Vogel").property(Cardinality.list,"title","analyst","since",datetime("2019-01-07T00:00:00Z"),"until",datetime("2020-12-31T00:00:00Z")).property(Cardinality.list,"title","manager","since",datetime("2021-01-01T00:00:00Z"),"until",datetime("2023-06-30T00:00:00Z")).
        addV("employee").property("name","Raj Patel").property(Cardinality.list,"title","engineer","since",datetime("2020-05-04T00:00:00Z"),"until",datetime("2023-01-01T00:00:00Z")).property(Cardinality.list,"title","manager","since",datetime("2023-01-01T00:00:00Z")).
        addV("employee").property("name","Omar Said").property(Cardinality.list,"title","manager","since",datetime("2022-11-15T00:00:00Z")).
        addV("employee").property("name","Kofi Mensah").property(Cardinality.list,"title","engineer","since",datetime("2022-09-12T00:00:00Z"))
      """
    And the traversal of
      """
      g.V().hasLabel("employee").where(__.properties("title").hasValue("manager").has("since",P.lt(datetime("2023-01-01T00:00:00Z"))).or(__.hasNot("until"),__.has("until",P.gte(datetime("2022-01-01T00:00:00Z"))))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Lena Vogel |
      | Omar Said |

  @AllowDateTimePropertyValues @DataDateTime
  Scenario: g_contactsWhoJoinedBefore2024
    Given the empty graph
    And the intent
      """
      Join dates were imported partly as dates and partly as ISO-8601 text. Which contacts joined before 2024 (UTC)?
      Return their names.
      """
    And the graph initializer of
      """
      g.addV("contact").property("name","Alba Reyes").property("joined",datetime("2023-05-14T10:00:00Z")).
        addV("contact").property("name","Bram de Vries").property("joined","2023-12-31T23:59:00Z").
        addV("contact").property("name","Chiara Russo").property("joined","2024-02-01T09:30:00Z").
        addV("contact").property("name","Esi Owusu").property("joined",datetime("2024-01-01T00:00:00Z"))
      """
    And the traversal of
      """
      g.V().hasLabel("contact").where(__.values("joined").asDate().is(P.lt(datetime("2024-01-01T00:00:00Z")))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Alba Reyes |
      | Bram de Vries |
