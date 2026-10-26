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
Feature: Step - exploration

  Scenario: g_propertyKeysOnRoutesFromDFW
    Given the airroutes graph
    And the intent
      """
      Which property keys appear on the edges leaving DFW? Return each key once.
      """
    And the traversal of
      """
      g.V().has("code","DFW").outE().properties().key().dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dist |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_edgeLabelPathCounts
    Given the airroutes graph
    And the intent
      """
      What kinds of connection does the graph contain? For every edge, take the label of the vertex it leaves, the edge
      label and the label of the vertex it reaches, joined with "-", and return one map from each such combination to
      the number of edges that have it.
      """
    And the traversal of
      """
      g.V().outE().otherV().groupCount().by(path().by(T.label).unfold().fold().conjoin("-"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"continent-contains-airport":"d[3504].l","country-contains-airport":"d[3504].l","airport-route-airport":"d[50637].l"}] |

  Scenario: g_edgeCountsByLabel
    Given the airroutes graph
    And the intent
      """
      How many edges of each label does the graph have? Return one map from label to count.
      """
    And the traversal of
      """
      g.E().groupCount().by(T.label)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"contains":"d[7008].l","route":"d[50637].l"}] |

  Scenario: g_codesLinkedToAUSByLabel
    Given the airroutes graph
    And the intent
      """
      Group everything directly connected to AUS, in either direction, by vertex label. For each label, return the
      distinct codes in alphabetical order, as one map from label to that list.
      """
    And the traversal of
      """
      g.V().has("code","AUS").both().group().by(T.label).by(values("code").fold().dedup(Scope.local).order(Scope.local))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"continent":"l[NA]","country":"l[US]","airport":"l[ABQ,AMA,AMS,ASE,ATL,BHM,BKG,BNA,BOI,BOS,BTR,BUF,BUR,BWI,BZN,CHS,CLE,CLT,CMH,CUN,CVG,CZM,DAL,DCA,DEN,DFW,DSM,DTW,ECP,ELP,EWR,FLL,FRA,GDL,GRR,HDN,HNL,HOU,HRL,IAD,IAH,IND,JAX,JFK,LAS,LAX,LBB,LGB,LGW,LHR,LIR,LIT,MCI,MCO,MDW,MEM,MEX,MIA,MKE,MSP,MSY,NAS,OAK,OKC,OMA,ONT,ORD,PDX,PHL,PHX,PIE,PIT,PNS,PVD,PVR,RDU,RNO,SAN,SAT,SDF,SEA,SFB,SFO,SJC,SJD,SLC,SMF,SNA,STL,TPA,TUL,TUS,TYS,VPS,XNA,YVR,YYC,YYZ]"}] |

  Scenario: g_propertyCountOfDFW
    Given the airroutes graph
    And the intent
      """
      How many properties does the DFW airport have?
      """
    And the traversal of
      """
      g.V().has("code","DFW").properties().count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[12].l |

  Scenario: g_verticesWithNoOutgoingEdges
    Given the airroutes graph
    And the intent
      """
      How many vertices have no outgoing edges?
      """
    And the traversal of
      """
      g.V().not(outE()).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[36].l |

  Scenario: g_routeCount
    Given the airroutes graph
    And the intent
      """
      How many routes are there in the graph?
      """
    And the traversal of
      """
      g.E().where(label().is(P.eq("route"))).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[50637].l |

  Scenario: g_labelOfLBB
    Given the airroutes graph
    And the intent
      """
      What kind of vertex is LBB? Return its label.
      """
    And the traversal of
      """
      g.V().has("code","LBB").label()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | airport |

  Scenario: g_vertexCount
    Given the airroutes graph
    And the intent
      """
      How many vertices does the graph have?
      """
    And the traversal of
      """
      g.V().count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3749].l |

  Scenario: g_reportingHierarchyAsTree
    Given the empty graph
    And the intent
      """
      Return the reporting hierarchy as a tree of employee names, starting from the head of the organization, the one
      employee who reports to no one.
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
      g.V().hasLabel("employee").not(__.out("reportsTo")).repeat(__.in("reportsTo")).emit().tree().by("name")
      """
    When iterated next
    Then the result should be a tree with a structure of
      """
      |--Grace Hopkins
         |--Omar Said
            |--Ines Duarte
            |--Kofi Mensah
         |--Lena Vogel
            |--Raj Patel
               |--Mei Tanaka
      """
