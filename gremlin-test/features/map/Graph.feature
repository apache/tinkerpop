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

Feature: Step - V()

  Scenario: g_VX1X_V_valuesXnameX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).V().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko  |
      | vadas |
      | lop   |
      | josh |
      | ripple |
      | peter  |

  Scenario: g_V_outXknowsX_V_name
    Given the modern graph
    And the traversal of
      """
      g.V().out("knows").V().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko  |
      | marko  |
      | vadas |
      | vadas |
      | lop   |
      | lop   |
      | josh |
      | josh |
      | ripple |
      | ripple |
      | peter  |
      | peter  |

  Scenario: g_V_hasXname_GarciaX_inXsungByX_asXsongX_V_hasXname_Willie_DixonX_inXwrittenByX_whereXeqXsongXX_name
    Given the grateful graph
    And the traversal of
      """
      g.V().has("artist", "name", "Garcia").in("sungBy").as("song").
        V().has("artist", "name", "Willie_Dixon").in("writtenBy").where(P.eq("song")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | MY BABE |
      | HOOCHIE COOCHIE MAN |

  Scenario: g_V_hasLabelXpersonX_asXpX_VXsoftwareX_addInEXuses_pX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
      """
    And using the parameter software defined as "l[v[lop],v[ripple]]"
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter v2Id defined as "v[vadas].id"
    And using the parameter v3Id defined as "v[lop].id"
    And using the parameter v4Id defined as "v[josh].id"
    And using the parameter v5Id defined as "v[ripple].id"
    And using the parameter v6Id defined as "v[peter].id"
    And the traversal of
      """
      g.V().hasLabel("person").as("p").V(software).addE("uses").from("p")
      """
    When iterated to list
    Then the result should have a count of 8
    And the graph should return 8 for count of "g.E().hasLabel(\"uses\")"
    And the graph should return 2 for count of "g.V(v1Id).outE(\"uses\")"
    And the graph should return 2 for count of "g.V(v2Id).outE(\"uses\")"
    And the graph should return 4 for count of "g.V(v3Id).inE(\"uses\")"
    And the graph should return 2 for count of "g.V(v4Id).outE(\"uses\")"
    And the graph should return 4 for count of "g.V(v5Id).inE(\"uses\")"
    And the graph should return 2 for count of "g.V(v6Id).outE(\"uses\")"
