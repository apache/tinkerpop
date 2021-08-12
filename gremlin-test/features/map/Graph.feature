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
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).V().values("name")
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
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And using the parameter xx1 defined as "l[v[lop],v[ripple]]"
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And using the parameter vid3 defined as "v[lop].id"
    And using the parameter vid4 defined as "v[josh].id"
    And using the parameter vid5 defined as "v[ripple].id"
    And using the parameter vid6 defined as "v[peter].id"
    And the traversal of
      """
      g.V().hasLabel("person").as("p").V(xx1).addE("uses").from("p")
      """
    When iterated to list
    Then the result should have a count of 8
    And the graph should return 8 for count of "g.E().hasLabel(\"uses\")"
    And the graph should return 2 for count of "g.V(vid1).outE(\"uses\")"
    And the graph should return 2 for count of "g.V(vid2).outE(\"uses\")"
    And the graph should return 4 for count of "g.V(vid3).inE(\"uses\")"
    And the graph should return 2 for count of "g.V(vid4).outE(\"uses\")"
    And the graph should return 4 for count of "g.V(vid5).inE(\"uses\")"
    And the graph should return 2 for count of "g.V(vid6).outE(\"uses\")"
