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

Feature: Step - choose()

  Scenario: g_VX2X_optionalXoutXknowsXX
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid2).optional(__.out("knows"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_VX2X_optionalXinXknowsXX
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid2).optional(__.in("knows"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_V_hasLabelXpersonX_optionalXoutXknowsX_optionalXoutXcreatedXXX_path
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        optional(__.out("knows").
                    optional(__.out("created"))).
        path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],v[vadas]] |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[marko],v[josh],v[lop]] |
      | p[v[vadas]] |
      | p[v[josh]] |
      | p[v[peter]] |
    
  Scenario: g_V_optionalXout_optionalXoutXX_path
    Given the modern graph
    And the traversal of
      """
      g.V().optional(__.out().optional(__.out())).path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],v[lop]] |
      | p[v[marko],v[vadas]] |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[marko],v[josh],v[lop]] |
      | p[v[vadas]] |
      | p[v[lop]] |
      | p[v[josh],v[ripple]] |
      | p[v[josh],v[lop]] |
      | p[v[ripple]] |
      | p[v[peter],v[lop]] |

  Scenario: g_VX1X_optionalXaddVXdogXX_label
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
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).optional(__.addV("dog")).label()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dog |
    And the graph should return 7 for count of "g.V()"
