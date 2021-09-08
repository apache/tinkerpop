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

@StepClassBranch @StepChoose
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

  @GraphComputerVerificationStarGraphExceeded
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

  @GraphComputerVerificationStarGraphExceeded
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

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_VX1X_optionalXaddVXdogXX_label
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
