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
Feature: Step - miscellaneous

  Scenario: g_V_coworker
    Given the modern graph
    And the intent
      """
      For each person who created software, show every other person who created software and what the two of them
      created in common. Return one map per person, keyed by their name, whose value maps each other person's name to a
      map of the number of shared creations ("numCoCreated") and the names of those creations ("coCreated").
      """
    And the traversal of
      """
      g.V().hasLabel("person").
        filter(__.outE("created")).
        aggregate("p").as("p1").
        values("name").as("p1n").
        select("p").unfold().
        where(neq("p1")).as("p2").values("name").as("p2n").
        select("p2").
        out("created").
        choose(__.in("created").where(P.eq("p1")), __.values("name"), __.constant([])).
        group().
          by(__.select("p1n")).
          by(__.group().
                  by(__.select("p2n")).
                  by(__.unfold().fold().
                     project("numCoCreated", "coCreated").
                       by(__.count(local)).by())).
        unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"peter": {"josh": {"numCoCreated":"d[1].l", "coCreated":["lop"]}, "marko": {"numCoCreated":"d[1].l", "coCreated":["lop"]}}}] |
      | m[{"josh": {"peter": {"numCoCreated":"d[1].l", "coCreated":["lop"]}, "marko": {"numCoCreated":"d[1].l", "coCreated":["lop"]}}}] |
      | m[{"marko": {"peter": {"numCoCreated":"d[1].l", "coCreated":["lop"]}, "josh": {"numCoCreated":"d[1].l", "coCreated":["lop"]}}}] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_coworker_with_midV
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        filter(__.outE("created")).as("p1").
        V().hasLabel("person").
        where(P.neq("p1")).
        filter(__.outE("created")).as("p2").
        map(__.out("created").where(__.in("created").as("p1")).values("name").fold()).
        group().
          by(__.select("p1").by("name")).
          by(__.group().by(select("p2").by("name")).
          by(__.project("numCoCreated", "coCreated").
                  by(__.count(local)).by())).
        unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"peter": {"josh": {"numCoCreated":"d[1].l", "coCreated":["lop"]}, "marko": {"numCoCreated":"d[1].l", "coCreated":["lop"]}}}] |
      | m[{"josh": {"peter": {"numCoCreated":"d[1].l", "coCreated":["lop"]}, "marko": {"numCoCreated":"d[1].l", "coCreated":["lop"]}}}] |
      | m[{"marko": {"peter": {"numCoCreated":"d[1].l", "coCreated":["lop"]}, "josh": {"numCoCreated":"d[1].l", "coCreated":["lop"]}}}] |


  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_openTriads
    Given the modern graph
    And the intent
      """
      Find the open triads: three vertices where the middle one is connected to both of the others, in either direction,
      but those two are not connected to each other. Return each triad once, as a map of the middle vertex's name
      ("middle") and the names of the other two in alphabetical order ("ends").
      """
    And the traversal of
      """
      g.V().as("a").both().as("b").both().where(P.neq("a")).as("c").not(both().where(P.eq("a"))).project("middle","ends").by(select("b").values("name")).by(union(select("a"),select("c")).values("name").order().fold()).dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"middle":"lop","ends":"l[marko,peter]"}] |
      | m[{"middle":"josh","ends":"l[marko,ripple]"}] |
      | m[{"middle":"marko","ends":"l[lop,vadas]"}] |
      | m[{"middle":"marko","ends":"l[josh,vadas]"}] |
      | m[{"middle":"josh","ends":"l[lop,ripple]"}] |
      | m[{"middle":"lop","ends":"l[josh,peter]"}] |
