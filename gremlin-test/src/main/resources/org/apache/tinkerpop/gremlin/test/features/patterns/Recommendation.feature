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
Feature: Step - recommendation

  Scenario: g_V_classic_recommendation
    Given the grateful graph
    And the intent
      """
      Which five songs are most like DARK STAR, judged by how many times they are followed by the same songs that follow
      DARK STAR? Leave out DARK STAR itself and any song that directly follows it. Rank by that count, break ties by
      preferring the song with fewer performances, and return the song names.
      """
    And the traversal of
      """
      g.V().has("name", "DARK STAR").as("a").out("followedBy").aggregate("stash").
        in("followedBy").where(P.neq("a").and(P.not(P.within("stash")))).
        groupCount().
        unfold().
        project("x", "y", "z").
          by(__.select(Column.keys).values("name")).
          by(__.select(Column.keys).values("performances")).
          by(__.select(Column.values)).
        order().
          by(__.select("z"), Order.desc).
          by(__.select("y"), Order.asc).
        limit(5).local(aggregate("m")).select("x")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | LET IT GROW |
      | UNCLE JOHNS BAND |
      | I KNOW YOU RIDER |
      | SHIP OF FOOLS |
      | GOOD LOVING |

  Scenario: g_V_classic_recommendation_ranked
    Given the grateful graph
    And the traversal of
      """
      g.V().has("name", "DARK STAR").as("a").out("followedBy").aggregate("stash").
        in("followedBy").where(P.neq("a").and(P.not(P.within("stash")))).
        groupCount().
        unfold().
        project("x", "y", "z").
          by(__.select(Column.keys).values("name")).
          by(__.select(Column.keys).values("performances")).
          by(__.select(Column.values)).
        order().
          by(__.select("z"), Order.desc).
          by(__.select("y"), Order.asc).
        limit(5).local(aggregate("m"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"x":"LET IT GROW","y":"d[276].i","z":"d[21].l"}] |
      | m[{"x":"UNCLE JOHNS BAND","y":"d[332].i","z":"d[20].l"}] |
      | m[{"x":"I KNOW YOU RIDER","y":"d[550].i","z":"d[20].l"}] |
      | m[{"x":"SHIP OF FOOLS","y":"d[225].i","z":"d[18].l"}] |
      | m[{"x":"GOOD LOVING","y":"d[428].i","z":"d[18].l"}] |

  Scenario: g_V_songsTwoStepsAfterDarkStar
    Given the grateful graph
    And the intent
      """
      Starting from DARK STAR, look at the songs that follow the songs directly following it, leaving out any song that
      also directly follows DARK STAR. Count how many times each song is reached this way and return the five most
      frequent as one map from song name to count.
      """
    And the traversal of
      """
      g.V().has("song","name","DARK STAR").out("followedBy").aggregate("direct").out("followedBy").where(P.without("direct")).groupCount().by("name").order(Scope.local).by(Column.values,Order.desc).limit(Scope.local,5)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"AROUND AND AROUND":"d[21].l","GOING DOWN THE ROAD FEELING BAD":"d[21].l","UNCLE JOHNS BAND":"d[19].l","GOOD LOVING":"d[19].l","DARK STAR":"d[18].l"}] |

  @MultiProperties @DataSet
  Scenario: g_kubernetesHelpForPlatformProjects
    Given the empty graph
    And the intent
      """
      Which employees from outside the Platform team know "kubernetes" and work on a project the Platform team owns?
      Return their names.
      """
    And the graph initializer of
      """
      g.addV("team").property("name","Platform").as("platform").
        addV("team").property("name","Data").as("data").
        addV("project").property("name","Atlas").property("priority",1).as("atlas").
        addV("project").property("name","Beacon").property("priority",2).as("beacon").
        addV("project").property("name","Comet").property("priority",3).as("comet").
        addV("employee").property("name","Ines Duarte").property(Cardinality.set,"skills","java").as("ines").
        addV("employee").property("name","Raj Patel").property(Cardinality.set,"skills","kubernetes").property(Cardinality.set,"skills","go").as("raj").
        addV("employee").property("name","Mei Tanaka").property(Cardinality.set,"skills","kubernetes").as("mei").
        addV("employee").property("name","Kofi Mensah").property(Cardinality.set,"skills","kubernetes").as("kofi").
        addE("owns").from("platform").to("atlas").addE("owns").from("platform").to("beacon").addE("owns").from("data").to("comet").
        addE("memberOf").from("ines").to("platform").addE("memberOf").from("raj").to("data").addE("memberOf").from("mei").to("data").addE("memberOf").from("kofi").to("data").
        addE("worksOn").from("ines").to("atlas").property("allocation",1.0d).
        addE("worksOn").from("raj").to("atlas").property("allocation",0.5d).
        addE("worksOn").from("raj").to("beacon").property("allocation",0.25d).
        addE("worksOn").from("mei").to("comet").property("allocation",1.0d).
        addE("worksOn").from("kofi").to("beacon").property("allocation",0.5d)
      """
    And the traversal of
      """
      g.V().has("team","name","Platform").as("t").out("owns").in("worksOn").not(__.out("memberOf").where(P.eq("t"))).has("skills","kubernetes").dedup().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Raj Patel |
      | Kofi Mensah |
