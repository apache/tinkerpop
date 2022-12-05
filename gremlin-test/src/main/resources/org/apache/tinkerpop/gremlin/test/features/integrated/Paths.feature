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

@StepClassIntegrated
Feature: Step - paths

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_playlist_paths
    Given the grateful graph
    And the traversal of
      """
      g.V().has("name", "Bob_Dylan").
        in("sungBy").order().by('name').as("a").
        repeat(__.out().order().by('name').simplePath().from("a")).
          until(__.out("writtenBy").has("name", "Johnny_Cash")).limit(1).as("b").
        repeat(__.out().order().by('name').as("c").simplePath().from("b").to("c")).
          until(__.out("sungBy").has("name", "Grateful_Dead")).limit(1).
        path().from("a").unfold().
        project("song", "artists").
          by("name").
          by(__.coalesce(__.out("sungBy", "writtenBy").dedup().values("name").order(), __.constant("Unknown")).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"song": "CHIMES OF FREEDOM", "artists": ["Bob_Dylan"]}] |
      | m[{"song": "QUEEN JANE", "artists": ["Unknown"]}] |
      | m[{"song": "ALTHEA", "artists": ["Garcia","Hunter"]}] |
      | m[{"song": "BIG RIVER", "artists": ["Johnny_Cash","Weir"]}] |
      | m[{"song": "HES GONE", "artists": ["Garcia","Hunter"]}] |
      | m[{"song": "CAUTION", "artists": ["Grateful_Dead"]}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_shortestpath
    Given the modern graph
    And the traversal of
      """
      g.V().as("v").both().as("v").
        project("src", "tgt", "p").
          by(__.select(first, "v")).
          by(__.select(last, "v")).
          by(__.select(Pop.all, "v")).as("triple").
        group("x").
          by(__.select("src", "tgt")).
          by(__.select("p").fold()).select("tgt").barrier().
        repeat(__.both().as("v").
                project("src", "tgt", "p").
                  by(__.select(first, "v")).
                  by(__.select(last, "v")).
                  by(__.select(Pop.all, "v")).as("t").
                filter(__.select(Pop.all, "p").count(local).as("l").
                       select(Pop.last, "t").select(Pop.all, "p").dedup(Scope.local).count(Scope.local).where(P.eq("l"))).
                select(Pop.last, "t").
                not(__.select(Pop.all, "p").as("p").count(local).as("l").
                    select(Pop.all, "x").unfold().filter(select(keys).where(P.eq("t")).by(select("src", "tgt"))).
                    filter(__.select(Column.values).unfold().or(__.count(Scope.local).where(P.lt("l")), __.where(P.eq("p"))))).
                barrier().
                group("x").
                  by(__.select("src", "tgt")).
                  by(__.select(Pop.all, "p").fold()).select("tgt").barrier()).
        cap("x").select(Column.values).unfold().unfold().map(__.unfold().values("name").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[josh,marko,vadas]|
      | l[ripple,josh,lop]|
      | l[josh,lop]|
      | l[peter,lop,marko]|
      | l[ripple,josh,marko]|
      | l[josh,marko]|
      | l[marko,lop]|
      | l[lop,marko]|
      | l[josh,lop,peter]|
      | l[peter,lop,josh]|
      | l[vadas,marko]|
      | l[ripple,josh]|
      | l[marko]|
      | l[josh]|
      | l[ripple]|
      | l[josh,ripple]|
      | l[peter,lop]|
      | l[vadas,marko,josh]|
      | l[lop,josh,ripple]|
      | l[marko,josh]|
      | l[lop,marko,vadas]|
      | l[lop]|
      | l[peter]|
      | l[vadas]|
      | l[marko,josh,ripple]|
      | l[marko,vadas]|
      | l[vadas,marko,lop]|
      | l[lop,peter]|
      | l[lop,josh]|
      | l[marko,lop,peter]|
