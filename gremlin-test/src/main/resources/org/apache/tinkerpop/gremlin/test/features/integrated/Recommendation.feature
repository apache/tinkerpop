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
Feature: Step - recommendation

  Scenario: g_V_classic_recommendation
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
        limit(5).aggregate(Scope.local,"m").select("x")
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
        limit(5).aggregate(Scope.local,"m")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"x":"LET IT GROW","y":"d[276].i","z":"d[21].l"}] |
      | m[{"x":"UNCLE JOHNS BAND","y":"d[332].i","z":"d[20].l"}] |
      | m[{"x":"I KNOW YOU RIDER","y":"d[550].i","z":"d[20].l"}] |
      | m[{"x":"SHIP OF FOOLS","y":"d[225].i","z":"d[18].l"}] |
      | m[{"x":"GOOD LOVING","y":"d[428].i","z":"d[18].l"}] |
