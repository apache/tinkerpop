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
Feature: Step - miscellaneous

  Scenario: g_V_coworker
    Given the modern graph
    And using the parameter xx1 defined as "l[]"
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
        choose(__.in("created").where(eq("p1")), values("name"), constant(xx1)).
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

