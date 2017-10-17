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

Feature: Step - drop()

  Scenario: g_V_drop
    Given the empty graph
    And the graph initializer of
      """
      g.addV().as("a").addV().as("b").addE("knows").to("a")
      """
    And the traversal of
      """
      g.V().drop()
      """
    When iterated to list
    Then the result should be empty
    And the graph should return 0 for count of "g.V()"
    And the graph should return 0 for count of "g.E()"

  Scenario: g_V_outE_drop
    Given the empty graph
    And the graph initializer of
      """
      g.addV().as("a").addV().as("b").addE("knows").to("a")
      """
    And the traversal of
      """
      g.V().outE().drop()
      """
    When iterated to list
    Then the result should be empty
    And the graph should return 2 for count of "g.V()"
    And the graph should return 0 for count of "g.E()"

  Scenario: g_V_properties_drop
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("name","bob").addV().property("name","alice")
      """
    And the traversal of
      """
      g.V().properties().drop()
      """
    When iterated to list
    Then the result should be empty
    And the graph should return 2 for count of "g.V()"
    And the graph should return 0 for count of "g.V().properties()"