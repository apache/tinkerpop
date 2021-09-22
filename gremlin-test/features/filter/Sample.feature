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

@StepClassFilter @StepSample
Feature: Step - sample()

  Scenario: g_E_sampleX1X
    Given the modern graph
    And the traversal of
      """
      g.E().sample(1)
      """
    When iterated to list
    Then the result should have a count of 1

  Scenario: g_E_sampleX2X_byXweightX
    Given the modern graph
    And the traversal of
      """
      g.E().sample(2).by("weight")
      """
    When iterated to list
    Then the result should have a count of 2

  Scenario: g_V_localXoutE_sampleX1X_byXweightXX
    Given the modern graph
    And the traversal of
      """
      g.V().local(__.outE().sample(1).by("weight"))
      """
    When iterated to list
    Then the result should have a count of 3

  Scenario: g_V_group_byXlabelX_byXbothE_weight_sampleX2X_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by(T.label).by(__.bothE().values("weight").sample(2).fold())
      """
    When iterated to list
    Then nothing should happen because
      """
      The return value of this traversal is a map of samples weights in a list for each key which makes it
      especially hard to assert with the current test language established and the non-deterministic outcomes
      of sample().
      """

  Scenario: g_V_group_byXlabelX_byXbothE_weight_fold_sampleXlocal_5XX
    Given the modern graph
    And the traversal of
      """
      g.V().group().by(T.label).by(__.bothE().values("weight").fold().sample(Scope.local, 5))
      """
    When iterated to list
    Then nothing should happen because
      """
      The return value of this traversal is a map of samples weights in a list for each key which makes it
      especially hard to assert with the current test language established and the non-deterministic outcomes
      of sample().
      """

