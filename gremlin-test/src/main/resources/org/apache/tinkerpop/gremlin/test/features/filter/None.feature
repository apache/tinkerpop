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

@StepClassMap @StepNone
Feature: Step - none()

  Scenario: g_V_count_none
    Given the modern graph
    And the traversal of
      """
      g.V().count().none()
      """
    When iterated to list
    Then the result should be empty

 Scenario: g_V_hasLabelXpersonX_none
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").none()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VX1X_outXcreatedX_none
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("created").none()
      """
    When iterated to list
    Then the result should be empty


