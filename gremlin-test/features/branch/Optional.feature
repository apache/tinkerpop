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
    And using the parameter v2Id is "v[vadas].id"
    And the traversal of
      """
      g.V(v2Id).optional(__.out("knows"))
      """
    When iterated to list
    Then the result should be unordered
      | v[vadas] |

  Scenario: g_VX2X_optionalXinXknowsXX
    Given the modern graph
    And using the parameter v2Id is "v[vadas].id"
    And the traversal of
      """
      g.V(v2Id).optional(__.in("knows"))
      """
    When iterated to list
    Then the result should be unordered
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
