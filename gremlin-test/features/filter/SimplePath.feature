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

Feature: Step - simplePath()

  Scenario: g_VX1X_outXcreatedX_inXcreatedX_simplePath
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("created").in("created").simplePath()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[peter] |

  Scenario: g_V_repeatXboth_simplePathX_timesX3X_path
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.both().simplePath()).times(3).path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],v[lop],v[josh],v[ripple]] |
      | p[v[marko],v[josh],v[lop],v[peter]] |
      | p[v[vadas],v[marko],v[lop],v[josh]] |
      | p[v[vadas],v[marko],v[lop],v[peter]] |
      | p[v[vadas],v[marko],v[josh],v[ripple]] |
      | p[v[vadas],v[marko],v[josh],v[lop]] |
      | p[v[lop],v[marko],v[josh],v[ripple]] |
      | p[v[lop],v[josh],v[marko],v[vadas]] |
      | p[v[josh],v[lop],v[marko],v[vadas]] |
      | p[v[josh],v[marko],v[lop],v[peter]] |
      | p[v[ripple],v[josh],v[lop],v[marko]] |
      | p[v[ripple],v[josh],v[lop],v[peter]] |
      | p[v[ripple],v[josh],v[marko],v[lop]] |
      | p[v[ripple],v[josh],v[marko],v[vadas]] |
      | p[v[peter],v[lop],v[marko],v[vadas]] |
      | p[v[peter],v[lop],v[marko],v[josh]] |
      | p[v[peter],v[lop],v[josh],v[ripple]] |
      | p[v[peter],v[lop],v[josh],v[marko]] |

  Scenario: g_V_asXaX_out_asXbX_out_asXcX_simplePath_byXlabelX_fromXbX_toXcX_path_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").out().as("c").simplePath().by(T.label).from("b").to("c").path().by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[marko,josh,ripple] |
      | p[marko,josh,lop]    |
