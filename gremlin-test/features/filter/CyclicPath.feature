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

@StepClassFilter @StepCyclicPath
Feature: Step - cyclicPath()

  Scenario: g_VX1X_outXcreatedX_inXcreatedX_cyclicPath
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("created").in("created").cyclicPath()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_VX1X_outXcreatedX_inXcreatedX_cyclicPath_path
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("created").in("created").cyclicPath().path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],v[lop],v[marko]] |

  Scenario: g_VX1X_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_cyclicPath_fromXaX_toXbX_path
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").out("created").as("b").in("created").as("c").cyclicPath().from("a").to("b").path()
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX0X_V_both_coalesceXhasXname_markoX_both_constantX0XX_cyclicPath_path
    Given the modern graph
    And the traversal of
      """
      g.inject(0).V().both().coalesce(has('name','marko').both(),constant(0)).cyclicPath().path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[d[0].i,v[marko],v[lop],d[0].i] |
      | p[d[0].i,v[marko],v[vadas],d[0].i] |
      | p[d[0].i,v[marko],v[josh],d[0].i] |
      | p[d[0].i,v[vadas],v[marko],v[vadas]] |
      | p[d[0].i,v[lop],v[marko],v[lop]] |
      | p[d[0].i,v[lop],v[josh],d[0].i] |
      | p[d[0].i,v[lop],v[peter],d[0].i] |
      | p[d[0].i,v[josh],v[ripple],d[0].i] |
      | p[d[0].i,v[josh],v[lop],d[0].i] |
      | p[d[0].i,v[josh],v[marko],v[josh]] |
      | p[d[0].i,v[ripple],v[josh],d[0].i] |
      | p[d[0].i,v[peter],v[lop],d[0].i] |