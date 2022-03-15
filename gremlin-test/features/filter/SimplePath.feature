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

@StepClassFilter @StepSimplePath
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

  @GraphComputerVerificationReferenceOnly
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

  @GraphComputerVerificationReferenceOnly
  Scenario: g_injectX0X_V_both_coalesceXhasXname_markoX_both_constantX0XX_simplePath_path
    Given the modern graph
    And the traversal of
      """
      g.inject(0).V().both().coalesce(has('name','marko').both(),constant(0)).simplePath().path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[d[0].i,v[vadas],v[marko],v[lop]] |
      | p[d[0].i,v[vadas],v[marko],v[josh]] |
      | p[d[0].i,v[lop],v[marko],v[vadas]] |
      | p[d[0].i,v[lop],v[marko],v[josh]] |
      | p[d[0].i,v[josh],v[marko],v[lop]] |
      | p[d[0].i,v[josh],v[marko],v[vadas]] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_both_asXaX_both_asXbX_simplePath_path_byXageX__fromXaX_toXbX
    Given the modern graph
    And the traversal of
      """
      g.V().both().as('a').both().as('b').simplePath().path().by('age').from('a').to('b')
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[d[29].i,d[32].i] |
      | p[d[29].i,d[27].i] |
      | p[d[29].i,d[32].i] |
      | p[d[32].i,d[29].i] |
      | p[d[29].i,d[27].i] |
      | p[d[32].i,d[29].i] |
