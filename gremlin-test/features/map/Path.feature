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

Feature: Step - path()

  Scenario: g_VX1X_name_path
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).values("name").path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],marko] |

  Scenario: g_VX1X_out_path_byXageX_byXnameX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().path().by("age").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[d[29].i,lop] |
      | p[d[29].i,vadas] |
      | p[d[29].i,josh] |

  Scenario: g_V_repeatXoutX_timesX2X_path_byXitX_byXnameX_byXlangX
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.out()).times(2).path().by().by("name").by("lang")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],josh,java] |
      | p[v[marko],josh,java] |

  Scenario: g_V_out_out_path_byXnameX_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().path().by("name").by("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[marko,d[32].i,ripple] |
      | p[marko,d[32].i,lop] |

  Scenario: g_V_asXaX_hasXname_markoX_asXbX_hasXage_29X_asXcX_path
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").has("name", "marko").as("b").has("age", 29).as("c").path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko]] |

  Scenario: g_VX1X_outEXcreatedX_inV_inE_outV_path
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("created").inV().inE().outV().path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],e[marko-created->lop],v[lop],e[marko-created->lop],v[marko]] |
      | p[v[marko],e[marko-created->lop],v[lop],e[josh-created->lop],v[josh]] |
      | p[v[marko],e[marko-created->lop],v[lop],e[peter-created->lop],v[peter]] |

  Scenario: g_V_asXaX_out_asXbX_out_asXcX_path_fromXbX_toXcX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").out().as("c").path().from("b").to("c").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[josh,ripple] |
      | p[josh,lop] |
