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

@StepClassFilter @StepFilter
Feature: Step - filter()

  Scenario: g_V_filterXisX0XX
    Given the modern graph
    And the traversal of
      """
      g.V().filter(__.is(0))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_filterXconstantX0XX
    Given the modern graph
    And the traversal of
      """
      g.V().filter(__.constant(0))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter]  |

  Scenario: g_V_filterXhasXlang_javaXX
    Given the modern graph
    And the traversal of
      """
      g.V().filter(__.has("lang","java"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[ripple] |
      | v[lop]  |

  Scenario: g_VX1X_filterXhasXage_gtX30XXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).filter(__.has("age",P.gt(30)))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VX2X_filterXhasXage_gtX30XXX
    Given the modern graph
    And using the parameter vid2 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid2).filter(__.has("age",P.gt(30)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

  Scenario: g_VX1X_out_filterXhasXage_gtX30XXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().filter(__.has("age",P.gt(30)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

  Scenario: g_V_filterXhasXname_startingWithXm_or_pXX
    Given the modern graph
    And the traversal of
      """
      g.V().filter(__.has("name", TextP.startingWith("m").or(TextP.startingWith("p"))))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[peter]  |

  Scenario: g_E_filterXisX0XX
    Given the modern graph
    And the traversal of
      """
      g.E().filter(__.is(0))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_E_filterXconstantX0XX
    Given the modern graph
    And the traversal of
      """
      g.E().filter(__.constant(0))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-created->lop] |
      | e[marko-knows->josh] |
      | e[marko-knows->vadas] |
      | e[peter-created->lop] |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |