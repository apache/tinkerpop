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

@StepClassSideEffect @StepSack
Feature: Step - sack()

  Scenario: g_withSackXhelloX_V_outE_sackXassignX_byXlabelX_inV_sack
    Given the modern graph
    And the traversal of
      """
      g.withSack("hello").V().outE().sack(Operator.assign).by(T.label).inV().sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | created |
      | knows |
      | knows |
      | created |
      | created |
      | created |

  Scenario: g_withSackX0X_V_outE_sackXsumX_byXweightX_inV_sack_sum
    Given the modern graph
    And the traversal of
      """
      g.withSack(0.0d).V().outE().sack(Operator.sum).by("weight").inV().sack().sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3.5].d |

  Scenario: g_withSackX0X_V_repeatXoutE_sackXsumX_byXweightX_inVX_timesX2X_sack
    Given the modern graph
    And the traversal of
      """
      g.withSack(0.0d).V().repeat(__.outE().sack(Operator.sum).by("weight").inV()).times(2).sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[2.0].d |
      | d[1.4].d |

  @GraphComputerVerificationOneBulk
  Scenario: g_withBulkXfalseX_withSackX1_sumX_VX1X_localXoutEXknowsX_barrierXnormSackX_inVX_inXknowsX_barrier_sack
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.withBulk(false).withSack(1.0d, Operator.sum).V(vid1).local(__.outE("knows").barrier(Barrier.normSack).inV()).in("knows").barrier().sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1.0].d |

  @GraphComputerVerificationOneBulk
  Scenario: g_withBulkXfalseX_withSackX1_sumX_V_out_barrier_sack
    Given the modern graph
    And the traversal of
      """
      g.withBulk(false).withSack(1, Operator.sum).V().out().barrier().sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].i |
      | d[1].i |
      | d[1].i |
      | d[1].i |

  Scenario: g_withSackX1_sumX_VX1X_localXoutXknowsX_barrierXnormSackXX_inXknowsX_barrier_sack
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.withSack(1.0d, Operator.sum).V(vid1).local(__.out("knows").barrier(Barrier.normSack)).in("knows").barrier().sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1.0].d |
      | d[1.0].d |

  Scenario: g_V_sackXassignX_byXageX_sack
    Given the modern graph
    And the traversal of
      """
      g.V().sack(assign).by("age").sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_withSackXBigInteger_TEN_powX1000X_assignX_V_localXoutXknowsX_barrierXnormSackXX_inXknowsX_barrier_sack
    Given the modern graph
    And using the parameter xx1 defined as "d[10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000].n"
    And the traversal of
      """
      g.withSack(xx1, Operator.assign).V().local(__.out("knows").barrier(Barrier.normSack)).in("knows").barrier().sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.5].m |
      | d[0.5].m |

  Scenario: g_withSackXmap__map_cloneX_V_out_out_sackXmap_a_nameX_sack
    Given an unsupported test
    Then nothing should happen because
      """
      This test is bound pretty tightly to the JVM in that it requires a UnaryOperator cast to get the right
      withSack() method called. Not sure how that would work with a GLV.
      """

  Scenario: g_withSackX2X_V_sackXdivX_byXconstantX4_0XX_sack
    Given the modern graph
    And using the parameter xx1 defined as "d[4.0].d"
    And the traversal of
      """
      g.withSack(2).V().sack(Operator.div).by(__.constant(xx1)).sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.5].d |
      | d[0.5].d |
      | d[0.5].d |
      | d[0.5].d |
      | d[0.5].d |
      | d[0.5].d |