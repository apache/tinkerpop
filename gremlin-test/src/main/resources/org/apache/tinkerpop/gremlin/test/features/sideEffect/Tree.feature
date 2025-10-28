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

@StepClassSideEffect @StepTree
Feature: Step - tree()

  @GraphComputerVerificationReferenceOnly
  Scenario: g_VX1X_out_out_tree_byXnameX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().out().tree().by("name")
      """
    When iterated next
    Then the result should be a tree with a structure of
      """
      |--marko
         |--josh
            |--lop
            |--ripple
      """

  Scenario: g_VX1X_out_out_tree
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().out().tree()
      """
    When iterated next
    Then the result should be a tree with a structure of
      """
      |--v[marko]
         |--v[josh]
            |--v[lop]
            |--v[ripple]
      """

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_tree_byXageX
    Given the modern graph
    And the traversal of
      """
      g.V().out().tree().by("age")
      """
    When iterated next
    Then the result should be a tree with a structure of
      """
      |--d[32].i
      |--d[35].i
      |--d[29].i
         |--d[32].i
         |--d[27].i
      """

  @GraphComputerVerificationReferenceOnly
  Scenario: g_VX1X_out_out_treeXaX_byXnameX_both_both_capXaX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().out().tree("a").by("name").both().both().cap("a")
      """
    When iterated next
    Then the result should be a tree with a structure of
      """
      |--marko
         |--josh
            |--lop
            |--ripple
      """

  Scenario: g_VX1X_out_out_treeXaX_both_both_capXaX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().out().tree("a").both().both().cap("a")
      """
    When iterated next
    Then the result should be a tree with a structure of
      """
      |--v[marko]
         |--v[josh]
            |--v[lop]
            |--v[ripple]
      """

  @GraphComputerVerificationReferenceOnly
  Scenario: g_VX1X_out_out_tree_byXlabelX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().out().tree().by(T.label)
      """
    When iterated next
    Then the result should be a tree with a structure of
      """
      |--person
         |--person
            |--software
      """

  @GraphComputerVerificationReferenceOnly
  Scenario: g_VX1X_out_out_treeXaX_byXlabelX_both_both_capXaX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().out().tree("a").by(T.label).both().both().cap("a")
      """
    When iterated next
    Then the result should be a tree with a structure of
      """
      |--person
         |--person
            |--software
      """

  Scenario: g_VX1X_out_out_out_tree
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().out().tree()
      """
    When iterated next
    Then the result should be a tree with a structure of
      """
      """

  Scenario: g_VX1X_outE_inV_bothE_otherV_tree
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE().inV().bothE().otherV().tree()
      """
    When iterated next
    Then the result should be a tree with a structure of
      """
      |--v[marko]
         |--e[marko-knows->vadas]
            |--v[vadas]
               |--e[marko-knows->vadas]
                  |--v[marko]
         |--e[marko-knows->josh]
            |--v[josh]
               |--e[josh-created->ripple]
                  |--v[ripple]
               |--e[josh-created->lop]
                  |--v[lop]
               |--e[marko-knows->josh]
                  |--v[marko]
         |--e[marko-created->lop]
            |--v[lop]
               |--e[marko-created->lop]
                  |--v[marko]
               |--e[josh-created->lop]
                  |--v[josh]
               |--e[peter-created->lop]
                  |--v[peter]
      """

  @GraphComputerVerificationReferenceOnly
  Scenario: g_VX1X_outE_inV_bothE_otherV_tree_byXnameX_byXlabelX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE().inV().bothE().otherV().tree().by("name").by(T.label)
      """
    When iterated next
    Then the result should be a tree with a structure of
      """
      |--marko
         |--knows
            |--vadas
               |--knows
                  |--marko
            |--josh
               |--created
                  |--ripple
                  |--lop
               |--knows
                  |--marko
         |--created
            |--lop
               |--created
                  |--marko
                  |--josh
                  |--peter
      """

  Scenario: g_V_out_treeXaX_selectXaX_countXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().out().tree("a").select("a").count(local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l |
      | d[3].l |
      | d[3].l |
      | d[3].l |
      | d[3].l |
      | d[3].l |

  @InsertionOrderingRequired
  Scenario: g_V_out_order_byXnameX_localXtreeXaX_selectXaX_countXlocalXX
    Given the modern graph
    And the traversal of
      """
      g.V().out().local(tree("a").select("a").count(local))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |
      | d[1].l |
      | d[1].l |
      | d[2].l |
      | d[2].l |
      | d[3].l |
