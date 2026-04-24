# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@StepClassMap @StepVertex @StepE
Feature: Step - V(traversal) and E(traversal)

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_id_VXidentityX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).id().V(__.identity()).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1_vid2X_id_VXidentityX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid1, vid2).id().V(__.identity()).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | josh |

  # Use as()/select() to bookmark a vertex ID and return to it later via V(select()).
  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_id_asXbookmarkX_V_hasXname_joshX_VXselectXbookmarkXX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).id().as("bookmark").V().has("name","josh").V(__.select("bookmark")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_VXoutXknowsX_idX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).V(__.out("knows").id()).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |

  @GraphComputerVerificationMidENotSupported
  Scenario: g_VXvid1X_EXoutE_idX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).E(__.outE().id())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-created->lop] |
      | e[marko-knows->josh] |
      | e[marko-knows->vadas] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_injectX9999X_VXidentityX
    Given the modern graph
    And the traversal of
      """
      g.inject(9999).V(__.identity())
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationMidENotSupported
  Scenario: g_VXvid1X_outEXknowsX_hasXinV_name_vadasX_id_EXidentityX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("knows").filter(__.inV().has("name","vadas")).id().E(__.identity())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |

  @GraphComputerVerificationMidENotSupported
  Scenario: g_injectX9999X_EXidentityX
    Given the modern graph
    And the traversal of
      """
      g.inject(9999).E(__.identity())
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VXVXvid1X_idX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(__.V(vid1).id()).values("name")
      """
    When iterated to list
    Then the traversal will raise an error

  Scenario: g_EXVXvid1X_outE_idX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.E(__.V(vid1).outE().id())
      """
    When iterated to list
    Then the traversal will raise an error
