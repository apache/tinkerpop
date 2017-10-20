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

Feature: Step - filter()

  Scenario: g_V_filterXfalseX
    Given the modern graph
    And using the parameter l1 defined as "c[false]"
    And the traversal of
      """
      g.V().filter(l1)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_filterXtrueX
    Given the modern graph
    And using the parameter l1 defined as "c[true]"
    And the traversal of
      """
      g.V().filter(l1)
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter]  |

  Scenario: g_V_filterXlang_eq_javaX
    Given the modern graph
    And using the parameter l1 defined as "c[it.get().property('lang').orElse('none').equals('java')]"
    And the traversal of
      """
      g.V().filter(l1)
      """
    When iterated to list
    Then the result should be unordered
      | v[ripple] |
      | v[lop]  |

  Scenario: g_VX1X_out_filterXage_gt_30X
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter l1 defined as "c[it.get().property('age').orElse(0) > 30]"
    And the traversal of
      """
      g.V(v1Id).out().filter(l1)
      """
    When iterated to list
    Then the result should be unordered
      | v[josh] |

  Scenario: g_V_filterXname_startsWith_m_OR_name_startsWith_pX
    Given the modern graph
    And using the parameter l1 defined as "c[{name = it.get().value('name'); name.startsWith('m') || name.startsWith('p')}]"
    And the traversal of
      """
      g.V().filter(l1)
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |
      | v[peter]  |

  Scenario: g_E_filterXfalseX
    Given the modern graph
    And using the parameter l1 defined as "c[false]"
    And the traversal of
      """
      g.E().filter(l1)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_E_filterXtrueX
    Given the modern graph
    And using the parameter l1 defined as "c[true]"
    And the traversal of
      """
      g.E().filter(l1)
      """
    When iterated to list
    Then the result should be unordered
      | e[marko-created->lop] |
      | e[marko-knows->josh] |
      | e[marko-knows->vadas] |
      | e[peter-created->lop] |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |