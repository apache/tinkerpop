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

Feature: Step - count()

  Scenario: g_V_count
    Given the modern graph
    And the traversal of
      """
      g.V().count()
      """
    When iterated to list
    Then the result should be ordered
      | d[6] |

  Scenario: g_V_out_count
    Given the modern graph
    And the traversal of
      """
      g.V().out().count()
      """
    When iterated to list
    Then the result should be ordered
      | d[6] |

  Scenario: g_V_both_both_count
    Given the modern graph
    And the traversal of
      """
      g.V().both().both().count()
      """
    When iterated to list
    Then the result should be ordered
      | d[30] |

  Scenario: g_V_fold_countXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().count(Scope.local)
      """
    When iterated to list
    Then the result should be ordered
      | d[6] |

  Scenario: g_V_hasXnoX_count
    Given the modern graph
    And the traversal of
      """
      g.V().has("no").count()
      """
    When iterated to list
    Then the result should be ordered
      | d[0] |