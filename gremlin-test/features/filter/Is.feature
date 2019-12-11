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

Feature: Step - coin()

  Scenario: g_V_valuesXageX_isX32X
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(32)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32].i |

  Scenario: g_V_valuesXageX_isXlte_30X
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.lte(30))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |
      | d[29].i |

  Scenario: g_V_valuesXageX_isXgte_29X_isXlt_34X
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.gte(29)).is(P.lt(34))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[32].i |

  Scenario: g_V_whereXinXcreatedX_count_isX1XX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.in("created").count().is(1)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |

  Scenario: g_V_whereXinXcreatedX_count_isXgte_2XX_valuesXnameX
    Given the modern graph
    And using the parameter two defined as "d[2].l"
    And the traversal of
      """
      g.V().where(__.in("created").count().is(P.gte(two))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |