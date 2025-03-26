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

@StepClassFilter @StepNone
Feature: Step - none()

  Scenario: g_V_valuesXageX_noneXgtX32XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").none(P.gt(32))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_whereXisXP_gtX33XXX_fold_noneXlteX33XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").where(__.is(P.gt(33))).fold().none(P.lte(33))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[35].i] |

  Scenario: g_V_valuesXageX_order_byXdescX_fold_noneXltX10XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().by(desc).fold().none(P.lt(10))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[35].i,d[32].i,d[29].i,d[27].i] |

  Scenario: g_V_valuesXageX_order_byXdescX_fold_noneXgtX30XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().by(desc).fold().none(P.gt(30))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXabc_bcdX_noneXeqXbcdXX
    Given the empty graph
    And the traversal of
      """
      g.inject(["abc","bcd"]).none(P.eq("bcd"))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXbcd_bcdX_noneXeqXabcXX
    Given the empty graph
    And using the parameter xx1 defined as "l[bcd,bcd]"
    And the traversal of
      """
      g.inject(xx1).none(P.eq("abc"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[bcd,bcd] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnull_bcdX_noneXP_eqXabcXX
    Given the empty graph
    And using the parameter xx1 defined as "l[null,bcd]"
    And the traversal of
      """
      g.inject(xx1).none(P.eq("abc"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[null,bcd] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5_8_10_10_7X_noneXltX7XX
    Given the empty graph
    And using the parameter xx1 defined as "l[d[5].i,d[8].i,d[10].i]"
    And using the parameter xx2 defined as "l[d[10].i,d[7].i]"
    And the traversal of
      """
      g.inject(xx1,xx2).none(P.lt(7))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[10].i,d[7].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_noneXeqXnullXX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).none(P.eq(null))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX7X_noneXeqX7XX
    Given the empty graph
    And the traversal of
      """
      g.inject(7).none(P.eq(7))
      """
    When iterated to list
    Then the result should be empty


  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnull_1_emptyX_noneXeqXnullXX
    Given the empty graph
    And using the parameter xx1 defined as "l[null,1]"
    And using the parameter xx2 defined as "l[]"
    And the traversal of
      """
      g.inject(xx1,xx2).none(P.eq(null))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnull_nullX_noneXnotXnullXX
    Given the empty graph
    And using the parameter xx1 defined as "l[null,null]"
    And the traversal of
      """
      g.inject(xx1).none(P.neq(null))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[null,null] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3_threeX_noneXeqX3XX
    Given the empty graph
    And using the parameter xx1 defined as "l[d[3].i,three]"
    And the traversal of
      """
      g.inject(xx1).none(P.eq(3))
      """
    When iterated to list
    Then the result should be empty