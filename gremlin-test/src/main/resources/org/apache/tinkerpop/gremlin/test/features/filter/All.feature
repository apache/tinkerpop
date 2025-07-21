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

@StepClassFilter @StepAll
Feature: Step - all()

  Scenario: g_V_valuesXageX_allXgtX32XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").all(P.gt(32))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_whereXisXP_gtX33XXX_fold_allXgtX33XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").where(__.is(P.gt(33))).fold().all(P.gt(33))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[35].i] |

  Scenario: g_V_valuesXageX_order_byXdescX_fold_allXgtX10XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().by(desc).fold().all(P.gt(10))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[35].i,d[32].i,d[29].i,d[27].i] |

  Scenario: g_V_valuesXageX_order_byXdescX_fold_allXgtX30XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().by(desc).fold().all(P.gt(30))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXabc_bcdX_allXeqXbcdXX
    Given the empty graph
    And the traversal of
      """
      g.inject(["abc","bcd"]).all(P.eq("bcd"))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXbcd_bcdX_allXeqXbcdXX
    Given the empty graph
    And the traversal of
      """
      g.inject(["bcd","bcd"]).all(P.eq("bcd"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[bcd,bcd] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnull_abcX_allXTextP_startingWithXaXX
    Given the empty graph
    And the traversal of
      """
      g.inject([null,"abc"]).all(TextP.startingWith("a"))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5_8_10_10_7X_allXgteX7XX
    Given the empty graph
    And the traversal of
      """
      g.inject([5,8,10],[10,7]).all(P.gte(7))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[10].i,d[7].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_allXeqXnullXX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).all(P.eq(null))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX7X_allXeqX7XX
    Given the empty graph
    And the traversal of
      """
      g.inject(7).all(P.eq(7))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnull_nullX_allXeqXnullXX
    Given the empty graph
    And the traversal of
      """
      g.inject([null,null]).all(P.eq(null))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[null,null] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3_threeX_allXeqX3XX
    Given the empty graph
    And the traversal of
      """
      g.inject([3,"three"]).all(P.eq(3))
      """
    When iterated to list
    Then the result should be empty