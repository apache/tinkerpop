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

@StepClassFilter @StepAny
Feature: Step - any()

  Scenario: g_V_valuesXageX_anyXgtX32XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").any(P.gt(32))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_order_byXdescX_fold_anyXeqX29XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().by(desc).fold().any(P.eq(29))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[35].i,d[32].i,d[29].i,d[27].i] |

  Scenario: g_V_valuesXageX_order_byXdescX_fold_anyXgtX10XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().by(desc).fold().any(P.gt(10))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[35].i,d[32].i,d[29].i,d[27].i] |

  Scenario: g_V_valuesXageX_order_byXdescX_fold_anyXgtX42XX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().by(desc).fold().any(P.gt(42))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXabc_cdeX_anyXeqXbcdXX
    Given the empty graph
    And the traversal of
      """
      g.inject(["abc","cde"]).any(P.eq("bcd"))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXabc_bcdX_anyXeqXbcdXX
    Given the empty graph
    And the traversal of
      """
      g.inject(["abc","bcd"]).any(P.eq("bcd"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[abc,bcd] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnull_abcX_anyXTextP_startingWithXaXX
    Given the empty graph
    And the traversal of
      """
      g.inject([null,"abc"]).any(TextP.startingWith("a"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[null,abc] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5_8_10_10_7X_anyXeqX7XX
    Given the empty graph
    And the traversal of
      """
      g.inject([5,8,10],[10,7]).any(P.eq(7))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[10].i,d[7].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_anyXeqXnullXX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).any(P.eq(null))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX7X_anyXeqX7XX
    Given the empty graph
    And the traversal of
      """
      g.inject(7).any(P.eq(7))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnull_nullX_anyXeqXnullXX
    Given the empty graph
    And the traversal of
      """
      g.inject([null,null]).any(P.eq(null))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[null,null] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3_threeX_anyXeqX3XX
    Given the empty graph
    And the traversal of
      """
      g.inject([3,"three"]).any(P.eq(3))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[3].i,three] |