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

@StepClassData @DataChar
Feature: Data - CHAR

  @GraphComputerVerificationInjectionNotSupported @SupportsChar
  Scenario: g_injectXaX
    Given the empty graph
    And the traversal of
      """
      g.inject("a"c)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | char[a] |

  @GraphComputerVerificationInjectionNotSupported @SupportsChar
  Scenario: g_injectXescaped_quoteX
    Given the empty graph
    And the traversal of
      """
      g.inject("\""c)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | char["] |

  @GraphComputerVerificationInjectionNotSupported @SupportsChar
  Scenario: g_injectXunicodeX
    Given the empty graph
    And the traversal of
      """
      g.inject("\u00E9"c)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | char[é] |

  @SupportsChar
  Scenario: g_valuesXinitialX_isXtypeOfXGType_CHARXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("initial", "a"c)
      """
    And the traversal of
      """
      g.V().values("initial").is(P.typeOf(GType.CHAR))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | char[a] |

  @GraphComputerVerificationInjectionNotSupported @SupportsChar
  Scenario: g_injectXaX_isXeqXaXX
    Given the empty graph
    And the traversal of
      """
      g.inject("a"c).is(P.eq("a"c))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | char[a] |
