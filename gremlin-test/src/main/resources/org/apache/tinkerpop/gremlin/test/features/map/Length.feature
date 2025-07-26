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

@StepClassMap @StepLength
Feature: Step - length()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXfeature_test_nullX_length
    Given the empty graph
    And the traversal of
      """
      g.inject("feature", "test", null).length()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[7].i |
      | d[4].i |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXfeature_test_nullX_lengthXlocalX
    Given the empty graph
    And the traversal of
      """
      g.inject("feature", "test", null).length(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[7].i |
      | d[4].i |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXListXa_bXX_length
    Given the empty graph
    And the traversal of
      """
      g.inject(["a","b"]).length()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The length() step can only take string as argument"

  Scenario: g_V_valuesXnameX_length
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").length()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].i |
      | d[5].i |
      | d[3].i |
      | d[4].i |
      | d[6].i |
      | d[5].i |

  Scenario: g_V_valuesXnameX_order_fold_lengthXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").order().fold().length(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[4].i,d[3].i,d[5].i,d[5].i,d[6].i,d[5].i] |
