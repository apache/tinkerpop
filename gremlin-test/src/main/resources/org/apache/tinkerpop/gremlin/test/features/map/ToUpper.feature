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

@StepClassMap @StepToUpper
Feature: Step - toUpper()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXfeature_test_nullX_toUpper
    Given the empty graph
    And the traversal of
      """
      g.inject("feature", "tESt", null).toUpper()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | FEATURE |
      | TEST |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXfeature_test_nullX_toUpperXlocalX
    Given the empty graph
    And the traversal of
      """
      g.inject(["feature","tESt",null]).toUpper(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[FEATURE,TEST,null] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXListXa_bXX_toUpper
    Given the empty graph
    And the traversal of
      """
      g.inject(["a","b"]).toUpper()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The toUpper() step can only take string as argument"

  Scenario: g_V_valuesXnameX_toUpper
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").toUpper()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | MARKO |
      | VADAS |
      | LOP |
      | JOSH |
      | RIPPLE |
      | PETER |

  Scenario: g_V_valuesXnameX_toUpperXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").toUpper(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | MARKO |
      | VADAS |
      | LOP |
      | JOSH |
      | RIPPLE |
      | PETER |

  Scenario: g_V_valuesXnameX_order_fold_toUpperXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").order().fold().toUpper(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[JOSH,LOP,MARKO,PETER,RIPPLE,VADAS] |
