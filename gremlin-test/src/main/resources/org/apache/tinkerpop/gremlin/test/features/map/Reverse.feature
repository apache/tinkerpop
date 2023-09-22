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

@StepClassMap @StepReverse
Feature: Step - reverse()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXfeature_test_nullX_reverse
    Given the empty graph
    And the traversal of
      """
      g.inject("feature", "test one", null).reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | erutaef |
      | eno tset |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXListXa_bXX_reverse
    Given the empty graph
    And using the parameter xx1 defined as "l[a,b]"
    And the traversal of
    """
    g.inject(xx1).reverse()
    """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The reverse() step can only take string as argument"

  Scenario: g_V_valuesXnameX_reverse
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | okram |
      | sadav |
      | pol |
      | hsoj |
      | elppir |
      | retep |
