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

@StepClassMap @StepReplace
Feature: Step - replace()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXthat_this_test_nullX_replaceXh_jX
    Given the empty graph
    And the traversal of
      """
      g.inject("that", "this", "test", null).replace("h", "j")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | tjat |
      | tjis |
      | test |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXthat_this_test_nullX_fold_replaceXlocal_h_jX
    Given the empty graph
    And the traversal of
      """
      g.inject("that", "this", "test", null).fold().replace(Scope.local, "h", "j")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[tjat,tjis,test,null] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXListXa_bXcX_replaceXa_bX
    Given the empty graph
    And the traversal of
      """
      g.inject(["a","b"]).replace("a", "b")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The replace() step can only take string as argument"

  Scenario: g_V_hasLabelXsoftwareX_valueXnameX_replaceXnull_iX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").replace(null, "g")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | ripple |

  Scenario: g_V_hasLabelXsoftwareX_valueXnameX_replaceXa_iX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").replace("p", "g")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | log |
      | riggle |

  Scenario: g_V_hasLabelXsoftwareX_valueXnameX_order_fold_replaceXloacl_a_iX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").order().fold().replace(Scope.local, "p", "g")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[log,riggle] |
