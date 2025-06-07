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

@StepClassMap @StepAsBool
Feature: Step - asBool()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1X_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject(1).asBool()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | true |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX0X_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject(0).asBool()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | false |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXstr_trueX_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject('true').asBool()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | true |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXinvalidstrX_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject('hello').asBool()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse"
