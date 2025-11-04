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

@StepClassMap @StepAsDate
Feature: Step - asDate()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXstrX_asDate
    Given the empty graph
    And the traversal of
      """
      g.inject("2023-08-02T00:00:00Z").asDate()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-02T00:00:00Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXstr_offsetX_asDate
    Given the empty graph
    And the traversal of
      """
      g.inject("2023-08-02T00:00:00-07:00").asDate()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-02T00:00:00-07:00] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1694017707000X_asDate
    Given the empty graph
    And the traversal of
      """
      g.inject(1694017707000).asDate()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-09-06T16:28:27Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1694017708000LX_asDate
    Given the empty graph
    And the traversal of
      """
      g.inject(1694017708000L).asDate()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-09-06T16:28:28Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1694017709000dX_asDate
    Given the empty graph
    And the traversal of
      """
      g.inject(1694017709000.1d).asDate()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse"

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_2X_asDate
    Given the empty graph
    And the traversal of
      """
      g.inject([1,2]).asDate()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse"

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_asDate
    Given the empty graph
    And the traversal of
      """
      g.inject(null).asDate()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse"

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXinvalidstrX_asDate
    Given the empty graph
    And the traversal of
      """
      g.inject('This String is not an ISO 8601 Date').asDate()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse"
    
  #  asDate should be round-trippable with asNumber
  Scenario: g_V_valuesXbirthdayX_asDate_asNumber_asDate
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "alice").property("birthday", "2020-08-02").
        addV("person").property("name", "john").property("birthday", "1988-12-10").
        addV("person").property("name","charlie").property("birthday", "2002-02-01").
        addV("person").property("name", "suzy").property("birthday", "1965-10-31")
      """
    And the traversal of
      """
      g.V().values("birthday").asDate().asNumber().asDate()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2020-08-02T00:00:00Z] |
      | dt[1988-12-10T00:00:00Z] |
      | dt[2002-02-01T00:00:00Z] |
      | dt[1965-10-31T00:00:00Z] |