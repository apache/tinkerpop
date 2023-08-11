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
      g.inject("2022-08-02T00:00:00Z").asDate()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[1659398400] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1000X_asDate
    Given the empty graph
    And the traversal of
      """
      g.inject(1000).asDate()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[1] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX2000LX_asDate
    Given the empty graph
    And using the parameter xx1 defined as "d[2000].l"
    And the traversal of
      """
      g.inject(xx1).asDate()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3000dX_asDate
    Given the empty graph
    And using the parameter xx1 defined as "d[3000.1].d"
    And the traversal of
      """
      g.inject(xx1).asDate()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[3] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_2X_asDate
    Given the empty graph
    And using the parameter xx1 defined as "l[1,2]"
    And the traversal of
      """
      g.inject(xx1).asDate()
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