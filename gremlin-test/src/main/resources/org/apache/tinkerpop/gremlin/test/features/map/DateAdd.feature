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

# todo: re-enable after datetime is implemented
@StepClassMap @StepDateAdd
Feature: Step - dateAdd()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstrXX_dateAddXDT_hour_2X
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-02T00:00:00Z')).dateAdd(DT.hour, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-02T02:00:00Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstrXX_dateAddXhour_2X
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-02T00:00:00Z')).dateAdd(hour, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-02T02:00:00Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstrXX_dateAddXhour_1X
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-02T00:00:00Z')).dateAdd(hour, -1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-01T23:00:00Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstrXX_dateAddXminute_10X
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-02T00:00:00Z')).dateAdd(minute, 10)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-02T00:10:00Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstrXX_dateAddXsecond_20X
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-02T00:00:00Z')).dateAdd(second, 20)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-02T00:00:20Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstrXX_dateAddXday_11X
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-09-06T00:00:00Z')).dateAdd(day, 11)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-09-17T00:00:00Z] |
