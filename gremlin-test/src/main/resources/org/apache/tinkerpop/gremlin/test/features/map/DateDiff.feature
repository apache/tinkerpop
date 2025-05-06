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

@StepClassMap @StepDateDiff
Feature: Step - dateDiff()

  # Tests using datetime

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstr1XX_dateDiffXdatetimeXstr2XX
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-02T00:00:00Z')).dateDiff(datetime('2023-08-09T00:00:00Z'))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[-604800].l |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstr1XX_dateDiffXconstantXdatetimeXstr2XXX
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-08T00:00:00Z')).dateDiff(constant(datetime('2023-08-01T00:00:00Z')))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[604800].l |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstr1XX_dateDiffXinjectXdatetimeXstr2XXX
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-08T00:00:00Z')).dateDiff(inject(datetime('2023-10-11T00:00:00Z')))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].l |

  # Tests using DateTime

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXDateTimeXstr1XX_dateDiffXDateTimeXstr2XX
    Given the empty graph
    And the traversal of
      """
      g.inject(DateTime('2023-08-02T00:00:00Z')).dateDiff(DateTime('2023-08-09T00:00:00Z'))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[-604800].l |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXDateTimeXstr1XX_dateDiffXconstantXDateTimeXstr2XXX
    Given the empty graph
    And the traversal of
      """
      g.inject(DateTime('2023-08-08T00:00:00Z')).dateDiff(constant(DateTime('2023-08-01T00:00:00Z')))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[604800].l |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXDateTimeXstr1XX_dateDiffXinjectXDateTimeXstr2XXX
    Given the empty graph
    And the traversal of
      """
      g.inject(DateTime('2023-08-08T00:00:00Z')).dateDiff(inject(DateTime('2023-10-11T00:00:00Z')))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].l |