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

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstr1XX_dateDiffXdatetimeXstr2XX
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-02T00:00:00Z'), DateTime('2023-08-02T00:00:00Z')).dateDiff(datetime('2023-08-09T00:00:00Z'))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[-604800000].l |
      | d[-604800000].l |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstr1XX_dateDiffXconstantXdatetimeXstr2XXX
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-08T00:00:00Z'), DateTime('2023-08-08T00:00:00Z')).dateDiff(constant(datetime('2023-08-01T00:00:00Z')))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[604800000].l |
      | d[604800000].l |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstr1XX_dateDiffXinjectXdatetimeXstr2XXX
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-08T00:00:00Z'), DateTime('2023-08-08T00:00:00Z')).dateDiff(inject(datetime('2023-10-11T00:00:00Z')))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].l |
      | d[0].l |

  Scenario: g_V_valuesXbirthdayX_asDate_dateDiffXdatetimeX19700101T0000ZXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "alice").property("birthday", "1596326400000").
        addV("person").property("name", "john").property("birthday", "597715200000").
        addV("person").property("name","charlie").property("birthday", "1012521600000").
        addV("person").property("name", "suzy").property("birthday", "-131587200000")
      """
    And the traversal of
      """
      g.V().values("birthday").asNumber().asDate().dateDiff(datetime("1970-01-01T00:00Z"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[597715200000].l |
      | d[1596326400000].l |
      | d[1012521600000].l |
      | d[-131587200000].l |

  Scenario: g_V_hasXname_aliceX_valuesXbirthdayX_asDate_dateDiffXconstantXnullXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "alice").property("birthday", 1596326400000)
      """
    And the traversal of
      """
      g.V().has("name", "alice").values("birthday").asDate().dateDiff(__.constant(null))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1596326400000].l |