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

@StepClassData @DataDateTime
Feature: Data - DATETIME

  Scenario: g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("event").property("datetime", datetime("2023-08-08T00:00:00Z"))
      """
    And the traversal of
      """
      g.V().values("datetime").is(P.typeOf(GType.DATETIME))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-08T00:00:00Z] |

  Scenario: g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX_project_byXidentityX_byXdateAddXDT_dayX1XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("event").property("datetime", datetime("2023-08-08T00:00:00Z"))
      """
    And the traversal of
      """
      g.V().values("datetime").is(P.typeOf(GType.DATETIME)).project("original", "nextDay").by(identity()).by(dateAdd(DT.day, 1))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"original":"dt[2023-08-08T00:00:00Z]","nextDay":"dt[2023-08-09T00:00:00Z]"}] |

  Scenario: g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX_dateDiffXdatetimeX2023_08_10XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("event").property("datetime", datetime("2023-08-08T00:00:00Z"))
      """
    And the traversal of
      """
      g.V().values("datetime").is(P.typeOf(GType.DATETIME)).dateDiff(datetime("2023-08-08T00:00:30Z"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[-30000].l |

  Scenario: g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX_whereXisXgtXdatetimeX2020_01_01XXXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("event").property("datetime", datetime("2023-08-08T00:00:00Z"))
      """
    And the traversal of
      """
      g.V().values("datetime").is(P.typeOf(GType.DATETIME)).where(__.is(P.gt(datetime("2020-01-01T00:00:00Z"))))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-08T00:00:00Z] |

  Scenario: g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX_chooseXisXeqXdatetimeX2023_08_08XXXX_constantXmatchX_constantXnoMatchXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("event").property("datetime", datetime("2023-08-08T00:00:00Z"))
      """
    And the traversal of
      """
      g.V().values("datetime").is(P.typeOf(GType.DATETIME)).choose(__.is(P.eq(datetime("2023-08-08T00:00:00Z"))), constant("match"), constant("noMatch"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | match |

  Scenario: g_V_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX_localXaggregateXaX_capXaX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("event").property("datetime", datetime("2023-08-08T00:00:00Z"))
      """
    And the traversal of
      """
      g.V().values("datetime").is(P.typeOf(GType.DATETIME)).local(aggregate("a")).cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | dt[2023-08-08T00:00:00Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeX_isXtypeOfXGType_DATETIMEXX_aggregateXaX_capXaX
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime("2023-08-08T00:00:00Z")).is(P.typeOf(GType.DATETIME)).aggregate("a").cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | dt[2023-08-08T00:00:00Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeX_isXtypeOfXGType_DATETIMEXX_groupCount
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime("2023-08-08T00:00:00Z")).is(P.typeOf(GType.DATETIME)).groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"dt[2023-08-08T00:00:00Z]":"d[1].l"}] |
