# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
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
      g.inject(datetime('2023-08-02T00:00:00Z'), DateTime('2023-08-02T00:00:00Z')).dateAdd(DT.hour, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-02T02:00:00Z] |
      | dt[2023-08-02T02:00:00Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstrXX_dateAddXhour_2X
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-02T00:00:00Z'), DateTime('2023-08-02T00:00:00Z')).dateAdd(hour, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-02T02:00:00Z] |
      | dt[2023-08-02T02:00:00Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstrXX_dateAddXhour_1X
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-02T00:00:00Z'), DateTime('2023-08-02T00:00:00Z')).dateAdd(hour, -1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-01T23:00:00Z] |
      | dt[2023-08-01T23:00:00Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstrXX_dateAddXminute_10X
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-02T00:00:00Z'), DateTime('2023-08-02T00:00:00Z')).dateAdd(minute, 10)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-02T00:10:00Z] |
      | dt[2023-08-02T00:10:00Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstrXX_dateAddXsecond_20X
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-08-02T00:00:00Z'), DateTime('2023-08-02T00:00:00Z')).dateAdd(second, 20)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-02T00:00:20Z] |
      | dt[2023-08-02T00:00:20Z] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXdatetimeXstrXX_dateAddXday_11X
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime('2023-09-06T00:00:00Z'), DateTime('2023-09-06T00:00:00Z')).dateAdd(day, 11)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-09-17T00:00:00Z] |
      | dt[2023-09-17T00:00:00Z] |

  Scenario: g_V_hasLabelXorderX_projectXnumber_dueX_byXnumberX_byXvaluesXplacedX_asDate_dateAddXday_30XX
    Given the empty graph
    And the intent
      """
      Each order is due 30 days after it was placed, and the placement time is stored as ISO-8601 text. Return a map for
      each order with its number ("number") and the date and time it is due ("due").
      """
    And the graph initializer of
      """
      g.addV("product").property("name","lamp").property("price","1999").property("inStock","true").
        addV("product").property("name","desk").property("price","12000").property("inStock","false").
        addV("product").property("name","chair").property("price","4550").property("inStock","true").
        addV("order").property("number","A1").property("placed","2024-03-05T10:00:00Z").property("shipped","2024-03-07T16:00:00Z").
        addV("order").property("number","A2").property("placed","2024-04-12T09:30:00Z").property("shipped","2024-04-13T09:30:00Z").
        addV("order").property("number","A3").property("placed","2024-03-28T18:15:00Z").property("shipped","2024-04-02T08:00:00Z")
      """
    And the traversal of
      """
      g.V().hasLabel("order").project("number","due").by("number").by(values("placed").asDate().dateAdd(DT.day,30))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"number":"A1","due":"dt[2024-04-04T10:00:00Z]"}] |
      | m[{"number":"A2","due":"dt[2024-05-12T09:30:00Z]"}] |
      | m[{"number":"A3","due":"dt[2024-04-27T18:15:00Z]"}] |
