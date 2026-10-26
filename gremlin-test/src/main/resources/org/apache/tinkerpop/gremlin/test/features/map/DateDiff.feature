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

  Scenario: g_V_hasLabelXorderX_asXoX_projectXnumber_millisX_byXnumberX_byXvaluesXshippedX_asDate_dateDiffXselectXoX_valuesXplacedX_asDateXX
    Given the empty graph
    And the intent
      """
      How long did each order take to ship? The placement and shipping times are stored as ISO-8601 text. Return a map
      for each order with its number ("number") and the time from placement to shipping in milliseconds ("millis").
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
      g.V().hasLabel("order").as("o").project("number","millis").by("number").by(values("shipped").asDate().dateDiff(select("o").values("placed").asDate()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"number":"A1","millis":"d[194400000].l"}] |
      | m[{"number":"A2","millis":"d[86400000].l"}] |
      | m[{"number":"A3","millis":"d[395100000].l"}] |
