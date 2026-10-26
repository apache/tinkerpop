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
  Scenario: g_injectX3_14X_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject(3.14).asBool()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | true |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXneg_1X_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject(-1).asBool()
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
  Scenario: g_injectXneg_0X_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject(-0.0).asBool()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | false |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXNaNX_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject(NaN).asBool()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | false |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXbool_trueX_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject(true).asBool()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | true |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXfalseX_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject(false).asBool()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | false |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXtrueX_asBool
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
  Scenario: g_injectXmixed_trueX_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject('tRUe').asBool()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | true |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject(null).asBool()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse null as Boolean."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXhelloX_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject('hello').asBool()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse hello as Boolean."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_2X_asBool
    Given the empty graph
    And the traversal of
      """
      g.inject([1,2]).asBool()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse"

  Scenario: g_VXX_localX_outE_countX_asBool
    Given the modern graph
    And the traversal of
      """
      g.V().local(outE().count()).asBool()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | true |
      | false |
      | false |
      | true |
      | false |
      | true |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_sackXassignX_byX_hasLabelXpersonX_count_asBoolX_sackXandX_byX_outE_count_asBoolX_sack_path
    Given the modern graph
    And the traversal of
      """
      g.V().sack(assign).by(__.hasLabel('person').count().asBool()).sack(and).by(__.outE().count().asBool()).sack().path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],true] |
      | p[v[vadas],false] |
      | p[v[lop],false] |
      | p[v[josh],true] |
      | p[v[ripple],false] |
      | p[v[peter],true] |

  Scenario: g_V_hasLabelXproductX_whereXvaluesXinStockX_asBool_isXtrueXX_name
    Given the empty graph
    And the intent
      """
      The in-stock flag on each product is stored as the text "true" or "false". Which products are in stock? Return
      their names.
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
      g.V().hasLabel("product").where(values("inStock").asBool().is(true)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lamp |
      | chair |
