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
    And using the parameter xx1 defined as "l[1,2]"
    And the traversal of
      """
      g.inject(xx1).asBool()
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
