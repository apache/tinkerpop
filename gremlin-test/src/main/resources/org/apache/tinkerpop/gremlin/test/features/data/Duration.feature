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

@StepClassData @DataDuration
Feature: Data - DURATION

  @GraphComputerVerificationInjectionNotSupported @SupportsDuration
  Scenario: g_injectXDurationX9000_0XX
    Given the empty graph
    And the traversal of
      """
      g.inject(Duration(9000, 0))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dur[9000,0] |

  @GraphComputerVerificationInjectionNotSupported @SupportsDuration
  Scenario: g_injectXDurationX0_0XX
    Given the empty graph
    And the traversal of
      """
      g.inject(Duration(0, 0))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dur[0,0] |

  @GraphComputerVerificationInjectionNotSupported @SupportsDuration
  Scenario: g_injectXDurationX0_500000000XX
    Given the empty graph
    And the traversal of
      """
      g.inject(Duration(0, 500000000))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dur[0,500000000] |

  @GraphComputerVerificationInjectionNotSupported @SupportsDuration
  Scenario: g_injectXDurationX30_0XX
    Given the empty graph
    And the traversal of
      """
      g.inject(Duration(30, 0))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dur[30,0] |

  @GraphComputerVerificationInjectionNotSupported @SupportsDuration
  Scenario: g_injectXDurationX30_0_falseXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Duration(30, 0, false))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dur[30,0,false] |

  @GraphComputerVerificationInjectionNotSupported @SupportsDuration
  Scenario: g_injectXDurationX1_500000000_falseXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Duration(1, 500000000, false))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dur[1,500000000,false] |

  @SupportsDuration
  Scenario: g_valuesXlengthX_isXtypeOfXGType_DURATIONXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("length", Duration(9000, 0))
      """
    And the traversal of
      """
      g.V().values("length").is(P.typeOf(GType.DURATION))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dur[9000,0] |

  @GraphComputerVerificationInjectionNotSupported @SupportsDuration
  Scenario: g_injectXDurationX9000_0XX_isXgtXDurationX3600_0XXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Duration(9000, 0)).is(P.gt(Duration(3600, 0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dur[9000,0] |
