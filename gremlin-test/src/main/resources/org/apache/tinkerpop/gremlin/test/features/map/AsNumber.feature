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

@StepClassMap @StepAsNumber
Feature: Step - asNumber()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5bX_asNumber
    Given the empty graph
    And the traversal of
      """
      g.inject(5b).asNumber()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5sX_asNumber
    Given the empty graph
    And the traversal of
      """
      g.inject(5s).asNumber()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].s |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5iX_asNumber
    Given the empty graph
    And the traversal of
      """
      g.inject(5i).asNumber()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5lX_asNumber
    Given the empty graph
    And the traversal of
      """
      g.inject(5l).asNumber()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].l |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5nX_asNumber
    Given the empty graph
    And the traversal of
      """
      g.inject(5n).asNumber()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].n |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5_0X_asNumber
    Given the empty graph
    And the traversal of
      """
      g.inject(5.0).asNumber()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5_75fX_asNumber
    Given the empty graph
    And the traversal of
      """
      g.inject(5.75f).asNumber()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5.75].f |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5_43X_asNumberXN_nintX
    Given the empty graph
    And the traversal of
      """
      g.inject(5.43).asNumber(N.nint)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5_67X_asNumberXN_nintX
    Given the empty graph
    And the traversal of
      """
      g.inject(5.67).asNumber(N.nint)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5X_asNumberXN_nlongX
    Given the empty graph
    And the traversal of
      """
      g.inject(5).asNumber(N.nlong)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].l |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX12X_asNumberXN_nbyteX
    Given the empty graph
    And the traversal of
      """
      g.inject(12).asNumber(N.nbyte)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[12].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX32768X_asNumberXN_nshortX
    Given the empty graph
    And the traversal of
      """
      g.inject(32768).asNumber(N.nshort)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't convert number of type Integer to Short due to overflow."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX300X_asNumberXN_nbyteX
    Given the empty graph
    And the traversal of
      """
      g.inject(300).asNumber(N.nbyte)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't convert number of type Integer to Byte due to overflow."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5X_asNumber
    Given the empty graph
    And the traversal of
      """
      g.inject("5").asNumber()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5X_asNumberXN_byteX
    Given the empty graph
    And the traversal of
      """
      g.inject("5").asNumber(N.nbyte)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_000X_asNumberXN_nintX
    Given the empty graph
    And the traversal of
      """
      g.inject("1,000").asNumber(N.nint)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse string '1,000' as number."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXtestX_asNumber
    Given the empty graph
    And the traversal of
      """
      g.inject("test").asNumber()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse string 'test' as number."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX_1_2_3_4X_asNumber
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3, 4]).asNumber()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse type ArrayList as number."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_2_3_4X_unfold_asNumber
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3, 4]).unfold().asNumber()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[2].i |
      | d[3].i |
      | d[4].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX_1__2__3__4_X_asNumberXX_foldXX
    Given the empty graph
    And the traversal of
      """
      g.inject("1", 2, "3", 4).asNumber().fold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[1].i,d[2].i,d[3].i,d[4].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_2_3_4_0x5X_asNumber_sum_asNumberXnbyteX
    Given the empty graph
    And using the parameter xx1 defined as "d[1.0].d"
    And using the parameter xx2 defined as "d[2].i"
    And using the parameter xx3 defined as "d[3].l"
    And the traversal of
      """
      g.inject(xx1, xx2, xx3, "4", "0x5").asNumber().sum().asNumber(N.nbyte)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[15].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_VX1X_asNumberXN_nintX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).asNumber(N.nint)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse type null as number."

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_outXknowsX_asXbX_mathXa_plus_bX_byXageX_asNumberXnintX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("knows").as("b").math("a + b").by("age").asNumber(N.nint)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[56].i |
      | d[61].i |

  Scenario: g_withSideEffectXx_100X_V_age_mathX__plus_xX_asNumberXnlongX
    Given the modern graph
    And using the parameter xx1 defined as "d[100].i"
    And the traversal of
      """
      g.withSideEffect("x", xx1).V().values("age").math("_ + x").asNumber(N.nlong)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[129].l |
      | d[127].l |
      | d[132].l |
      | d[135].l |
