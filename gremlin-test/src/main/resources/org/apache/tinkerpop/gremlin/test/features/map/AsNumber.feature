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
  Scenario: g_injectX5_43X_asNumberXGType_INTX
    Given the empty graph
    And the traversal of
      """
      g.inject(5.43).asNumber(GType.INT)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5_67X_asNumberXGType_INTX
    Given the empty graph
    And the traversal of
      """
      g.inject(5.67).asNumber(GType.INT)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5X_asNumberXGType_LONGX
    Given the empty graph
    And the traversal of
      """
      g.inject(5).asNumber(GType.LONG)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].l |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX12X_asNumberXGType_BYTEX
    Given the empty graph
    And the traversal of
      """
      g.inject(12).asNumber(GType.BYTE)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[12].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX32768X_asNumberXGType_SHORTX
    Given the empty graph
    And the traversal of
      """
      g.inject(32768).asNumber(GType.SHORT)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't convert number of type Integer to Short due to overflow."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX300X_asNumberXGType_BYTEX
    Given the empty graph
    And the traversal of
      """
      g.inject(300).asNumber(GType.BYTE)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't convert number of type Integer to Byte due to overflow."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX32768X_asNumberXGType_VertexX
    Given the empty graph
    And the traversal of
      """
      g.inject(32768).asNumber(GType.VERTEX)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "asNumber() requires a numeric type token, got VERTEX"

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5X_asNumberXGType_BYTEX
    Given the empty graph
    And the traversal of
      """
      g.inject("5").asNumber(GType.BYTE)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_000X_asNumberXGType_BIGINTX
    Given the empty graph
    And the traversal of
      """
      g.inject("1,000").asNumber(GType.BIGINT)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse string '1,000' as number."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_2_3_4_0x5X_asNumber_sum_asNumberXGType_BYTEX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0, 2, 3, "4", "0x5").asNumber().sum().asNumber(GType.BYTE)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[15].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_asNumberXGType_INTX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).asNumber(GType.INT)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_outXknowsX_asXbX_mathXa_plus_bX_byXageX_asNumberXGType_INTX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("knows").as("b").math("a + b").by("age").asNumber(GType.INT)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[56].i |
      | d[61].i |

  Scenario: g_withSideEffectXx_100X_V_age_mathX__plus_xX_asNumberXGType_LONGX
    Given the modern graph
    And the traversal of
      """
      g.withSideEffect("x", 100).V().values("age").math("_ + x").asNumber(GType.LONG)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[129].l |
      | d[127].l |
      | d[132].l |
      | d[135].l |

  Scenario: g_V_valuesXageX_asString_asNumberXGType_DOUBLEX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").asString().asNumber(GType.DOUBLE)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29.0].d |
      | d[27.0].d |
      | d[32.0].d |
      | d[35.0].d |

  #  asNumber should be round-trippable with asDate
  Scenario: g_V_valuesXbirthdayX_asNumber_asDate_asNumber
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "alice").property("birthday", 1596326400000).
        addV("person").property("name", "john").property("birthday", 597715200000).
        addV("person").property("name","charlie").property("birthday", 1012521600000).
        addV("person").property("name", "suzy").property("birthday", -131587200000)
      """
    And the traversal of
      """
      g.V().values("birthday").asNumber().asDate().asNumber()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[597715200000].l |
      | d[1596326400000].l |
      | d[1012521600000].l |
      | d[-131587200000].l |
