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
  Scenario: g_injectX5bX_asNumberXX
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
  Scenario: g_injectX5sX_asNumberXX
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
  Scenario: g_injectX5iX_asNumberXX
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
  Scenario: g_injectX5lX_asNumberXX
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
  Scenario: g_injectX5nX_asNumberXX
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
  Scenario: g_injectX5_0X_asNumberXX
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
  Scenario: g_injectX5_75fX_asNumberXX
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
  Scenario: g_injectX5X_asNumberXX
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
  Scenario: g_injectXtestX_asNumberXX
    Given the empty graph
    And the traversal of
      """
      g.inject("test").asNumber()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse string 'test' as number."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX_1__2__3__4_X_asNumberXX
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3, 4]).asNumber()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse type ArrayList as number."

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX_1__2__3__4_X_unfoldXX_asNumberXX
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
  Scenario: g_injectX_1__2__3__4_X_unfoldXX_asNumberXX_foldXX
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3, 4]).unfold().asNumber().fold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[1].i,d[2].i,d[3].i,d[4].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_VX1X_asNumberXN_nintX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).asNumber(N.nint)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Can't parse type null as number."