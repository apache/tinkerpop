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

@StepClassMap @StepSubstring
Feature: Step - substring()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXthat_this_testX_substringX1_8X
    Given the empty graph
    And the traversal of
      """
      g.inject("test", "hello world", null).substring(1, 8)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | est |
      | ello wo |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXListXa_bXcX_substringX1_2X
    Given the empty graph
    And the traversal of
      """
      g.inject(["aa","bb"]).substring(1, 2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The substring() step can only take string as argument"

  Scenario: g_V_hasLabelXpersonX_valueXnameX_substringX2X
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").substring(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p |
      | pple |

  Scenario: g_V_hasLabelXsoftwareX_valueXnameX_substringX1_4X
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").substring(1, 4)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | op |
      | ipp |

  Scenario: g_V_hasLabelXpersonX_valueXnameX_order_fold_substringXlocal_2X
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").order().fold().substring(Scope.local, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[p,pple]|

  Scenario: g_V_hasLabelXsoftwareX_valueXnameX_order_fold_substringXlocal_1_4X
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").order().fold().substring(Scope.local, 1, 4)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[op,ipp] |

  Scenario: g_V_hasLabelXsoftwareX_valueXnameX_substringX1_0X
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").substring(1, 0)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | str[] |
      | str[] |

  Scenario: g_V_hasLabelXpersonX_valueXnameX_substringXneg3X
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").substring(-3)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | rko |
      | osh |
      | das |
      | ter |

Scenario: g_V_hasLabelXsoftwareX_valueXnameX_substringX1_neg1X
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").substring(1, -1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | o |
      | ippl |

Scenario: g_V_hasLabelXsoftwareX_valueXnameX_substringXneg4_2X
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").substring(-4, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lo |
      | str[] |

Scenario: g_V_hasLabelXsoftwareX_valueXnameX_substringXneg3_neg1X
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").substring(-3, -1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lo |
      | pl |