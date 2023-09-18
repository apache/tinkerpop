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
      | ello wor |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXListXa_bXcX_substringX1_1X
    Given the empty graph
    And using the parameter xx1 defined as "l[aa,bb]"
    And the traversal of
      """
      g.inject(xx1).substring(1, 1)
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

  Scenario: g_V_hasLabelXsoftwareX_valueXnameX_substringX1_3X
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").substring(1, 3)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | op |
      | ipp |

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
