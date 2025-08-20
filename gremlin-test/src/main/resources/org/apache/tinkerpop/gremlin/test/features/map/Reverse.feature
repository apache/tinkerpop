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

@StepClassMap @StepReverse
Feature: Step - reverse()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXfeature_test_nullX_reverse
    Given the empty graph
    And the traversal of
      """
      g.inject("feature", "test one", null).reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | erutaef |
      | eno tset |
      | null |

  Scenario: g_V_valuesXnameX_reverse
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | okram |
      | sadav |
      | pol |
      | hsoj |
      | elppir |
      | retep |

  Scenario: g_V_valuesXageX_reverse
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_path_byXnameX_reverse
    Given the modern graph
    And the traversal of
      """
      g.V().out().path().by("name").reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[lop,marko] |
      | l[vadas,marko] |
      | l[josh,marko] |
      | l[ripple,josh] |
      | l[lop,josh] |
      | l[lop,peter] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_reverse
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().path().by("name").reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[ripple,josh,marko] |
      | l[lop,josh,marko] |

  Scenario: g_V_valuesXageX_fold_orderXlocalX_byXdescX_reverse
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().order(local).by(desc).reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[27].i,d[29].i,d[32].i,d[35].i] |

  Scenario: g_V_valuesXnameX_fold_orderXlocalX_by_reverse
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().order(local).by().reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[vadas,ripple,peter,marko,lop,josh] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_reverse
    Given the empty graph
    And the traversal of
      """
      g.inject(null).reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXbX_reverse
    Given the empty graph
    And the traversal of
      """
      g.inject("b").reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3_threeX_reverse
    Given the empty graph
    And the traversal of
      """
      g.inject([3,"three"]).reverse()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[three,d[3].i] |
