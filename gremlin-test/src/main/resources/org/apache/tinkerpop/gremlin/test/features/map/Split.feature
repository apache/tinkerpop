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

@StepClassMap @StepSplit
Feature: Step - split()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXthat_this_testX_spiltXhX
    Given the empty graph
    And the traversal of
      """
      g.inject("that", "this", "test", null).split("h")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[t,at] |
      | l[t,is] |
      | l[test] |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXhello_worldX_spiltXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject("hello world").split(null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[hello,world] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXthat_this_test_nullX_splitXemptyX
    Given the empty graph
    And the traversal of
      """
      g.inject("that", "this", "test", null).split("")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[t,h,a,t] |
      | l[t,h,i,s] |
      | l[t,e,s,t] |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXListXa_bXcX_splitXa_bX
    Given the empty graph
    And the traversal of
      """
      g.inject(["a","b"]).split("a")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The split() step can only take string as argument"

  Scenario: g_V_hasLabelXpersonX_valueXnameX_splitXnullX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").split(null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko] |
      | l[vadas] |
      | l[josh] |
      | l[peter] |

  Scenario: g_V_hasLabelXpersonX_valueXnameX_order_fold_splitXlocal_aX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").order().fold().split(Scope.local, "a").unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[josh] |
      | l[m,rko] |
      | l[peter] |
      | l[v,d,s] |

  Scenario: g_V_hasLabelXpersonX_valueXnameX_order_fold_splitXlocal_emptyX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").order().fold().split(Scope.local, "").unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[j,o,s,h] |
      | l[m,a,r,k,o] |
      | l[p,e,t,e,r] |
      | l[v,a,d,a,s] |