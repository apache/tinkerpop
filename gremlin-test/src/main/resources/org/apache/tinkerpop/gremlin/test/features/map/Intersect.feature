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

@StepClassMap @StepIntersect
Feature: Step - intersect()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_intersectXinjectX1XX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).intersect(__.inject(1))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Incoming traverser for intersect step can't be null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_intersectXV_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").intersect(__.V().fold())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "intersect step can only take an array or an Iterable type for incoming traversers, encountered"

  Scenario: g_V_fold_intersectXconstantXnullXX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().intersect(__.constant(null))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for intersect step must yield an iterable type, not null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_fold_intersectXVX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().intersect(__.V())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for intersect step must yield an iterable type, encountered"

  Scenario: g_V_valuesXnameX_fold_intersectX2X
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().intersect(2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "intersect step can only take an array or an Iterable as an argument, encountered"

  Scenario: g_V_valuesXnameX_fold_intersectX5varX
    Given the modern graph
    And using the parameter xx1 defined as "5"
    And the traversal of
      """
      g.V().values("name").fold().intersect(xx1)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "intersect step can only take an array or an Iterable as an argument, encountered"

  Scenario: g_V_valuesXnameX_fold_intersectXnullX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().intersect(null)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Argument provided for intersect step can't be null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnonexistantX_fold_intersectXV_valuesXnameX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("nonexistant").fold().intersect(__.V().values("name").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_fold_intersectXV_valuesXnonexistantX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().intersect(__.V().values("nonexistant").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_fold_intersectXV_valuesXageX_foldX_order_local
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().intersect(__.V().values("age").fold()).order(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[27].i,d[29].i,d[32].i,d[35].i] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_path_byXvaluesXnameX_toUpperX_intersectXMARKOX
    Given the modern graph
    And using the parameter xx1 defined as "l[MARKO]"
    And the traversal of
      """
      g.V().out().path().by(values("name").toUpper()).intersect(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[MARKO] |
      | s[MARKO] |
      | s[MARKO] |
      | s[] |
      | s[] |
      | s[] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXmarkoX_intersectX___V_valuesXnameX_foldX
    Given the modern graph
    And using the parameter xx1 defined as "l[marko]"
    And the traversal of
      """
      g.inject(xx1).intersect(__.V().values("name").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko] |

  @MultiProperties @MetaProperties
  Scenario: g_V_valueMapXlocationX_selectXvaluesX_unfold_intersectXseattle_vancouverX
    Given the crew graph
    And using the parameter xx1 defined as "l[seattle,vancouver]"
    And the traversal of
      """
      g.V().valueMap("location").select(values).unfold().intersect(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[] |
      | s[] |
      | s[seattle] |
      | s[] |

  Scenario: g_V_valuesXageX_fold_intersectX___constantX27X_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().intersect(__.constant(27).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[27].i] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_intersectXdave_kelvinX
    Given the modern graph
    And using the parameter xx1 defined as "l[dave,kelvin]"
    And the traversal of
      """
      g.V().out().out().path().by("name").intersect(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[] |
      | s[] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_intersectXa_cX
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,c]"
    And the traversal of
      """
      g.inject(xx1).intersect(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[a] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_intersectXa_null_cX
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,null,c]"
    And the traversal of
      """
      g.inject(xx1).intersect(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[a,null] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3_threeX_intersectXfive_three_7X
    Given the empty graph
    And using the parameter xx1 defined as "l[d[3].i,three]"
    And using the parameter xx2 defined as "l[five,three,d[7].i]"
    And the traversal of
      """
      g.inject(xx1).intersect(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[three] |
