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

@StepClassMap @StepDisjunct
Feature: Step - disjunct()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_disjunctXinjectX1XX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).disjunct(__.inject(1))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Incoming traverser for disjunct step can't be null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_disjunctXV_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").disjunct(__.V().fold())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "disjunct step can only take an array or an Iterable type for incoming traversers, encountered"

  Scenario: g_V_fold_disjunctXconstantXnullXX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().disjunct(__.constant(null))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for disjunct step must yield an iterable type, not null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_fold_disjunctXVX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().disjunct(__.V())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for disjunct step must yield an iterable type, encountered"

  Scenario: g_V_valuesXnameX_fold_disjunctX2X
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().disjunct(2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "disjunct step can only take an array or an Iterable as an argument, encountered"

  Scenario: g_V_valuesXnameX_fold_disjunctX3varX
    Given the modern graph
    And using the parameter xx1 defined as "3"
    And the traversal of
      """
      g.V().values("name").fold().disjunct(xx1)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "disjunct step can only take an array or an Iterable as an argument, encountered"

  Scenario: g_V_valuesXnameX_fold_disjunctXnullX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().disjunct(null)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Argument provided for disjunct step can't be null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnonexistantX_fold_disjunctXV_valuesXnameX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("nonexistant").fold().disjunct(__.V().values("name").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,vadas,lop,josh,ripple,peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_fold_disjunctXV_valuesXnonexistantX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().disjunct(__.V().values("nonexistant").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,vadas,lop,josh,ripple,peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_fold_disjunctXV_valuesXageX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().disjunct(__.V().values("age").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_path_byXvaluesXnameX_toUpperX_disjunctXMARKOX
    Given the modern graph
    And using the parameter xx1 defined as "l[MARKO]"
    And the traversal of
      """
      g.V().out().path().by(values("name").toUpper()).disjunct(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[LOP] |
      | s[VADAS] |
      | s[JOSH] |
      | s[JOSH,RIPPLE,MARKO] |
      | s[LOP,JOSH,MARKO] |
      | s[LOP,PETER,MARKO] |

  @MultiProperties @MetaProperties
  Scenario: g_V_valueMapXlocationX_selectXvaluesX_unfold_disjunctXseattle_vancouverX
    Given the crew graph
    And using the parameter xx1 defined as "l[seattle,vancouver]"
    And the traversal of
      """
      g.V().valueMap("location").select(values).unfold().disjunct(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[san diego,santa cruz,brussels,santa fe,seattle,vancouver] |
      | s[centreville,dulles,purcellville,seattle,vancouver] |
      | s[bremen,baltimore,oakland,vancouver] |
      | s[spremberg,kaiserslautern,aachen,seattle,vancouver] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_disjunctXmarkoX
    Given the modern graph
    And using the parameter xx1 defined as "l[marko]"
    And the traversal of
      """
      g.V().out().out().path().by("name").disjunct(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[josh,ripple] |
      | s[josh,lop] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_disjunctXstephen_markoX
    Given the modern graph
    And using the parameter xx1 defined as "l[stephen,marko]"
    And the traversal of
      """
      g.V().out().out().path().by("name").disjunct(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[stephen,josh,ripple] |
      | s[stephen,josh,lop] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_disjunctXdave_kelvinX
    Given the modern graph
    And using the parameter xx1 defined as "l[dave,kelvin]"
    And the traversal of
      """
      g.V().out().out().path().by("name").disjunct(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,josh,ripple,dave,kelvin] |
      | s[marko,josh,lop,dave,kelvin] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_disjunctXa_cX
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,c]"
    And the traversal of
      """
      g.inject(xx1).disjunct(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[null,b,c] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_disjunctXa_null_cX
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,null,c]"
    And the traversal of
      """
      g.inject(xx1).disjunct(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[b,c] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3_threeX_disjunctXfive_three_7X
    Given the empty graph
    And using the parameter xx1 defined as "l[d[3].i,three]"
    And using the parameter xx2 defined as "l[five,three,d[7].i]"
    And the traversal of
      """
      g.inject(xx1).disjunct(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[3].i,five,d[7].i] |
