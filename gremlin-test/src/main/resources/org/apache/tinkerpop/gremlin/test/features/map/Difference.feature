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

@StepClassMap @StepDifference
Feature: Step - difference()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_differenceXinjectX1XX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).difference(__.inject(1))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Incoming traverser for difference step can't be null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_differenceXV_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").difference(__.V().fold())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "difference step can only take an array or an Iterable type for incoming traversers"

  Scenario: g_V_fold_differenceXconstantXnullXX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().difference(__.constant(null))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for difference step must yield an iterable type, not null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_fold_differenceXVX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().difference(__.V())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for difference step must yield an iterable type, encountered"

  Scenario: g_V_valuesXnameX_fold_differenceX2X
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().difference(2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "difference step can only take an array or an Iterable as an argument, encountered"

  Scenario: g_V_valuesXnameX_fold_differenceX2varX
    Given the modern graph
    And using the parameter xx1 defined as "2"
    And the traversal of
      """
      g.V().values("name").fold().difference(xx1)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "difference step can only take an array or an Iterable as an argument, encountered"

  Scenario: g_V_valuesXnameX_fold_differenceXnullX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().difference(null)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Argument provided for difference step can't be null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnonexistantX_fold_differenceXV_valuesXnameX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("nonexistant").fold().difference(__.V().values("name").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_fold_differenceXV_valuesXnonexistantX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().difference(__.V().values("nonexistant").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,vadas,lop,josh,ripple,peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_fold_differenceXV_valuesXageX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().difference(__.V().values("age").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_path_byXvaluesXnameX_toUpperX_differenceXMARKOX
    Given the modern graph
    And using the parameter xx1 defined as "l[MARKO]"
    And the traversal of
      """
      g.V().out().path().by(values("name").toUpper()).difference(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[LOP] |
      | s[VADAS] |
      | s[JOSH] |
      | s[JOSH,RIPPLE] |
      | s[LOP,JOSH] |
      | s[LOP,PETER] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXmarkoX_differenceXV_valuesXnameX_foldX
    Given the modern graph
    And using the parameter xx1 defined as "l[marko]"
    And the traversal of
      """
      g.inject(xx1).difference(__.V().values("name").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[] |

  @MultiProperties @MetaProperties
  Scenario: g_V_valueMapXlocationX_selectXvaluesX_unfold_differenceXseattle_vancouverX
    Given the crew graph
    And using the parameter xx1 defined as "l[seattle,vancouver]"
    And the traversal of
      """
      g.V().valueMap("location").select(values).unfold().difference(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[san diego,santa cruz,brussels,santa fe] |
      | s[centreville,dulles,purcellville] |
      | s[bremen,baltimore,oakland] |
      | s[spremberg,kaiserslautern,aachen] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_differenceXrippleX
    Given the modern graph
    And using the parameter xx1 defined as "l[ripple]"
    And the traversal of
      """
      g.V().out().out().path().by("name").difference(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,josh] |
      | s[josh,lop,marko] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_differenceXempty_listX
    Given the modern graph
    And using the parameter xx1 defined as "l[]"
    And the traversal of
      """
      g.V().out().out().path().by("name").difference(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,josh,ripple] |
      | s[marko,josh,lop] |

  Scenario: g_V_valuesXageX_fold_differenceXconstantX27X_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().difference(__.constant(27).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[29].i,d[32].i,d[35].i] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_differenceXdave_kelvinX
    Given the modern graph
    And using the parameter xx1 defined as "l[dave,kelvin]"
    And the traversal of
      """
      g.V().out().out().path().by("name").difference(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,josh,ripple] |
      | s[marko,josh,lop] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_differenceXa_cX
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,c]"
    And the traversal of
      """
      g.inject(xx1).difference(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[null,b] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_differenceXa_null_cX
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,null,c]"
    And the traversal of
      """
      g.inject(xx1).difference(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[b] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3_threeX_differenceXfive_three_7X
    Given the empty graph
    And using the parameter xx1 defined as "l[d[3].i,three]"
    And using the parameter xx2 defined as "l[five,three,d[7].i]"
    And the traversal of
      """
      g.inject(xx1).difference(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[3].i] |
