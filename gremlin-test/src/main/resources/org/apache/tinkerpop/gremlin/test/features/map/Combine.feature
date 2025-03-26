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

@StepClassMap @StepCombine
Feature: Step - combine()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_combineXinjectX1XX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).combine(__.inject(1))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Incoming traverser for combine step can't be null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_combineXV_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").combine(__.V().fold())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "combine step can only take an array or an Iterable type for incoming traversers"

  Scenario: g_V_fold_combineXconstantXnullXX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().combine(__.constant(null))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for combine step must yield an iterable type, not null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_fold_combineXVX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().combine(__.V())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for combine step must yield an iterable type, encountered"

  Scenario: g_V_valuesXnameX_fold_combineX2X
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().combine(2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "combine step can only take an array or an Iterable as an argument, encountered"

  Scenario: g_V_valuesXnameX_fold_combineX2varX
    Given the modern graph
    And using the parameter xx1 defined as "2"
    And the traversal of
      """
      g.V().values("name").fold().combine(xx1)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "combine step can only take an array or an Iterable as an argument, encountered"

  Scenario: g_V_valuesXnameX_fold_combineXnullX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().combine(null)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Argument provided for combine step can't be null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnonexistantX_fold_combineXV_valuesXnameX_foldX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().values("nonexistant").fold().combine(__.V().values("name").fold()).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_fold_combineXV_valuesXnonexistantX_foldX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().combine(__.V().values("nonexistant").fold()).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_order_byXdescX_fold_combineXV_valuesXageX_order_byXdescX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().by(desc).fold().combine(__.V().values("age").order().by(desc).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[35].i,d[32].i,d[29].i,d[27].i,d[35].i,d[32].i,d[29].i,d[27].i] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_path_byXvaluesXnameX_toUpperX_combineXMARKOX
    Given the modern graph
    And using the parameter xx1 defined as "l[MARKO]"
    And the traversal of
      """
      g.V().out().path().by(values("name").toUpper()).combine(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[MARKO,LOP,MARKO] |
      | l[MARKO,VADAS,MARKO] |
      | l[MARKO,JOSH,MARKO] |
      | l[JOSH,RIPPLE,MARKO] |
      | l[JOSH,LOP,MARKO] |
      | l[PETER,LOP,MARKO] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXxx1X_combineXV_valuesXnameX_foldX_unfold
    Given the modern graph
    And using the parameter xx1 defined as "l[marko]"
    And the traversal of
      """
      g.inject(xx1).combine(__.V().values("name").fold()).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |

  @MultiProperties @MetaProperties
  Scenario: g_V_valueMapXlocationX_selectXvaluesX_unfold_combineXseattle_vancouverX_orderXlocalX
    Given the crew graph
    And using the parameter xx1 defined as "l[seattle,vancouver]"
    And the traversal of
      """
      g.V().valueMap("location").select(values).unfold().combine(xx1).order(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[brussels,san diego,santa cruz,santa fe,seattle,vancouver] |
      | l[centreville,dulles,purcellville,seattle,vancouver] |
      | l[baltimore,bremen,oakland,seattle,seattle,vancouver] |
      | l[aachen,kaiserslautern,seattle,spremberg,vancouver] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_combineXempty_listX
    Given the modern graph
    And using the parameter xx1 defined as "l[]"
    And the traversal of
      """
      g.V().out().out().path().by("name").combine(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko,josh,ripple] |
      | l[marko,josh,lop] |

  Scenario: g_V_valuesXageX_order_fold_combineXconstantX27X_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().fold().combine(__.constant(27).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[27].i,d[29].i,d[32].i,d[35].i,d[27].i] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_combineXdave_kelvinX
    Given the modern graph
    And using the parameter xx1 defined as "l[dave,kelvin]"
    And the traversal of
      """
      g.V().out().out().path().by("name").combine(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko,josh,ripple,dave,kelvin] |
      | l[marko,josh,lop,dave,kelvin] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_combineXa_cX
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,c]"
    And the traversal of
      """
      g.inject(xx1).combine(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[a,null,b,a,c] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_combineXa_null_cX
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,null,c]"
    And the traversal of
      """
      g.inject(xx1).combine(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[a,null,b,a,null,c] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3_threeX_combineXfive_three_7X
    Given the empty graph
    And using the parameter xx1 defined as "l[d[3].i,three]"
    And using the parameter xx2 defined as "l[five,three,d[7].i]"
    And the traversal of
      """
      g.inject(xx1).combine(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[3].i,three,five,three,d[7].i] |
