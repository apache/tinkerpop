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

@StepClassMap @StepProduct
Feature: Step - product()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_productXinjectX1XX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).product(__.inject(1))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Incoming traverser for product step can't be null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_productXV_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").product(__.V().fold())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "product step can only take an array or an Iterable type for incoming traversers, encountered"

  Scenario: g_V_fold_productXconstantXnullXX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().product(__.constant(null))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for product step must yield an iterable type, not null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_fold_productXVX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().product(__.V())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for product step must yield an iterable type, encountered"

  Scenario: g_V_valuesXnameX_fold_productX2X
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().product(2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "product step can only take an array or an Iterable as an argument, encountered"

  Scenario: g_V_valuesXnameX_fold_productX7varX
    Given the modern graph
    And using the parameter xx1 defined as "7"
    And the traversal of
      """
      g.V().values("name").fold().product(xx1)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "product step can only take an array or an Iterable as an argument, encountered"

  Scenario: g_V_valuesXnameX_fold_productXnullX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().product(null)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Argument provided for product step can't be null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnonexistantX_fold_productXV_valuesXnameX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("nonexistant").fold().product(__.V().values("name").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_fold_productXV_valuesXnonexistantX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().product(__.V().values("nonexistant").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_order_byXdescX_limitX3X_fold_productXV_valuesXageX_order_byXascX_limitX2X_foldX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().by(desc).limit(3).fold().product(__.V().values("age").order().by(asc).limit(2).fold()).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[35].i,d[27].i] |
      | l[d[35].i,d[29].i] |
      | l[d[32].i,d[27].i] |
      | l[d[32].i,d[29].i] |
      | l[d[29].i,d[27].i] |
      | l[d[29].i,d[29].i] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_path_byXvaluesXnameX_toUpperX_productXMARKOX_unfold
    Given the modern graph
    And using the parameter xx1 defined as "l[MARKO]"
    And the traversal of
      """
      g.V().out().path().by(values("name").toUpper()).product(xx1).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[MARKO,MARKO] |
      | l[LOP,MARKO] |
      | l[MARKO,MARKO] |
      | l[VADAS,MARKO] |
      | l[MARKO,MARKO] |
      | l[JOSH,MARKO] |
      | l[JOSH,MARKO] |
      | l[RIPPLE,MARKO] |
      | l[JOSH,MARKO] |
      | l[LOP,MARKO] |
      | l[PETER,MARKO] |
      | l[LOP,MARKO] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXmarkoX_productXV_valuesXnameX_order_foldX_unfold
    Given the modern graph
    And using the parameter xx1 defined as "l[marko]"
    And the traversal of
      """
      g.inject(xx1).product(__.V().values("name").order().fold()).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko,josh] |
      | l[marko,lop] |
      | l[marko,marko] |
      | l[marko,peter] |
      | l[marko,ripple] |
      | l[marko,vadas] |

  @MultiProperties @MetaProperties
  Scenario: g_V_valueMapXlocationX_selectXvaluesX_unfold_productXdulles_seattle_vancouverX_unfold
    Given the crew graph
    And using the parameter xx1 defined as "l[dulles,seattle,vancouver]"
    And the traversal of
      """
      g.V().valueMap("location").select(values).unfold().product(xx1).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[san diego,dulles] |
      | l[san diego,seattle] |
      | l[san diego,vancouver] |
      | l[santa cruz,dulles] |
      | l[santa cruz,seattle] |
      | l[santa cruz,vancouver] |
      | l[brussels,dulles] |
      | l[brussels,seattle] |
      | l[brussels,vancouver] |
      | l[santa fe,dulles] |
      | l[santa fe,seattle] |
      | l[santa fe,vancouver] |
      | l[centreville,dulles] |
      | l[centreville,seattle] |
      | l[centreville,vancouver] |
      | l[dulles,dulles] |
      | l[dulles,seattle] |
      | l[dulles,vancouver] |
      | l[purcellville,dulles] |
      | l[purcellville,seattle] |
      | l[purcellville,vancouver] |
      | l[bremen,dulles] |
      | l[bremen,seattle] |
      | l[bremen,vancouver] |
      | l[baltimore,dulles] |
      | l[baltimore,seattle] |
      | l[baltimore,vancouver] |
      | l[oakland,dulles] |
      | l[oakland,seattle] |
      | l[oakland,vancouver] |
      | l[seattle,dulles] |
      | l[seattle,seattle] |
      | l[seattle,vancouver] |
      | l[spremberg,dulles] |
      | l[spremberg,seattle] |
      | l[spremberg,vancouver] |
      | l[kaiserslautern,dulles] |
      | l[kaiserslautern,seattle] |
      | l[kaiserslautern,vancouver] |
      | l[aachen,dulles] |
      | l[aachen,seattle] |
      | l[aachen,vancouver] |

  Scenario: g_V_valuesXageX_order_byXascX_fold_productXconstantX27X_foldX_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().by(asc).fold().product(__.constant(27).fold()).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[27].i,d[27].i] |
      | l[d[29].i,d[27].i] |
      | l[d[32].i,d[27].i] |
      | l[d[35].i,d[27].i] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_productXdave_kelvinX_unfold
    Given the modern graph
    And using the parameter xx1 defined as "l[dave,kelvin]"
    And the traversal of
      """
      g.V().out().out().path().by("name").product(xx1).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[marko,dave] |
      | l[marko,kelvin] |
      | l[josh,dave] |
      | l[josh,kelvin] |
      | l[ripple,dave] |
      | l[ripple,kelvin] |
      | l[marko,dave] |
      | l[marko,kelvin] |
      | l[josh,dave] |
      | l[josh,kelvin] |
      | l[lop,dave] |
      | l[lop,kelvin] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_productXa_cX_unfold
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,c]"
    And the traversal of
      """
      g.inject(xx1).product(xx2).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[a,a] |
      | l[a,c] |
      | l[null,a] |
      | l[null,c] |
      | l[b,a] |
      | l[b,c] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_productXa_null_cX_unfold
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,null,c]"
    And the traversal of
      """
      g.inject(xx1).product(xx2).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[a,a] |
      | l[a,null] |
      | l[a,c] |
      | l[null,a] |
      | l[null,null] |
      | l[null,c] |
      | l[b,a] |
      | l[b,null] |
      | l[b,c] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3_threeX_productXfive_three_7X_unfold
    Given the empty graph
    And using the parameter xx1 defined as "l[d[3].i,three]"
    And using the parameter xx2 defined as "l[five,three,d[7].i]"
    And the traversal of
      """
      g.inject(xx1).product(xx2).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[3].i,five] |
      | l[d[3].i,three] |
      | l[d[3].i,d[7].i] |
      | l[three,five] |
      | l[three,three] |
      | l[three,d[7].i] |
