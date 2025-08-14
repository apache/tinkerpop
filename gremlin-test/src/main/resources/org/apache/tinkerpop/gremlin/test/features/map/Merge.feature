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

@StepClassMap @StepMerge
Feature: Step - merge()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_mergeXinjectX1XX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).merge(__.inject(1))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Incoming traverser for merge step can't be null"

  # Test that non-iterable data isn't accepted

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_mergeXV_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").merge(__.V().fold())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "merge step can only take an array or an Iterable type for incoming traversers, encountered"

  Scenario: g_V_fold_mergeXconstantXnullXX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().merge(__.constant(null))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for merge step must yield an iterable type, not null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_fold_mergeXVX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().merge(__.V())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for merge step must yield an iterable type, encountered"

  Scenario: g_V_elementMap_mergeXconstantXaXX
    Given the modern graph
    And the traversal of
      """
      g.V().elementMap().merge(__.constant("a"))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "merge step expected provided argument to evaluate to a Map, encountered"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_fold_mergeXV_asXaX_projectXaX_byXnameXX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().merge(__.V().as("a").project("a").by("name"))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "traversal argument for merge step must yield an iterable type, encountered"

  Scenario: g_V_fold_mergeXk_vX
    Given the modern graph
    And the traversal of
      """
      g.V().fold().merge([k:"v"])
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "step type mismatch: expected argument to be Iterable but got Map"

  Scenario: g_V_valuesXnameX_fold_mergeX2X
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().merge(2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "merge step can only take an array or an Iterable as an argument, encountered"

  Scenario: g_V_valuesXnameX_fold_mergeXnullX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().merge(null)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Argument provided for merge step can't be null"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnonexistantX_fold_mergeXV_valuesXnameX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("nonexistant").fold().merge(__.V().values("name").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,vadas,lop,josh,ripple,peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXnameX_fold_mergeXV_valuesXnonexistantX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").fold().merge(__.V().values("nonexistant").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,vadas,lop,josh,ripple,peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_valuesXageX_fold_mergeXV_valuesXageX_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().merge(__.V().values("age").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[35].i,d[32].i,d[29].i,d[27].i] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_path_byXvaluesXnameX_toUpperX_mergeXMARKOX
    Given the modern graph
    And using the parameter xx1 defined as "l[MARKO]"
    And the traversal of
      """
      g.V().out().path().by(values("name").toUpper()).merge(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[MARKO,LOP] |
      | s[MARKO,VADAS] |
      | s[MARKO,JOSH] |
      | s[MARKO,JOSH,RIPPLE] |
      | s[MARKO,LOP,JOSH] |
      | s[MARKO,LOP,PETER] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXmarkoX_mergeXV_valuesXnameX_foldX
    Given the modern graph
    And using the parameter xx1 defined as "l[marko]"
    And the traversal of
      """
      g.inject(xx1).merge(__.V().values("name").fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,vadas,lop,josh,ripple,peter] |

  @MultiProperties @MetaProperties
  Scenario: g_V_valueMapXlocationX_selectXvaluesX_unfold_mergeXseattle_vancouverX
    Given the crew graph
    And using the parameter xx1 defined as "l[seattle,vancouver]"
    And the traversal of
      """
      g.V().valueMap("location").select(values).unfold().merge(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[san diego,santa cruz,brussels,santa fe,seattle,vancouver] |
      | s[centreville,dulles,purcellville,seattle,vancouver] |
      | s[bremen,baltimore,oakland,seattle,vancouver] |
      | s[spremberg,kaiserslautern,aachen,seattle,vancouver] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_mergeXempty_listX
    Given the modern graph
    And using the parameter xx1 defined as "l[]"
    And the traversal of
      """
      g.V().out().out().path().by("name").merge(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,josh,ripple] |
      | s[marko,josh,lop] |

  Scenario: g_V_valuesXageX_fold_mergeXconstantX27X_foldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").fold().merge(__.constant(27).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[27].i,d[29].i,d[32].i,d[35].i] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_mergeXdave_kelvinX
    Given the modern graph
    And using the parameter xx1 defined as "l[dave,kelvin]"
    And the traversal of
      """
      g.V().out().out().path().by("name").merge(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,josh,ripple,dave,kelvin] |
      | s[marko,josh,lop,dave,kelvin] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_mergeXa_cX
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,c]"
    And the traversal of
      """
      g.inject(xx1).merge(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[null,a,c,b] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_mergeXa_null_cX
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And using the parameter xx2 defined as "l[a,null,c]"
    And the traversal of
      """
      g.inject(xx1).merge(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[null,a,c,b] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3_threeX_mergeXfive_three_7X
    Given the empty graph
    And using the parameter xx1 defined as "l[d[3].i,three]"
    And using the parameter xx2 defined as "l[five,three,d[7].i]"
    And the traversal of
      """
      g.inject(xx1).merge(xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[3].i,five,three,d[7].i] |

  Scenario: g_V_asXnameX_projectXnameX_byXnameX_mergeXother_blueprintX
    Given the modern graph
    And using the parameter xx1 defined as "m[{\"other\":\"blueprint\"}]"
    And the traversal of
      """
      g.V().as("name").project("name").by("name").merge(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name":"marko", "other":"blueprint"}] |
      | m[{"name":"lop", "other":"blueprint"}] |
      | m[{"name":"vadas", "other":"blueprint"}] |
      | m[{"name":"josh", "other":"blueprint"}] |
      | m[{"name":"peter", "other":"blueprint"}] |
      | m[{"name":"ripple", "other":"blueprint"}] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_markoX_elementMap_mergeXV_hasXname_lopX_elementMapX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name","marko").elementMap().merge(__.V().has("name","lop").elementMap())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]":"v[lop].id","t[label]":"software","name":"lop","age":29,"lang":"java"}] |
