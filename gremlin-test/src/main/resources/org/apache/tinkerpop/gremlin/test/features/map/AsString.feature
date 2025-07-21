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

@StepClassMap @StepAsString
Feature: Step - asString()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_2X_asString
    Given the empty graph
    And the traversal of
      """
      g.inject(1, 2).asString()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | 1 |
      | 2 |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_2X_asStringXlocalX
    Given the empty graph
    And the traversal of
      """
      g.inject(1, 2).asString(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | 1 |
      | 2 |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXlist_1_2X_asStringXlocalX
    Given the empty graph
    And the traversal of
      """
      g.inject([1,2]).asString(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[1,2] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_nullX_asString
    Given the empty graph
    And the traversal of
      """
      g.inject(null, 1).asString()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |
      | 1 |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_nullX_asStringXlocalX
    Given the empty graph
    And the traversal of
      """
      g.inject([1,null]).asString(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[1,null] |

  Scenario: g_V_valueMapXnameX_asString
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap("name").asString()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | {name=[marko]} |
      | {name=[vadas]} |
      | {name=[lop]} |
      | {name=[josh]} |
      | {name=[ripple]} |
      | {name=[peter]} |

  Scenario: g_V_valueMapXnameX_order_fold_asStringXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap("name").order().fold().asString(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[{name=[josh]},{name=[lop]},{name=[marko]},{name=[peter]},{name=[ripple]},{name=[vadas]}] |

  @UserSuppliedVertexIds
  Scenario: g_V_asString
    Given the modern graph
    And the traversal of
    """
    g.V().asString()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | str[v[1]] |
      | str[v[2]] |
      | str[v[3]] |
      | str[v[4]] |
      | str[v[5]] |
      | str[v[6]] |

  @UserSuppliedVertexIds
  Scenario: g_V_fold_asStringXlocalX_orderXlocalX
    Given the modern graph
    And the traversal of
    """
    g.V().fold().asString(Scope.local).order(local)
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[str[v[1]],str[v[2]],str[v[3]],str[v[4]],str[v[5]],str[v[6]]] |

  @UserSuppliedEdgeIds
  Scenario: g_E_asString
    Given the modern graph
    And the traversal of
    """
    g.E().asString()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | str[e[7][1-knows->2]] |
      | str[e[8][1-knows->4]] |
      | str[e[9][1-created->3]] |
      | str[e[10][4-created->5]] |
      | str[e[11][4-created->3]] |
      | str[e[12][6-created->3]] |

  @UserSuppliedVertexPropertyIds
  Scenario: g_V_properties
    Given the modern graph
    And the traversal of
      """
      g.V().properties().asString()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | str[vp[name->marko]] |
      | str[vp[age->29]] |
      | str[vp[name->vadas]]  |
      | str[vp[age->27]] |
      | str[vp[name->lop]]  |
      | str[vp[lang->java]]  |
      | str[vp[name->josh]]  |
      | str[vp[age->32]] |
      | str[vp[name->ripple]]  |
      | str[vp[lang->java]]  |
      | str[vp[name->peter]]  |
      | str[vp[age->35]] |

  Scenario: g_V_hasLabelXpersonX_valuesXageX_asString
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("age").asString()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | 29 |
      | 27 |
      | 32 |
      | 35 |

  Scenario: g_V_hasLabelXpersonX_valuesXageX_order_fold_asStringXlocalX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("age").order().fold().asString(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[27,29,32,35] |

  Scenario: g_V_hasLabelXpersonX_valuesXageX_asString_concatX_years_oldX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("age").asString().concat(" years old")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | 29 years old |
      | 27 years old |
      | 32 years old |
      | 35 years old |