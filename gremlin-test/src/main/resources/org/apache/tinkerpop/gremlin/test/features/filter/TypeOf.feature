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

@StepClassFilter @StepIs
Feature: Predicate - typeOf() General

  Scenario: g_V_valuesXnameX_isXtypeOfXGType_STRINGXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").is(P.typeOf(GType.STRING))
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

  Scenario: g_V_valuesXnameX_isXtypeOfXjava_lang_StringXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").is(P.typeOf("String"))
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

  Scenario: g_V_hasXname_typeOfXGType_STRINGXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", P.typeOf(GType.STRING)).values("name")
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

  Scenario: g_V_orXhasXname_typeOfXGType_STRINGXX__hasXage_typeOfXGType_INTXXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().or(__.has("name", P.typeOf(GType.STRING)), __.has("age", P.typeOf(GType.INT))).values("name")
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

  Scenario: g_V_andXhasXname_typeOfXGType_STRINGXX__hasXage_typeOfXGType_INTXXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().and(__.has("name", P.typeOf(GType.STRING)), __.has("age", P.typeOf(GType.INT))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
      | peter |

  Scenario: g_V_notXhasXage_typeOfXGType_STRINGXXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().not(__.has("age", P.typeOf(GType.STRING))).values("name")
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

  Scenario: g_V_valuesXageX_isXnotXtypeOfXGType_STRINGXXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.not(P.typeOf(GType.STRING)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_valuesXnameX_isXtypeOfXstringStringXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").is(P.typeOf("String"))
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

  Scenario: g_V_orXvaluesXageX_isXtypeOfXGType_INTXX__valuesXnameX_isXtypeOfXGType_STRINGXXX_count
    Given the modern graph
    And the traversal of
      """
      g.V().or(__.values("age").is(P.typeOf(GType.INT)), __.values("name").is(P.typeOf(GType.STRING))).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_V_whereXvaluesXnameX_isXtypeOfXGType_STRINGXXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.values("name").is(P.typeOf(GType.STRING))).values("name")
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

  Scenario: g_V_whereXvaluesXageX_isXtypeOfXGType_STRINGXXX_count
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.values("age").is(P.typeOf(GType.STRING))).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].l |

  Scenario: g_V_whereXnotXvaluesXageX_isXtypeOfXGType_STRINGXXXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.not(__.values("age").is(P.typeOf(GType.STRING)))).values("name")
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

  Scenario: g_V_valuesXageX_isXtypeOfXGType_BOOLEANXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.BOOLEAN))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_CHARXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.CHAR))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_BINARYXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.BINARY))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_UUIDXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.UUID))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_DATETIMEXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.DATETIME))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_DURATIONXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.DURATION))
      """
    When iterated to list
    Then the result should be empty

#  TODO: with validation moved to P, deserialization will cause early error in GLV, leading to 499 instead, revisit
#  Scenario: g_V_valuesXageX_isXtypeOfXnon_registered_NameXX
#    Given the modern graph
#    And the traversal of
#    """
#    g.V().values("age").is(P.typeOf("non-registered-Name"))
#    """
#    When iterated to list
#    Then the traversal will raise an error with message containing text of "non-registered-Name is not a registered type"

  Scenario: g_V_hasLabelXgeneralX_valuesXbooleanX_isXtypeOfXGType_BOOLEANXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("boolean").is(P.typeOf(GType.BOOLEAN))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | true |

  Scenario: g_V_hasLabelXgeneralX_valuesXstringX_isXtypeOfXGType_STRINGXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("string").is(P.typeOf(GType.STRING))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | a string |

  Scenario: g_V_hasLabelXgeneralX_valuesXuuidX_isXtypeOfXGType_UUIDXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("uuid").is(P.typeOf(GType.UUID))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | uuid[ffffffff-fd49-1e4b-0000-00000d4b8a1d] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_inject_XtimeX_valuesXdatetimeX_isXtypeOfXGType_DATETIMEXX
    Given the empty graph
    And the traversal of
      """
      g.inject(datetime("2023-08-08T00:00:00Z")).is(P.typeOf(GType.DATETIME))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | dt[2023-08-08T00:00:00Z] |

  # TODO update depends on graph needs
#  Scenario: g_V_hasLabelXtimeX_valuesXdurationX_isXtypeOfXGType_DURATIONXX
#    Given the sink graph
#    And the traversal of
#      """
#      g.V().hasLabel("data").values().is(P.typeOf(GType.DURATION)).count()
#      """
#    When iterated to list
#    Then the result should be unordered
#      | result |
#      | d[1].l |
#
#  Scenario: g_V_hasLabelXgeneralX_valuesXbinaryX_isXtypeOfXGType_BINARYXX
#    Given the sink graph
#    And the traversal of
#      """
#      g.V().hasLabel("data").values().is(P.typeOf(GType.BINARY)).count()
#      """
#    When iterated to list
#    Then the result should be unordered
#      | result |
#      | d[1].l |
#
#  Scenario: g_V_hasLabelXgeneralX_valuesXcharX_isXtypeOfXGType_CHARXX
#    Given the sink graph
#    And the traversal of
#      """
#      g.V().hasLabel("data").values().is(P.typeOf(GType.CHAR)).count()
#      """
#    When iterated to list
#    Then the result should be unordered
#      | result |
#      | d[1].l |