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
Feature: Predicate - typeOf()

  Scenario: g_V_valuesXageX_isXtypeOfXGType_INTXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.INT))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

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

  Scenario: g_V_isXtypeOfXGType_VERTEXXX_count
    Given the modern graph
    And the traversal of
      """
      g.V().is(P.typeOf(GType.VERTEX)).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_E_isXtypeOfXGType_EDGEXX_count
    Given the modern graph
    And the traversal of
      """
      g.E().is(P.typeOf(GType.EDGE)).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_V_propertiesXnameX_isXtypeOfXGType_PROPERTYXX_count
    Given the modern graph
    And the traversal of
      """
      g.V().properties("name").is(P.typeOf(GType.PROPERTY)).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_NUMBERXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.NUMBER))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_BYTEXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.BYTE))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_SHORTXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.SHORT))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_LONGXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.LONG))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_FLOATXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.FLOAT))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_DOUBLEXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.DOUBLE))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_BIGINTXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.BIGINT))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_BIGDECIMALXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.BIGDECIMAL))
      """
    When iterated to list
    Then the result should be empty

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

  Scenario: g_V_valuesXageX_isXtypeOfXGType_LISTXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.LIST))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_MAPXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.MAP))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_SETXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.SET))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_PATHXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.PATH))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_TREEXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.TREE))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_GRAPHXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.GRAPH))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_VPXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.VP))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXGType_NULLXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.NULL))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_valuesXageX_isXtypeOfXjava_lang_IntegerXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf("Integer"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

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

  Scenario: g_V_valuesXageX_isXtypeOfXinvalid_type_NameXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf("invalid.type.Name"))
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "invalid.type.Name is not a registered type"

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

  Scenario: g_V_hasXage_typeOfXGType_NUMBERXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age", P.typeOf(GType.NUMBER)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
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

  Scenario: g_V_valuesXageX_isXtypeOfXGType_INTX_orXtypeOfXGType_LONGXXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.INT).or(P.typeOf(GType.LONG)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

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

  Scenario: g_V_whereXvaluesXageX_isXtypeOfXGType_INTXXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.values("age").is(P.typeOf(GType.INT))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
      | peter |

  Scenario: g_V_valuesXageX_isXtypeOfXintegerStringXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf("Integer"))
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

  Scenario: g_V_orXvaluesXageX_isXtypeOfXGType_BYTEXX__valuesXageX_isXtypeOfXGType_SHORTXXX_count
    Given the modern graph
    And the traversal of
      """
      g.V().or(__.values("age").is(P.typeOf(GType.BYTE)), __.values("age").is(P.typeOf(GType.SHORT))).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].l |

  Scenario: g_V_orXvaluesXageX_isXtypeOfXGType_NUMBERXX__valuesXnameX_isXtypeOfXGType_BOOLEANXXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().or(__.values("age").is(P.typeOf(GType.NUMBER)), __.values("name").is(P.typeOf(GType.BOOLEAN))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
      | peter |

  Scenario: g_E_orXisXtypeOfXGType_EDGEXX__isXtypeOfXGType_VERTEXXX_count
    Given the modern graph
    And the traversal of
      """
      g.E().or(__.is(P.typeOf(GType.EDGE)), __.is(P.typeOf(GType.VERTEX))).count()
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

  Scenario: g_V_whereXvaluesXageX_isXtypeOfXGType_NUMBERXXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.values("age").is(P.typeOf(GType.NUMBER))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
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

  Scenario: g_V_whereXisXtypeOfXGType_VERTEXXX_andXhasXnameXXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.is(P.typeOf(GType.VERTEX)).and(__.has("name"))).values("name")
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

  Scenario: g_E_whereXisXtypeOfXGType_EDGEXXX_count
    Given the modern graph
    And the traversal of
      """
      g.E().where(__.is(P.typeOf(GType.EDGE))).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_V_whereXpropertiesXnameX_isXtypeOfXGType_PROPERTYXXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.properties("name").is(P.typeOf(GType.PROPERTY))).values("name")
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

  Scenario: g_V_whereXvaluesXageX_isXtypeOfXGType_BYTEXX_orXvaluesXageX_isXtypeOfXGType_SHORTXXXX_count
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.values("age").is(P.typeOf(GType.BYTE)).or(__.values("age").is(P.typeOf(GType.SHORT)))).count()
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
