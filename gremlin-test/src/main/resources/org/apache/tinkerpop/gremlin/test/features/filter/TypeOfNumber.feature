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
Feature: Predicate - typeOf() Number

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

  Scenario: g_V_valuesXage_nameX_isXtypeOfXintegerStringXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age", "name").is(P.typeOf("Integer"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

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

  Scenario: g_V_hasLabelXnumberX_isXtypeOfXGType_NUMBERXX_valuesXnameX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values().is(P.typeOf(GType.NUMBER))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[1].l |
      | d[1.0].f |
      | d[1.0].d |

  Scenario: g_V_hasLabelXnumberX_valuesXintX_isXtypeOfXGType_INTXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("int").is(P.typeOf(GType.INT))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |

  Scenario: g_V_hasLabelXnumberX_valuesXlongX_isXtypeOfXGType_LONGXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("long").is(P.typeOf(GType.LONG))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |

  Scenario: g_V_hasLabelXnumberX_valuesXfloatX_isXtypeOfXGType_FLOATXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("float").is(P.typeOf(GType.FLOAT))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1.0].f |

  Scenario: g_V_hasLabelXnumberX_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("double").is(P.typeOf(GType.DOUBLE))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1.0].d |

  Scenario: g_V_hasLabelXnumberX_valuesXintX_asNumberXGType_BYTEX_isXtypeOfXGType_BYTEXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("int").asNumber(GType.BYTE).is(P.typeOf(GType.BYTE))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].b |

  Scenario: g_V_hasLabelXnumberX_valuesXintX_asNumberXGType_SHORTX_isXtypeOfXGType_SHORTXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("int").asNumber(GType.SHORT).is(P.typeOf(GType.SHORT))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].s |

  Scenario: g_V_hasLabelXnumberX_valuesXintX_asNumberXGType_BIGINTX_isXtypeOfXGType_BIGINTXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("int").asNumber(GType.BIGINT).is(P.typeOf(GType.BIGINT)).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |

  Scenario: g_V_hasLabelXnumberX_valuesXintX_asNumberXGType_BIGDECIMALX_isXtypeOfXGType_BIGDECIMALXX
    Given the sink graph
    And the traversal of
      """
      g.V().hasLabel("data").values("int").asNumber(GType.BIGDECIMAL).is(P.typeOf(GType.BIGDECIMAL)).count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |