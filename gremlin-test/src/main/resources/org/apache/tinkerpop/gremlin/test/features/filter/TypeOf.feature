# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
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

  Scenario: g_V_valuesXageX_isXtypeOfXGType_NULLXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.NULL))
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

  Scenario: g_V_valuesXageX_isXtypeOfXnon_registered_NameXX
    Given the modern graph
    And the traversal of
    """
    g.V().values("age").is(P.typeOf("non-registered-Name"))
    """
    When iterated to list
    Then the traversal will raise an error

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXtrueX_isXtypeOfXGType_BOOLEANX
    Given the empty graph
    And the traversal of
      """
      g.inject(true).is(P.typeOf(GType.BOOLEAN))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | true |

  Scenario: g_V_hasLabelXpersonX_hasXage_typeOfXGType_STRINGXX_name
    Given the empty graph
    And the intent
      """
      Some people's ages were stored as text by mistake. Which people have an age stored as text rather than a number?
      Return their names.
      """
    And the graph initializer of
      """
      g.addV("person").property("name","alice").property("age",31).
        addV("person").property("name","bob").property("age","40").
        addV("person").property("name","carol").property("age",25).
        addV("person").property("name","dave").property("age","unknown")
      """
    And the traversal of
      """
      g.V().hasLabel("person").has("age",P.typeOf(GType.STRING)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | bob |
      | dave |

  @GraphComputerVerificationInjectionNotSupported @DataGeometry
  Scenario: g_injectXgeometriesX_isXtypeOfXGType_GEOMETRYXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT(1 2)"), Geometry("LINESTRING(0 0, 1 1)"), Geometry("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))"), Geometry("POINT(13.4 52.5)", GEOGRAPHIC)).is(P.typeOf(GType.GEOMETRY))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[POINT(1 2)] |
      | geo[LINESTRING(0 0, 1 1)] |
      | geo[POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))] |
      | geo[SRID=4326;POINT(13.4 52.5)] |

  @GraphComputerVerificationInjectionNotSupported @DataGeometry
  Scenario: g_injectXgeometriesX_isXtypeOfXGType_POINTXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT(1 2)"), Geometry("LINESTRING(0 0, 1 1)"), Geometry("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))"), Geometry("POINT(13.4 52.5)", GEOGRAPHIC)).is(P.typeOf(GType.POINT))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[POINT(1 2)] |
      | geo[SRID=4326;POINT(13.4 52.5)] |

  @GraphComputerVerificationInjectionNotSupported @DataGeometry
  Scenario: g_injectXgeometriesX_isXtypeOfXGType_LINESTRINGXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT(1 2)"), Geometry("LINESTRING(0 0, 1 1)"), Geometry("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))"), Geometry("POINT(13.4 52.5)", GEOGRAPHIC)).is(P.typeOf(GType.LINESTRING))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[LINESTRING(0 0, 1 1)] |

  @GraphComputerVerificationInjectionNotSupported @DataGeometry
  Scenario: g_injectXgeometriesX_isXtypeOfXGType_POLYGONXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT(1 2)"), Geometry("LINESTRING(0 0, 1 1)"), Geometry("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))"), Geometry("POINT(13.4 52.5)", GEOGRAPHIC)).is(P.typeOf(GType.POLYGON))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))] |

  @GraphComputerVerificationInjectionNotSupported @DataGeometry
  Scenario: g_injectXgeometriesX_isXtypeOfXGType_CARTESIANXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT(1 2)"), Geometry("LINESTRING(0 0, 1 1)"), Geometry("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))"), Geometry("POINT(13.4 52.5)", GEOGRAPHIC)).is(P.typeOf(GType.CARTESIAN))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[POINT(1 2)] |
      | geo[LINESTRING(0 0, 1 1)] |
      | geo[POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))] |

  @GraphComputerVerificationInjectionNotSupported @DataGeometry
  Scenario: g_injectXgeometriesX_isXtypeOfXGType_GEOGRAPHICX_andXtypeOfXGType_POINTXXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT(1 2)"), Geometry("LINESTRING(13.4 52.5, 13.5 52.6)", GEOGRAPHIC), Geometry("POINT(13.4 52.5)", GEOGRAPHIC)).is(P.typeOf(GType.GEOGRAPHIC).and(P.typeOf(GType.POINT)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[SRID=4326;POINT(13.4 52.5)] |

  @GraphComputerVerificationInjectionNotSupported @DataGeometry
  Scenario: g_injectXmultiGeometriesX_isXtypeOfXGType_MULTIPOINTXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("MULTIPOINT((1 1), (2 2))"), Geometry("MULTILINESTRING((0 0, 1 1), (2 2, 3 3))"), Geometry("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))"), Geometry("GEOMETRYCOLLECTION(POINT(1 1))")).is(P.typeOf(GType.MULTIPOINT))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[MULTIPOINT((1 1), (2 2))] |

  @GraphComputerVerificationInjectionNotSupported @DataGeometry
  Scenario: g_injectXmultiGeometriesX_isXtypeOfXGType_MULTILINESTRINGXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("MULTIPOINT((1 1), (2 2))"), Geometry("MULTILINESTRING((0 0, 1 1), (2 2, 3 3))"), Geometry("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))"), Geometry("GEOMETRYCOLLECTION(POINT(1 1))")).is(P.typeOf(GType.MULTILINESTRING))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[MULTILINESTRING((0 0, 1 1), (2 2, 3 3))] |

  @GraphComputerVerificationInjectionNotSupported @DataGeometry
  Scenario: g_injectXmultiGeometriesX_isXtypeOfXGType_MULTIPOLYGONXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("MULTIPOINT((1 1), (2 2))"), Geometry("MULTILINESTRING((0 0, 1 1), (2 2, 3 3))"), Geometry("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))"), Geometry("GEOMETRYCOLLECTION(POINT(1 1))")).is(P.typeOf(GType.MULTIPOLYGON))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))] |

  @GraphComputerVerificationInjectionNotSupported @DataGeometry
  Scenario: g_injectXmultiGeometriesX_isXtypeOfXGType_GEOMETRYCOLLECTIONXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("MULTIPOINT((1 1), (2 2))"), Geometry("MULTILINESTRING((0 0, 1 1), (2 2, 3 3))"), Geometry("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))"), Geometry("GEOMETRYCOLLECTION(POINT(1 1))")).is(P.typeOf(GType.GEOMETRYCOLLECTION))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[MULTIPOINT((1 1), (2 2))] |
      | geo[MULTILINESTRING((0 0, 1 1), (2 2, 3 3))] |
      | geo[MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))] |
      | geo[GEOMETRYCOLLECTION(POINT(1 1))] |

  @AllowGeometryPropertyValues @DataGeometry
  Scenario: g_V_hasXloc_typeOfXGType_POLYGONXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().has("loc", P.typeOf(GType.POLYGON)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Midtown |
      | North Brooklyn and Queens |
      | Greenwich Town |
      | Congestion Zone |
      | Draft Zone |
      | Chilled |
      | Ambient |
