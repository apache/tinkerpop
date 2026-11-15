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

@StepClassData @DataGeometry
Feature: Data - Geometry

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXGeometryXofEachKindXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT(1 2)"), Geometry("LINESTRING(0 0, 1 1)"), Geometry("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))"), Geometry("MULTIPOINT((1 1), (2 2))"), Geometry("MULTILINESTRING((0 0, 1 1), (2 2, 3 3))"), Geometry("MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))"), Geometry("GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(2 2, 3 3))"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[POINT(1 2)] |
      | geo[LINESTRING(0 0, 1 1)] |
      | geo[POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))] |
      | geo[MULTIPOINT((1 1), (2 2))] |
      | geo[MULTILINESTRING((0 0, 1 1), (2 2, 3 3))] |
      | geo[MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))] |
      | geo[GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(2 2, 3 3))] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXgeometryXPOINTX1_2XX_GeometryXPOINTX1_2XXX
    Given the empty graph
    And the traversal of
      """
      g.inject(geometry("POINT(1 2)"), Geometry("POINT(1 2)"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[POINT(1 2)] |
      | geo[POINT(1 2)] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXGeometryXPOINTX13_4_52_5X_GEOGRAPHICXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT(13.4 52.5)", GEOGRAPHIC))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[SRID=4326;POINT(13.4 52.5)] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXGeometryXPOINTX1_2X_32618XX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT(1 2)", 32618))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[SRID=32618;POINT(1 2)] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXGeometryXofEmptyKindsXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT EMPTY"), Geometry("POLYGON EMPTY"), Geometry("GEOMETRYCOLLECTION EMPTY"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[POINT EMPTY] |
      | geo[POLYGON EMPTY] |
      | geo[GEOMETRYCOLLECTION EMPTY] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXGeometryXPOINT_ZX1_2_3XXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT Z (1 2 3)"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[POINT Z (1 2 3)] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXGeometryXPOINT_Z_EMPTYXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT Z EMPTY"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[POINT Z EMPTY] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXlistOfGeometryX
    Given the empty graph
    And the traversal of
      """
      g.inject([Geometry("POINT(1 2)"), Geometry("POINT(3 4)")])
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[geo[POINT(1 2)],geo[POINT(3 4)]] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXmapOfGeometryX
    Given the empty graph
    And the traversal of
      """
      g.inject([loc:Geometry("POINT(1 2)")])
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"loc":"geo[POINT(1 2)]"}] |

  @AllowGeometryPropertyValues
  Scenario: g_V_valuesXlocX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("store").property("loc", Geometry("POINT(13.4 52.5)", GEOGRAPHIC))
      """
    And the traversal of
      """
      g.V().values("loc")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[SRID=4326;POINT(13.4 52.5)] |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasXloc_GeometryXPOLYGONXX_valuesXnameX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("zone").property("name", "midtown").property("loc", Geometry("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))"))
      """
    And the traversal of
      """
      g.V().has("loc", Geometry("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | midtown |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasXloc_xx1X_valuesXnameX
    Given the empty graph
    And using the parameter xx1 defined as "geo[SRID=4326;POINT(13.4 52.5)]"
    And the graph initializer of
      """
      g.addV("store").property("name", "midtown").property("loc", Geometry("POINT(13.4 52.5)", GEOGRAPHIC))
      """
    And the traversal of
      """
      g.V().has("loc", xx1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | midtown |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasLabelXhubX_valuesXlocX
    Given the delivery graph
    And the traversal of
      """
      g.V().hasLabel("hub").values("loc")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[SRID=4326;POINT(-73.9925 40.7614)] |
      | geo[SRID=4326;POINT(-73.9571 40.7146)] |
      | geo[SRID=4326;POINT(-118.2437 34.0522)] |
      | geo[SRID=4326;POINT(0 51.48)] |
      | geo[SRID=4326;POINT(174.7633 -36.8485)] |
