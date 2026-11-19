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

  @AllowGeometryPropertyValues
  Scenario: g_V_hasXname_GreenwichX_valuesXlocX
    Given the delivery graph
    And the traversal of
      """
      g.V().has("name", "Greenwich").values("loc")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[SRID=4326;POINT(0 51.48)] |

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

  @AllowGeometryPropertyValues
  Scenario: g_V_hasXname_AnaX_valuesXlocX
    Given the delivery graph
    And the traversal of
      """
      g.V().has("name", "Ana").values("loc")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[SRID=4326;POINT Z (-73.9857 40.7484 86)] |

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

  @AllowGeometryPropertyValues
  Scenario: g_V_hasLabelXroomX_order_byXnameX_valuesXlocX_fold
    Given the delivery graph
    And the traversal of
      """
      g.V().hasLabel("room").order().by("name").values("loc").fold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[geo[POLYGON((8 0, 20 0, 20 10, 8 10, 8 0), (13 4, 15 4, 15 6, 13 6, 13 4))],geo[POLYGON((0 0, 8 0, 8 10, 0 10, 0 0))]] |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasXpicker_name_PatX_projectXname_locX_byXnameX_byXlocX
    Given the delivery graph
    And the traversal of
      """
      g.V().has("picker", "name", "Pat").project("name", "loc").by("name").by("loc")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name":"Pat","loc":"geo[POINT(3 4)]"}] |

  @AllowGeometryPropertyValues
  Scenario: g_V_valuesXlocX
    Given the delivery graph
    And the traversal of
      """
      g.V().values("loc")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[SRID=4326;POINT(-73.9925 40.7614)] |
      | geo[SRID=4326;POINT(-73.9571 40.7146)] |
      | geo[SRID=4326;POINT(-118.2437 34.0522)] |
      | geo[SRID=4326;POINT(0 51.48)] |
      | geo[SRID=4326;POINT(174.7633 -36.8485)] |
      | geo[SRID=4326;POLYGON((-74.02 40.74, -73.93 40.74, -73.93 40.81, -74.02 40.81, -74.02 40.74), (-73.9819 40.7681, -73.973 40.7644, -73.9498 40.7968, -73.958 40.8006, -73.9819 40.7681))] |
      | geo[SRID=4326;POLYGON((-73.97 40.69, -73.88 40.69, -73.88 40.8, -73.93 40.8, -73.93 40.735, -73.97 40.735, -73.97 40.69))] |
      | geo[SRID=4326;POLYGON((-0.03 51.46, 0.03 51.46, 0.03 51.5, -0.03 51.5, -0.03 51.46))] |
      | geo[SRID=4326;MULTIPOLYGON(((174.74 -36.87, 174.79 -36.87, 174.79 -36.84, 174.74 -36.84, 174.74 -36.87)), ((174.98 -36.82, 175.06 -36.82, 175.06 -36.77, 174.98 -36.77, 174.98 -36.82)))] |
      | geo[SRID=4326;MULTILINESTRING((-73.975 40.695, -73.965 40.72, -73.958 40.745, -73.951 40.77), (-73.958 40.745, -73.946 40.755, -73.945 40.77))] |
      | geo[SRID=4326;GEOMETRYCOLLECTION(MULTIPOINT((-73.952 40.715), (-73.951 40.716)), POLYGON((-73.955 40.713, -73.949 40.713, -73.949 40.717, -73.955 40.717, -73.955 40.713)))] |
      | geo[SRID=4326;POINT Z (-73.9857 40.7484 86)] |
      | geo[SRID=4326;POINT(-73.9654 40.7829)] |
      | geo[SRID=4326;POINT(-73.93 40.77)] |
      | geo[SRID=4326;POINT Z (-74.05 40.72 12)] |
      | geo[SRID=4326;POINT Z (0.012 51.481 0)] |
      | geo[POLYGON((0 0, 8 0, 8 10, 0 10, 0 0))] |
      | geo[POLYGON((8 0, 20 0, 20 10, 8 10, 8 0), (13 4, 15 4, 15 6, 13 6, 13 4))] |
      | geo[POINT(3 4)] |
      | geo[SRID=4326;POLYGON((-74.02 40.7, -73.975 40.7, -73.975 40.7644, -74.02 40.7644, -74.02 40.7))] |
      | geo[SRID=4326;POLYGON((-73.96 40.7, -73.94 40.72, -73.96 40.72, -73.94 40.7, -73.96 40.7))] |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasXloc_midtownPolygonX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().has("loc", Geometry("POLYGON((-74.02 40.74, -73.93 40.74, -73.93 40.81, -74.02 40.81, -74.02 40.74), (-73.9819 40.7681, -73.973 40.7644, -73.9498 40.7968, -73.958 40.8006, -73.9819 40.7681))", GEOGRAPHIC)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Midtown |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasXloc_xx1X_valuesXnameX
    Given the delivery graph
    And using the parameter xx1 defined as "geo[SRID=4326;POINT(0 51.48)]"
    And the traversal of
      """
      g.V().has("loc", xx1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Greenwich |

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
