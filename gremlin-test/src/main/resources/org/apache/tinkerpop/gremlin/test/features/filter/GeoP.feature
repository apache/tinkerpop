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

@StepClassFilter @StepIs @DataGeometry
Feature: Predicate - GeoP

  @AllowGeometryPropertyValues
  Scenario: g_V_hasLabelXhub_customerX_hasXloc_geoWithinXnorthBrooklynAndQueensXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().hasLabel("hub", "customer").has("loc", GeoP.geoWithin(Geometry("POLYGON((-73.97 40.69, -73.88 40.69, -73.88 40.8, -73.93 40.8, -73.93 40.735, -73.97 40.735, -73.97 40.69))", GEOGRAPHIC))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Williamsburg |

  @AllowGeometryPropertyValues
  Scenario: g_V_unionXhasLabelXzone_waterwayX_valuesXlocX_outEXdeliveredX_valuesXpathXX_isXgeoWithinXdeliveryBoxXX
    Given the delivery graph
    And the traversal of
      """
      g.V().union(__.hasLabel("zone", "waterway").values("loc"), __.outE("delivered").values("path")).is(GeoP.geoWithin(Geometry("POLYGON((-74.03 40.69, -73.95 40.69, -73.95 40.766, -74.03 40.766, -74.03 40.69))", GEOGRAPHIC)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[SRID=4326;LINESTRING(-73.9571 40.7146, -73.9857 40.7484)] |
      | geo[SRID=4326;POLYGON((-74.02 40.7, -73.975 40.7, -73.975 40.7644, -74.02 40.7644, -74.02 40.7))] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXemptyAndGeographicAndZPointsX_isXgeoWithinXpolygonXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT EMPTY"), Geometry("POINT(2 2)", GEOGRAPHIC), Geometry("POINT Z (2 2 9)")).is(GeoP.geoWithin(Geometry("POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[POINT Z (2 2 9)] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXpointX_isXgeoWithinXemptyPolygonXX
    Given the empty graph
    And the traversal of
      """
      g.inject(Geometry("POINT(2 2)")).is(GeoP.geoWithin(Geometry("POLYGON EMPTY")))
      """
    When iterated to list
    Then the result should be empty

  @AllowGeometryPropertyValues
  Scenario: g_V_hasLabelXhub_customerX_hasXloc_geoWithinXconstantXgreenwichTownXXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().hasLabel("hub", "customer").has("loc", GeoP.geoWithin(__.constant(Geometry("POLYGON((-0.03 51.46, 0.03 51.46, 0.03 51.5, -0.03 51.5, -0.03 51.46))", GEOGRAPHIC)))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Greenwich |
      | Hana |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasLabelXcustomerX_asXcX_outXorderedFromX_outXservesX_valuesXlocX_asXzX_selectXcX_valuesXlocX_isXgeoWithinXselectXzXXX
    Given the delivery graph
    And the traversal of
      """
      g.V().hasLabel("customer").as("c").out("orderedFrom").out("serves").values("loc").as("z").select("c").values("loc").is(GeoP.geoWithin(__.select("z")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | geo[SRID=4326;POINT Z (-73.9857 40.7484 86)] |
      | geo[SRID=4326;POINT Z (0.012 51.481 0)] |

  @AllowGeometryPropertyValues @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXhub_name_HellsKitchenX_outXservesX_valuesXlocX_asXzX_V_hasLabelXcustomerX_whereXvaluesXlocX_isXgeoWithinXselectXzXXXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().has("hub", "name", "Hell's Kitchen").out("serves").values("loc").as("z").V().hasLabel("customer").where(__.values("loc").is(GeoP.geoWithin(__.select("z")))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ana |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasXhub_name_HellsKitchenX_asXhX_outXservesX_valuesXlocX_asXzX_selectXhX_inXorderedFromX_notXwhereXvaluesXlocX_isXgeoWithinXselectXzXXXXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().has("hub", "name", "Hell's Kitchen").as("h").out("serves").values("loc").as("z").select("h").in("orderedFrom").not(__.where(__.values("loc").is(GeoP.geoWithin(__.select("z"))))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ben |
      | Dee |
      | Eli |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasLabelXcustomerX_hasXloc_geoWithinXmidtownXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().hasLabel("customer").has("loc", GeoP.geoWithin(Geometry("POLYGON((-74.02 40.74, -73.93 40.74, -73.93 40.81, -74.02 40.81, -74.02 40.74), (-73.9819 40.7681, -73.973 40.7644, -73.9498 40.7968, -73.958 40.8006, -73.9819 40.7681))", GEOGRAPHIC))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ana |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasLabelXzoneX_hasXloc_geoContainsXdeePointXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().hasLabel("zone").has("loc", GeoP.geoContains(Geometry("POINT(-73.93 40.77)", GEOGRAPHIC))).values("name")
      """
    When iterated to list
    Then the result should be empty

  @AllowGeometryPropertyValues
  Scenario: g_V_hasLabelXzoneX_hasXloc_geoContainsXhellsKitchenPointXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().hasLabel("zone").has("loc", GeoP.geoContains(Geometry("POINT(-73.9925 40.7614)", GEOGRAPHIC))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Midtown |
      | Congestion Zone |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasLabelXzoneX_hasXloc_geoContainsXnightMarketCollectionXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().hasLabel("zone").has("loc", GeoP.geoContains(Geometry("GEOMETRYCOLLECTION(MULTIPOINT((-73.952 40.715), (-73.951 40.716)), POLYGON((-73.955 40.713, -73.949 40.713, -73.949 40.717, -73.955 40.717, -73.955 40.713)))", GEOGRAPHIC))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | North Brooklyn and Queens |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasXloc_geoWithinXaucklandMultipolygonXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().has("loc", GeoP.geoWithin(Geometry("MULTIPOLYGON(((174.74 -36.87, 174.79 -36.87, 174.79 -36.84, 174.74 -36.84, 174.74 -36.87)), ((174.98 -36.82, 175.06 -36.82, 175.06 -36.77, 174.98 -36.77, 174.98 -36.82)))", GEOGRAPHIC))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Auckland |
      | Auckland and Waiheke |

  @AllowGeometryPropertyValues
  Scenario: g_V_hasLabelXcustomerX_hasXloc_geoWithinXcongestionZoneXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().hasLabel("customer").has("loc", GeoP.geoWithin(Geometry("POLYGON((-74.02 40.7, -73.975 40.7, -73.975 40.7644, -74.02 40.7644, -74.02 40.7))", GEOGRAPHIC))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ana |

  @AllowGeometryPropertyValues @AllowMixedGeometryModels
  Scenario: g_V_hasXloc_geoWithinXchilledRoomXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().has("loc", GeoP.geoWithin(Geometry("POLYGON((0 0, 8 0, 8 10, 0 10, 0 0))"))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Chilled |
      | Pat |

  @AllowGeometryPropertyValues @AllowMixedGeometryModels
  Scenario: g_V_hasXloc_geoWithinXcartesianGreenwichBoxXX_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.V().has("loc", GeoP.geoWithin(Geometry("POLYGON((-1 51, 1 51, 1 52, -1 52, -1 51))"))).values("name")
      """
    When iterated to list
    Then the result should be empty

  @AllowGeometryPropertyValues
  Scenario: g_E_hasLabelXdeliveredX_hasXpath_geoWithinXwilliamsburgToMidtownBoxXX_inV_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.E().hasLabel("delivered").has("path", GeoP.geoWithin(Geometry("POLYGON((-74 40.7, -73.95 40.7, -73.95 40.76, -74 40.76, -74 40.7))", GEOGRAPHIC))).inV().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Ana |

  @AllowGeometryPropertyValues
  Scenario: g_E_hasLabelXdeliveredX_hasXpath_geoWithinXshiftedWilliamsburgToMidtownBoxXX_inV_valuesXnameX
    Given the delivery graph
    And the traversal of
      """
      g.E().hasLabel("delivered").has("path", GeoP.geoWithin(Geometry("POLYGON((-74.1 40.7, -74.05 40.7, -74.05 40.76, -74.1 40.76, -74.1 40.7))", GEOGRAPHIC))).inV().values("name")
      """
    When iterated to list
    Then the result should be empty
