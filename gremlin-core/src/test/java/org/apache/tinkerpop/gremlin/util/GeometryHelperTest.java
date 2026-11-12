/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.util;

import org.apache.tinkerpop.gremlin.process.traversal.GType;
import org.apache.tinkerpop.gremlin.structure.geo.Geometry;
import org.apache.tinkerpop.gremlin.structure.geo.Model;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GeometryHelperTest {

    private static final String POINT = "POINT(1 2)";
    private static final String POLYGON = "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))";

    @Test
    public void shouldProxyWkt() {
        assertEquals(Geometry.fromWkt(POINT), GeometryHelper.Geometry(POINT));
        assertEquals(Geometry.fromWkt("SRID=4326;" + POINT), GeometryHelper.Geometry("SRID=4326;" + POINT));
    }

    @Test
    public void shouldProxyWktWithModel() {
        assertEquals(Geometry.fromWkt(POINT, Model.CARTESIAN), GeometryHelper.Geometry(POINT, Model.CARTESIAN));
        assertEquals(Geometry.fromWkt(POINT, Model.GEOGRAPHIC), GeometryHelper.Geometry(POINT, Model.GEOGRAPHIC));
    }

    @Test
    public void shouldProxyWktWithSrid() {
        assertEquals(Geometry.fromWkt(POINT, 32618), GeometryHelper.Geometry(POINT, 32618));
        assertEquals(32618, GeometryHelper.Geometry(POINT, 32618).srid());
    }

    @Test
    public void shouldMapGTypeToModel() {
        assertEquals(Geometry.fromWkt(POINT, Model.CARTESIAN), GeometryHelper.Geometry(POINT, GType.CARTESIAN));
        assertEquals(Model.CARTESIAN, GeometryHelper.Geometry(POINT, GType.CARTESIAN).model());

        assertEquals(Geometry.fromWkt(POLYGON, Model.GEOGRAPHIC), GeometryHelper.Geometry(POLYGON, GType.GEOGRAPHIC));
        assertEquals(Model.GEOGRAPHIC, GeometryHelper.Geometry(POLYGON, GType.GEOGRAPHIC).model());
        assertEquals(4326, GeometryHelper.Geometry(POLYGON, GType.GEOGRAPHIC).srid());
    }

    @Test
    public void shouldRejectGTypeThatIsNotAModel() {
        try {
            GeometryHelper.Geometry(POINT, GType.POINT);
            fail("Expected IAE for a GType that does not name a coordinate model");
        } catch (IllegalArgumentException e) {
            assertTrue("Message should name the accepted values, was: " + e.getMessage(),
                    e.getMessage().contains("CARTESIAN") && e.getMessage().contains("GEOGRAPHIC"));
            assertTrue("Message should name the accepted values, was: " + e.getMessage(),
                    e.getMessage().contains("model must be GType.CARTESIAN or GType.GEOGRAPHIC"));
            assertTrue("Message should name the rejected value, was: " + e.getMessage(),
                    e.getMessage().contains(GType.POINT.toString()));
        }
    }

    @Test
    public void shouldRejectNullGType() {
        try {
            GeometryHelper.Geometry(POINT, (GType) null);
            fail("Expected IAE for a null GType");
        } catch (IllegalArgumentException e) {
            assertTrue("Message should name the null model, was: " + e.getMessage(),
                    e.getMessage().contains("model cannot be null"));
        }
    }
}
