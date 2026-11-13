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

/**
 * Utility class holding the Gremlin Console's {@code Geometry(...)} functions, which build a
 * geometry from WKT or EWKT text. Each overload is a proxy to the matching
 * {@link Geometry#fromWkt(String)} factory.
 * <p>
 *
 * @since 4.0.0
 */
public final class GeometryHelper {

    private GeometryHelper() {}

    /**
     * A proxy call to {@link Geometry#fromWkt(String)} for use as a console function.
     *
     * @param wktOrEwkt the WKT or EWKT text
     * @return the parsed geometry
     * @since 4.0.0
     */
    public static Geometry Geometry(final String wktOrEwkt) {
        return Geometry.fromWkt(wktOrEwkt);
    }

    /**
     * A proxy call to {@link Geometry#fromWkt(String, Model)} for use as a console function.
     *
     * @param wkt the WKT text
     * @param model {@link Model#CARTESIAN} or {@link Model#GEOGRAPHIC}
     * @return the parsed geometry
     * @since 4.0.0
     */
    public static Geometry Geometry(final String wkt, final Model model) {
        return Geometry.fromWkt(wkt, model);
    }

    /**
     * A proxy call to {@link Geometry#fromWkt(String, int)} for use as a console function.
     *
     * @param wkt the WKT text
     * @param srid the spatial reference ID
     * @return the parsed geometry
     * @since 4.0.0
     */
    public static Geometry Geometry(final String wkt, final int srid) {
        return Geometry.fromWkt(wkt, srid);
    }

    /**
     * A proxy call to {@link Geometry#fromWkt(String, Model)} that names the model with a
     * {@link GType}. This overload exists because the console statically imports every
     * {@code GType} value, so a bare {@code CARTESIAN} or {@code GEOGRAPHIC} written as the
     * second argument is {@link GType#CARTESIAN} or {@link GType#GEOGRAPHIC} rather than the
     * {@link Model} of the same name.
     *
     * @param wkt the WKT text
     * @param model {@link GType#CARTESIAN} or {@link GType#GEOGRAPHIC}
     * @return the parsed geometry
     * @throws IllegalArgumentException if {@code model} is null or is any other {@code GType}
     * @since 4.0.0
     */
    public static Geometry Geometry(final String wkt, final GType model) {
        if (model == null) {
            throw new IllegalArgumentException("model cannot be null");
        }
        if (model == GType.CARTESIAN) {
            return Geometry.fromWkt(wkt, Model.CARTESIAN);
        }
        if (model == GType.GEOGRAPHIC) {
            return Geometry.fromWkt(wkt, Model.GEOGRAPHIC);
        }
        throw new IllegalArgumentException("model must be GType.CARTESIAN or GType.GEOGRAPHIC, got " + model);
    }
}
