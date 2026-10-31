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
package org.apache.tinkerpop.gremlin.structure.geo;

/**
 * Defines the coordinate interpretation model for a geometry.
 * <p>
 * {@code CARTESIAN} is planar, with math in coordinate units. TinkerPop defines it completely.
 * {@code GEOGRAPHIC} is longitude/latitude on the earth (WGS84), with geodetic math in meters.
 * {@code OTHER} describes data in any other coordinate system, such as a map projection.
 * <p>
 * {@code OTHER} is only ever reported by {@link Geometry#model()}. It is never passed to a constructor,
 * because a geometry in another coordinate system is meaningless without knowing which one,
 * and the SRID says exactly that.
 *
 * @since 4.0.0
 */
public enum Model {

    /**
     * Planar coordinates with math in coordinate units. SRID 0.
     *
     * @since 4.0.0
     */
    CARTESIAN,

    /**
     * Longitude/latitude on WGS84 earth surface. SRID 4326.
     *
     * @since 4.0.0
     */
    GEOGRAPHIC,

    /**
     * Any other spatial reference system. Reported by {@link Geometry#model()} for SRIDs
     * other than 0 and 4326. Never accepted by constructors.
     *
     * @since 4.0.0
     */
    OTHER
}
