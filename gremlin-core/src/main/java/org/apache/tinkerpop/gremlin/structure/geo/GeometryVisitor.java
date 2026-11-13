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
 * Package-private visitor over the geometry kinds, one method per kind. A geometry hands itself
 * to the matching method through {@link Geometry#accept(GeometryVisitor)}, so that per-kind
 * behaviour such as serialization lives with the algorithm. Each Multi* kind routes to its own
 * visit method, so an implementation of {@link #visitGeometryCollection(GeometryCollection)}
 * receives a plain GEOMETRYCOLLECTION rather than a Multi* value.
 *
 * @param <R> the result type of a visit
 */
interface GeometryVisitor<R> {

    R visitPoint(Point point);

    R visitLineString(LineString lineString);

    R visitPolygon(Polygon polygon);

    R visitMultiPoint(MultiPoint multiPoint);

    R visitMultiLineString(MultiLineString multiLineString);

    R visitMultiPolygon(MultiPolygon multiPolygon);

    R visitGeometryCollection(GeometryCollection geometryCollection);
}
