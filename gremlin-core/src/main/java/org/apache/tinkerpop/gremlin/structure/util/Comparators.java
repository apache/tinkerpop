/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Comparator;
import java.util.Map;

/**
 * A collection of commonly used {@link Comparator} instances.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Comparators {

    private Comparators() {}

    /**
     * Sorts {@link Element} objects  by the {@code toString()} value of {@link Element#id()} using
     * {@link String#CASE_INSENSITIVE_ORDER}.
     */
    public static final Comparator<Element> ELEMENT_COMPARATOR = Comparator.comparing(e -> e.id().toString(), String.CASE_INSENSITIVE_ORDER);

    /**
     * Sorts {@link Vertex} objects  by the {@code toString()} value of {@link Vertex#id()} using
     * {@link String#CASE_INSENSITIVE_ORDER}.
     */
    public static final Comparator<Vertex> VERTEX_COMPARATOR = Comparator.comparing(e -> e.id().toString(), String.CASE_INSENSITIVE_ORDER);

    /**
     * Sorts {@link Edge} objects  by the {@code toString()} value of {@link Edge#id()} using
     * {@link String#CASE_INSENSITIVE_ORDER}.
     */
    public static final Comparator<Edge> EDGE_COMPARATOR = Comparator.comparing(e -> e.id().toString(), String.CASE_INSENSITIVE_ORDER);

    /**
     * Sorts {@link Property} objects  by the value of {@link Property#key()} using
     * {@link String#CASE_INSENSITIVE_ORDER}.
     */
    public static final Comparator<Property> PROPERTY_COMPARATOR = Comparator.comparing(Property::key, String.CASE_INSENSITIVE_ORDER);
}
