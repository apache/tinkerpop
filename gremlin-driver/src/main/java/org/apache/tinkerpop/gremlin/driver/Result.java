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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.message.ResponseResult;

import java.util.Iterator;

/**
 * A {@code Result} represents a result value from the server-side {@link Iterator} of results.  This would be
 * one item from that result set.  This class provides methods for coercing the result {@link Object} to an
 * expected type.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Result {
    final Object resultObject;

    /**
     * Constructs a "result" from data found in {@link ResponseResult#getData()}.
     */
    public Result(final Object responseData) {
        this.resultObject = responseData;
    }

    /**
     * Gets the result item by coercing it to a {@code String} via {@code toString()}.
     */
    public String getString() {
        return resultObject.toString();
    }

    /**
     * Gets the result item by coercing it to an {@code int}.
     *
     * @throws NumberFormatException if the value is not parsable as an {@code int}.
     */
    public int getInt() {
        return Integer.parseInt(resultObject.toString());
    }

    /**
     * Gets the result item by coercing it to an {@code byte}.
     *
     * @throws NumberFormatException if the value is not parsable as an {@code byte}.
     */
    public byte getByte() {
        return Byte.parseByte(resultObject.toString());
    }

    /**
     * Gets the result item by coercing it to an {@code short}.
     *
     * @throws NumberFormatException if the value is not parsable as an {@code short}.
     */
    public short getShort() {
        return Short.parseShort(resultObject.toString());
    }

    /**
     * Gets the result item by coercing it to an {@code long}.
     *
     * @throws NumberFormatException if the value is not parsable as an {@code long}.
     */
    public long getLong() {
        return Long.parseLong(resultObject.toString());
    }

    /**
     * Gets the result item by coercing it to an {@code float}.
     *
     * @throws NumberFormatException if the value is not parsable as an {@code float}.
     */
    public float getFloat() {
        return Float.parseFloat(resultObject.toString());
    }

    /**
     * Gets the result item by coercing it to an {@code double}.
     *
     * @throws NumberFormatException if the value is not parsable as an {@code double}.
     */
    public double getDouble() {
        return Double.parseDouble(resultObject.toString());
    }

    /**
     * Gets the result item by coercing it to an {@code boolean}.
     *
     * @throws NumberFormatException if the value is not parsable as an {@code boolean}.
     */
    public boolean getBoolean() {
        return Boolean.parseBoolean(resultObject.toString());
    }

    /**
     * Determines if the result item is null or not.  This is often a good check prior to calling other methods to
     * get the object as they could throw an unexpected {@link NullPointerException} if the result item is
     * {@code null}.
     */
    public boolean isNull() {
        return null == resultObject;
    }

    /**
     * Gets the result item by casting it to a {@link Vertex}.
     */
    public Vertex getVertex() {
        return (Vertex) resultObject;
    }
    /**
     * Gets the result item by casting it to an {@link Edge}.
     */
    public Edge getEdge() {
        return (Edge) resultObject;
    }
    /**
     * Gets the result item by casting it to an {@link Element}.
     */
    public Element getElement() {
        return (Element) resultObject;
    }

    /**
     * Gets the result item by casting it to a {@link Path}.
     */
    public Path getPath() {
        return (Path) resultObject;
    }

    /**
     * Gets the result item by casting it to a {@link Property}.
     */
    public <V> Property<V> getProperty() {
        return (Property<V>) resultObject;
    }

    /**
     * Gets the result item by casting it to a {@link VertexProperty}.
     */
    public <V> VertexProperty<V> getVertexProperty() {
        return (VertexProperty<V>) resultObject;
    }

    /**
     * Gets the result item by casting it to the specified {@link Class}.
     */
    public <T> T get(final Class<? extends T> clazz) {
        return clazz.cast(this.resultObject);
    }

    /**
     * Gets the result item.
     */
    public Object getObject() {
        return this.resultObject;
    }

    @Override
    public String toString() {
        final String c = resultObject != null ? resultObject.getClass().getCanonicalName() : "null";
        return "result{" +
                "object=" + resultObject + " " +
                "class=" + c +
                '}';
    }
}
