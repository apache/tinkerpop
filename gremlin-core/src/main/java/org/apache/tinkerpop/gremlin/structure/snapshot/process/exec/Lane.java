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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.exec;

/**
 * The kind of element a native frontier or {@link Batch} carries, see the execution vocabulary of the spike document.
 * Which {@link Batch} arrays are used for each lane:
 * <table>
 * <caption>Batch layout per lane</caption>
 * <tr><th>Lane</th><th>{@code ord}</th><th>{@code key}</th><th>{@code aux}</th><th>{@code src}</th><th>{@code val}</th></tr>
 * <tr><td>{@link #V}</td><td>vertex ordinal</td><td></td><td></td><td></td><td></td></tr>
 * <tr><td>{@link #E}</td><td>edge ordinal</td><td></td><td></td><td>source vertex, only if the batch records
 * sources</td><td></td></tr>
 * <tr><td>{@link #VP}</td><td>owner vertex</td><td>vertex key code</td><td>vertex-property ordinal</td><td></td>
 * <td></td></tr>
 * <tr><td>{@link #EP}</td><td>edge ordinal</td><td>edge key code</td><td></td><td></td><td></td></tr>
 * <tr><td>{@link #MP}</td><td>owner vertex</td><td>vertex key code</td><td>vertex-property ordinal</td>
 * <td>meta key code</td><td></td></tr>
 * <tr><td>{@link #VAL}</td><td></td><td>column id, or {@link Batch#DECODED}</td><td>column entry</td><td></td>
 * <td>decoded value when {@code key} is {@code DECODED}</td></tr>
 * <tr><td>{@link #SCALAR}</td><td></td><td></td><td></td><td></td><td>the value of the single entry</td></tr>
 * </table>
 * Every entry has a {@code long} bulk in {@code Batch.bulk}.
 */
public enum Lane {
    /**
     * Vertices.
     */
    V,
    /**
     * Edges, optionally with the vertex they were reached from.
     */
    E,
    /**
     * Vertex properties.
     */
    VP,
    /**
     * Edge properties.
     */
    EP,
    /**
     * Meta-properties.
     */
    MP,
    /**
     * Values, either lazy column references or decoded objects.
     */
    VAL,
    /**
     * The single value produced by a reducing operator.
     */
    SCALAR;

    /**
     * Whether the lane is {@link #V} or {@link #E}, the lanes that frontiers and bulk-merging operate on.
     */
    public boolean isElement() {
        return this == V || this == E;
    }

    /**
     * Whether the lane is {@link #VP}, {@link #EP} or {@link #MP}.
     */
    public boolean isProperty() {
        return this == VP || this == EP || this == MP;
    }

    /**
     * Whether entries of this lane are turned into facades when they leave the native region.
     */
    public boolean isFacade() {
        return this != VAL && this != SCALAR;
    }
}
