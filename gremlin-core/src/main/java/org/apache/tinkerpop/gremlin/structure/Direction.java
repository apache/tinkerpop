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
package org.apache.tinkerpop.gremlin.structure;

/**
 * {@link Direction} is used to denote the direction of an {@link Edge} or location of a {@link Vertex} on an
 * {@link Edge}. For example:
 * <p/>
 * <pre>
 * gremlin--knows--&gt;rexster
 * </pre>
 * is an {@link Direction#OUT} {@link Edge} for Gremlin and an {@link Direction#IN} edge for Rexster. Moreover, given
 * that {@link Edge}, Gremlin is the {@link Direction#OUT} {@link Vertex} and Rexster is the {@link Direction#IN}
 * {@link Vertex}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public enum Direction {

    /**
     * Refers to an outgoing direction.
     */
    OUT,

    /**
     * Refers to an incoming direction.
     */
    IN,

    /**
     * Refers to either direction ({@link #IN} or {@link #OUT}).
     */
    BOTH;

    /**
     * The actual direction of an {@link Edge} may only be {@link #IN} or {@link #OUT}, as defined in this array.
     */
    public static final Direction[] proper = new Direction[]{OUT, IN};

    /**
     * Produce the opposite representation of the current {@code Direction} enum.
     */
    public Direction opposite() {
        if (this.equals(OUT))
            return IN;
        else if (this.equals(IN))
            return OUT;
        else
            return BOTH;
    }
}
