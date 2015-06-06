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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents the end of a vertex in a serialization stream.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class VertexTerminator {
    public static final VertexTerminator INSTANCE = new VertexTerminator();

    public final byte[] terminal;

    private VertexTerminator() {
        terminal = ByteBuffer.allocate(8).putLong(4185403236219066774L).array();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final VertexTerminator that = (VertexTerminator) o;

        return terminal == that.terminal;

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(terminal);
    }

    public static VertexTerminator instance() {
        return INSTANCE;
    }
}
