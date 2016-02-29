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

package org.apache.tinkerpop.gremlin.process.computer;

import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.Serializable;

/**
 * A {@code VertexComputeKey} specifies a property of a vertex that will be used to store {@link GraphComputer} data.
 * If the VertexComputeKey is specified as transient, it will be dropped from the vertex prior to returning the {@link ComputerResult} graph.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class VertexComputeKey implements Serializable {

    private final String key;
    private final boolean isTransient;

    private VertexComputeKey(final String key, final boolean isTransient) {
        this.key = key;
        this.isTransient = isTransient;
        ElementHelper.validateProperty(key, key);
    }

    public String getKey() {
        return this.key;
    }

    public boolean isTransient() {
        return this.isTransient;
    }

    @Override
    public int hashCode() {
        return this.key.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof VertexComputeKey && ((VertexComputeKey) object).key.equals(this.key);
    }

    public static VertexComputeKey of(final String key, final boolean isTransient) {
        return new VertexComputeKey(key, isTransient);
    }
}
