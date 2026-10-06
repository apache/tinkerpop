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
package org.apache.tinkerpop.gremlin.structure.snapshot.spi;

/**
 * Visits the vertex properties of a vertex. The arguments, including {@code metaProperties}, are valid only during the
 * call.
 */
public interface VertexPropertyVisitor {

    /**
     * Visits one vertex property. It is called once per vertex property, so a key repeats for a multi-property, with
     * the vertex properties of that key in source order. Duplicate values are kept.
     *
     * @param id             the vertex-property identifier, which must not be null and must be a scalar value
     * @param key            the property key
     * @param value          the value, which may be null
     * @param metaProperties the meta-properties of this vertex property, possibly empty. It is valid only during the
     *                       call, and its values may be null.
     */
    void vertexProperty(Object id, String key, Object value, PropertySource metaProperties);
}
