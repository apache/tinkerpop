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
package org.apache.tinkerpop.gremlin.tinkergraph.structure.storage;

import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * A single element change within a committing transaction, handed to a {@link TinkerStorage} engine to persist. This
 * is the stable, public view of a change that decouples storage engines from TinkerGraph's internal transactional
 * containers. A mutation is either a <em>put</em> (the element was added or modified, {@link #element()} is non-null)
 * or a <em>delete</em> ({@link #element()} is {@code null} and {@link #isDeleted()} is {@code true}).
 *
 * @param <T> the element type ({@code TinkerVertex} or {@code TinkerEdge})
 */
public final class TinkerStorageMutation<T extends Element> {

    private final Object id;
    private final T element;

    /**
     * Create a mutation for the given element id.
     *
     * @param id      the element identifier (never {@code null})
     * @param element the committed element for a put, or {@code null} for a delete
     */
    public TinkerStorageMutation(final Object id, final T element) {
        this.id = id;
        this.element = element;
    }

    /**
     * The identifier of the changed element.
     */
    public Object id() {
        return id;
    }

    /**
     * The committed element to persist, or {@code null} when this mutation is a deletion.
     */
    public T element() {
        return element;
    }

    /**
     * Returns {@code true} when this mutation deletes the element rather than adding or modifying it.
     */
    public boolean isDeleted() {
        return element == null;
    }
}
