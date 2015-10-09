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

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SelectColumnStep<S, E> extends MapStep<S, Collection<E>> {

    public enum Column {
        keys,
        values
    }

    private final Column column;

    public SelectColumnStep(final Traversal.Admin traversal, final Column column) {
        super(traversal);
        this.column = column;
    }

    @Override
    protected Collection<E> map(Traverser.Admin<S> traverser) {
        final S start = traverser.get();
        if (start instanceof Map)
            return this.column.equals(Column.keys) ? ((Map<E, ?>) start).keySet() : ((Map<?, E>) start).values();
        else if (start instanceof Map.Entry)
            return Collections.singleton(this.column.equals(Column.keys) ? ((Map.Entry<E, ?>) start).getKey() : ((Map.Entry<?, E>) start).getValue());
        else
            throw new IllegalStateException("The traverser does not reference a map-like object: " + traverser);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.column);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.column.hashCode();
    }
}
