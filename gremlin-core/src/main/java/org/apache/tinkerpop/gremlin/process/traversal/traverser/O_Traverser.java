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
package org.apache.tinkerpop.gremlin.process.traversal.traverser;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class O_Traverser<T> extends AbstractTraverser<T> {

    protected Set<String> tags = null;

    protected O_Traverser() {
    }

    public O_Traverser(final T t) {
        super(t);
    }


    public Set<String> getTags() {
        if (null == this.tags) this.tags = new HashSet<>();
        return this.tags;
    }

    @Override
    public <R> Admin<R> split(final R r, final Step<T, R> step) {
        final O_Traverser<R> clone = (O_Traverser<R>) super.split(r, step);
        if (null != this.tags)
            clone.tags = new HashSet<>(this.tags);
        return clone;
    }

    @Override
    public Admin<T> split() {
        final O_Traverser<T> clone = (O_Traverser<T>) super.split();
        if (null != this.tags)
            clone.tags = new HashSet<>(this.tags);
        return clone;
    }

    @Override
    public void merge(final Traverser.Admin<?> other) {
        super.merge(other);
        if (!other.getTags().isEmpty()) {
            if (this.tags == null) this.tags = new HashSet<>();
            this.tags.addAll(other.getTags());
        }
    }
}
