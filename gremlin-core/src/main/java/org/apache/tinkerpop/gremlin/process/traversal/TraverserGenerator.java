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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Iterator;
import java.util.Set;

/**
 * A TraverserGenerator will generate traversers for a particular {@link Traversal}. In essence, wrap objects in a {@link Traverser}.
 * Typically the {@link TraverserGenerator} chosen is determined by the {@link TraverserRequirement} of the {@link Traversal}.
 * Simple requirements, simpler traversers. Complex requirements, complex traversers.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraverserGenerator {

    public Set<TraverserRequirement> getProvidedRequirements();

    public <S> Traverser.Admin<S> generate(final S start, final Step<S, ?> startStep, final long initialBulk);

    public default <S> Iterator<Traverser.Admin<S>> generateIterator(final Iterator<S> starts, final Step<S, ?> startStep, final long initialBulk) {
        return new Iterator<Traverser.Admin<S>>() {
            @Override
            public boolean hasNext() {
                return starts.hasNext();
            }

            @Override
            public Traverser.Admin<S> next() {
                return generate(starts.next(), startStep, initialBulk);
            }
        };
    }
}
