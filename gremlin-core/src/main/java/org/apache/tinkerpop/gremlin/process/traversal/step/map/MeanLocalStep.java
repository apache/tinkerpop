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
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.util.NumberHelper.add;
import static org.apache.tinkerpop.gremlin.util.NumberHelper.div;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class MeanLocalStep<E extends Number, S extends Iterable<E>> extends MapStep<S, E> {

    public MeanLocalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        while (true) {
            final Traverser.Admin<S> traverser = this.starts.next();
            final Iterator<E> iterator = IteratorUtils.asIterator(traverser.get());

            if (iterator.hasNext()) {
                // forward the iterator to the first non-null or return null
                E result = untilNonNull(iterator);
                Long counter = 1L;
                while (iterator.hasNext()) {
                    final Number n = iterator.next();
                    if (n != null) {
                        result = (E) add(result, n);
                        counter++;
                    }
                }
                return traverser.split((E) div(result, counter, true), this);
            }
        }
    }

    private E untilNonNull(final Iterator<E> itty) {
        E result = null;
        while (itty.hasNext() && null == result) {
            result = itty.next();
        }
        return result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
