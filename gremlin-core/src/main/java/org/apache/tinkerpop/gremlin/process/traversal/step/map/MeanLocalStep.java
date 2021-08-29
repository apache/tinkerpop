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

import org.apache.tinkerpop.gremlin.util.NumberHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class MeanLocalStep<E extends Number, S extends Iterable<E>> extends ScalarMapStep<S, Number> {

    public MeanLocalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Number map(final Traverser.Admin<S> traverser) {
        final Iterator<E> iterator = traverser.get().iterator();
        if (iterator.hasNext()) {
            // forward the iterator to the first non-null or return null
            E result = untilNonNull(iterator);
            Long counter = 1L;
            while (iterator.hasNext()) {
                final Number n = iterator.next();
                if (n != null) {
                    result = (E) NumberHelper.add(result, n);
                    counter++;
                }
            }
            return NumberHelper.div(result, counter, true);
        }
        throw FastNoSuchElementException.instance();
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
