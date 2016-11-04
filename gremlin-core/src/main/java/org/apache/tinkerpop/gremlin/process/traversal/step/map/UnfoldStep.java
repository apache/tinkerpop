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
import org.apache.tinkerpop.gremlin.util.iterator.ArrayIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnfoldStep<S, E> extends FlatMapStep<S, E> {

    public UnfoldStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<S> traverser) {
        final S s = traverser.get();
        if (s instanceof Iterator)
            return (Iterator) s;
        else if (s instanceof Iterable)
            return ((Iterable) s).iterator();
        else if (s instanceof Map)
            return ((Map) s).entrySet().iterator();
        else if (s.getClass().isArray())
            return handleArrays(s);
        else
            return IteratorUtils.of((E) s);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    private final Iterator<E> handleArrays(final Object array) {
        if (array instanceof Object[]) {
            return new ArrayIterator<>((E[]) array);
        } else {
            int len = Array.getLength(array);
            final Object[] objectArray = new Object[len];
            for (int i = 0; i < len; i++)
                objectArray[i] = Array.get(array, i);
            return new ArrayIterator<>((E[]) objectArray);
        }
    }
}
