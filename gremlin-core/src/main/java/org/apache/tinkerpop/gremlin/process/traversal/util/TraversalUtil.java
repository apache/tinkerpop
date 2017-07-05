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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalUtil {

    private TraversalUtil() {
    }

    public static final <S, E> E apply(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        final Traverser.Admin<S> split = traverser.split();
        split.setSideEffects(traversal.getSideEffects());
        split.setBulk(1l);
        traversal.reset();
        traversal.addStart(split);
        try {
            return traversal.next(); // map
        } catch (final NoSuchElementException e) {
            throw new IllegalArgumentException("The provided traverser does not map to a value: " + split + "->" + traversal);
        }
    }

    public static final <S, E> Iterator<E> applyAll(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        final Traverser.Admin<S> split = traverser.split();
        split.setSideEffects(traversal.getSideEffects());
        split.setBulk(1l);
        traversal.reset();
        traversal.addStart(split);
        return traversal; // flatmap
    }

    public static final <S, E> boolean test(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal, E end) {
        if (null == end) return TraversalUtil.test(traverser, traversal);

        final Traverser.Admin<S> split = traverser.split();
        split.setSideEffects(traversal.getSideEffects());
        split.setBulk(1l);
        traversal.reset();
        traversal.addStart(split);
        final Step<?, E> endStep = traversal.getEndStep();
        while (traversal.hasNext()) {
            if (endStep.next().get().equals(end))
                return true;
        }
        return false;
    }

    public static final <S, E> E applyNullable(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        return null == traversal ? (E) traverser.get() : TraversalUtil.apply(traverser, traversal);
    }

    public static final <S, E> boolean test(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        final Traverser.Admin<S> split = traverser.split();
        split.setSideEffects(traversal.getSideEffects());
        split.setBulk(1l);
        traversal.reset();
        traversal.addStart(split);
        return traversal.hasNext(); // filter
    }

    ///////

    public static final <S, E> E apply(final S start, final Traversal.Admin<S, E> traversal) {
        traversal.reset();
        traversal.addStart(traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l));
        try {
            return traversal.next(); // map
        } catch (final NoSuchElementException e) {
            throw new IllegalArgumentException("The provided start does not map to a value: " + start + "->" + traversal);
        }
    }

    public static final <S, E> Iterator<E> applyAll(final S start, final Traversal.Admin<S, E> traversal) {
        traversal.reset();
        traversal.addStart(traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l));
        return traversal; // flatMap
    }

    public static final <S, E> boolean test(final S start, final Traversal.Admin<S, E> traversal, final E end) {
        if (null == end) return TraversalUtil.test(start, traversal);

        traversal.reset();
        traversal.addStart(traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l));
        final Step<?, E> endStep = traversal.getEndStep();
        while (traversal.hasNext()) {
            if (endStep.next().get().equals(end))
                return true;
        }
        return false;
    }

    public static final <S, E> E applyNullable(final S start, final Traversal.Admin<S, E> traversal) {
        return null == traversal ? (E) start : TraversalUtil.apply(start, traversal);
    }

    public static final <S, E> boolean test(final S start, final Traversal.Admin<S, E> traversal) {
        traversal.reset();
        traversal.addStart(traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l));
        return traversal.hasNext(); // filter
    }
}
