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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collection;
import java.util.Set;

/**
 * List functions are a separate paradigm for Steps as they behave in a Scope.Local manner by default. This class
 * provides helper methods that are common amongst these steps and should be used to keep behavior consistent in terms
 * of handling of incoming traversers and argument types.
 */
public interface ListFunction {

    /**
     * Turn an iterable type into a collection. Doesn't wrap any non-iterable type into an iterable (e.g. single Object
     * into list), but will transform one iterable type to another (e.g. array to list).
     *
     * @param iterable an Iterable or array.
     * @return The iterable type as a Collection or null if argument isn't iterable.
     */
    public static Collection asCollection(final Object iterable) {
        if (iterable instanceof Collection) {
            return (Collection) iterable;
        } else if ((iterable != null && iterable.getClass().isArray()) || iterable instanceof Iterable) {
            return (Collection) IteratorUtils.asList(iterable);
        } else {
            return null;
        }
    }

    /**
     * Turn an iterable type into a set. Doesn't wrap any non-iterable type into an iterable (e.g. single Object
     * into set), but will transform one iterable type to another (e.g. array to set).
     *
     * @param iterable an Iterable or array.
     * @return The iterable type as a Collection or null if argument isn't iterable.
     */
    public static Set asSet(final Object iterable) {
        if (iterable instanceof Iterable || iterable != null && iterable.getClass().isArray()) {
            return IteratorUtils.asSet(iterable);
        } else {
            return null;
        }
    }

    /**
     * Template method used for retrieving the implementing Step's name.
     *
     * @return this step's name.
     */
    public String getStepName();

    public default Collection convertArgumentToCollection(final Object arg) {
        if (null == arg) {
            throw new IllegalArgumentException(
                    String.format("Argument provided for %s step can't be null.", getStepName()));
        }

        final Collection incoming = asCollection(arg);

        if (null == incoming) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s step can only take an array or an Iterable as an argument, encountered %s",
                            getStepName(),
                            arg.getClass()));
        }

        return incoming;
    }

    public default <S> Collection convertTraverserToCollection(final Traverser.Admin<S> traverser) {
        final S items = traverser.get();

        if (null == items) {
            throw new IllegalArgumentException(
                    String.format("Incoming traverser for %s step can't be null.", getStepName()));
        }

        final Collection incoming = asCollection(items);

        if (null == incoming) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s step can only take an array or an Iterable type for incoming traversers, encountered %s",
                            getStepName(),
                            items.getClass()));
        }

        return incoming;
    }

    public default <S, E> Collection convertTraversalToCollection(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        final Object array = TraversalUtil.apply(traverser, traversal);

        if (null == array) {
            throw new IllegalArgumentException(
                    String.format(
                            "Provided traversal argument for %s step must yield an iterable type, not null",
                            getStepName()));
        }

        final Collection input = asCollection(array);

        if (null == input) {
            throw new IllegalArgumentException(
                    String.format(
                            "Provided traversal argument for %s step must yield an iterable type, encountered %s",
                            getStepName(),
                            array.getClass()));
        }

        return input;
    }

    public default Set convertArgumentToSet(final Object arg) {
        if (null == arg) {
            throw new IllegalArgumentException(
                    String.format("Argument provided for %s step can't be null.", getStepName()));
        }

        final Set incoming = asSet(arg);

        if (null == incoming) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s step can only take an array or an Iterable as an argument, encountered %s",
                            getStepName(),
                            arg.getClass()));
        }

        return incoming;
    }

    public default <S> Set convertTraverserToSet(final Traverser.Admin<S> traverser) {
        final S items = traverser.get();

        if (null == items) {
            throw new IllegalArgumentException(
                    String.format("Incoming traverser for %s step can't be null.", getStepName()));
        }

        final Set incoming = asSet(items);

        if (null == incoming) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s step can only take an array or an Iterable type for incoming traversers, encountered %s",
                            getStepName(),
                            items.getClass()));
        }

        return incoming;
    }

    public default <S, E> Set convertTraversalToSet(final Traverser.Admin<S> traverser, final Traversal.Admin<S, E> traversal) {
        final Object array = TraversalUtil.apply(traverser, traversal);

        if (null == array) {
            throw new IllegalArgumentException(
                    String.format(
                            "Provided traversal argument for %s step must yield an iterable type, not null",
                            getStepName()));
        }

        final Set input = asSet(array);

        if (null == input) {
            throw new IllegalArgumentException(
                    String.format(
                            "Provided traversal argument for %s step must yield an iterable type, encountered %s",
                            getStepName(),
                            array.getClass()));
        }

        return input;
    }
}
