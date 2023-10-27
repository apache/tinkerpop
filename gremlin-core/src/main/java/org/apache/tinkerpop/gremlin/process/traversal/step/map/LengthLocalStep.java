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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Reference implementation for length() step, a mid-traversal step which returns the length of the incoming string
 * traverser. Null values are not processed and remain as null when returned. If the incoming traverser is a non-String
 * value then an {@code IllegalArgumentException} will be thrown.
 *
 * @author David Bechberger (http://bechberger.com)
 * @author Yang Xia (http://github.com/xiazcy)
 */
public final class LengthLocalStep<S, E> extends ScalarMapStep<S, E> {

    public LengthLocalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        final S item = traverser.get();
        if (null == item) {
            // we will pass null values to next step
            return null;
        }

        if ((item instanceof Iterable) || (item instanceof Iterator) || item.getClass().isArray()) {
            final List<Integer> resList = new ArrayList<>();
            final Iterator<E> iterator = IteratorUtils.asIterator(item);
            while (iterator.hasNext()) {
                final E i = iterator.next();
                if (null == i) {
                    // we will pass null values to next step
                    resList.add(null);
                } else if (i instanceof String) {
                    resList.add(((String) i).length());
                } else {
                    throw new IllegalArgumentException(
                            String.format("The length(local) step can only take list of strings, encountered %s in list", i.getClass()));
                }
            }
            return (E) resList;
        } else {
            // String values not wrapped in a list will not be processed in local scope
            throw new IllegalArgumentException(
                    String.format("The length(local) step can only take list of strings, encountered %s", item.getClass()));
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

}
