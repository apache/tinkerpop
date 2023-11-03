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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ScalarMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public abstract class StringLocalStep<S, E> extends ScalarMapStep<S, E> {

    public StringLocalStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        final S item = traverser.get();

        if (null == item) {
            // we will pass null values to next step
            return null;
        } else if ((item instanceof Iterable) || (item instanceof Iterator) || item.getClass().isArray()) {
            final List<E> resList = new ArrayList<>();
            final Iterator<E> iterator = IteratorUtils.asIterator(item);
            while (iterator.hasNext()) {
                final E i = iterator.next();
                if (null == i) {
                    // we will pass null values to next step
                    resList.add(null);
                } else if (i instanceof String) {
                    resList.add(applyStringOperation((String) i));
                } else {
                    throw new IllegalArgumentException(
                            String.format("The %s step can only take string or list of strings, encountered %s in list", getStepName(), i.getClass()));
                }
            }
            return (E) resList;
        } else if (item instanceof String) {
            return applyStringOperation((String) item);
        } else {
            throw new IllegalArgumentException(
                    String.format("The %s step can only take string or list of strings, encountered %s", getStepName(), item.getClass()));
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    protected abstract E applyStringOperation(final String item);

    protected abstract String getStepName();

}
