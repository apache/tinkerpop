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
 * Reference implementation for asString() step, a mid-traversal step which returns the incoming traverser value
 * as a string. Null values are returned unchanged.
 *
 * @author David Bechberger (http://bechberger.com)
 * @author Yang Xia (http://github.com/xiazcy)
 */
public final class AsStringLocalStep<S, E> extends ScalarMapStep<S, E> {

    public AsStringLocalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        final S item = traverser.get();

        if (null == item) {
            throw new IllegalArgumentException("Can't parse null as String.");
        } else if ((item instanceof Iterable) || (item instanceof Iterator) || item.getClass().isArray()) {
            final List<String> resList = new ArrayList<>();
            final Iterator<E> iterator = IteratorUtils.asIterator(item);
            while (iterator.hasNext()) {
                final E i = iterator.next();
                if (i == null)
                    throw new IllegalArgumentException("Can't parse null as String.");
                resList.add(String.valueOf(i));
            }
            return (E) resList;
        } else {
            return (E) String.valueOf(item);
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

}
