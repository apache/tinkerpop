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
import org.apache.tinkerpop.gremlin.process.traversal.util.ListFunction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asList;

/**
 * Reference implementation for reverse() step, a mid-traversal step which returns the reverse value.
 * Null values are not processed and remain as null when returned.
 *
 * @author David Bechberger (http://bechberger.com)
 * @author Yang Xia (http://github.com/xiazcy)
 */
public final class ReverseStep<S, E> extends ScalarMapStep<S, E> {

    public ReverseStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        final S items = traverser.get();

        if (null == items) {
            return null;
        } else if (items instanceof String) {
            return (E) new StringBuilder(((String) items)).reverse().toString();
        } else if ((items instanceof Iterable) || (items instanceof Iterator) || items.getClass().isArray()) {
            final List itemsAsList = IteratorUtils.asList(items);
            Collections.reverse(itemsAsList);
            return (E) itemsAsList;
        } else {
            // null traversers are passed on without throwing an exception so the same should be done with other types.
            return (E) items;
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

}
