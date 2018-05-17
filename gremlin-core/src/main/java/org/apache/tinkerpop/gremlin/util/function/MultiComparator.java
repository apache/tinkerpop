/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.util.function;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MultiComparator<C> implements Comparator<C>, Serializable {

    private List<Comparator> comparators;
    private boolean isShuffle;
    int startIndex = 0;

    private MultiComparator() {
        // for serialization purposes
    }

    public MultiComparator(final List<Comparator<C>> comparators) {
        this.comparators = (List) comparators;
        this.isShuffle = !this.comparators.isEmpty() && Order.shuffle == this.comparators.get(this.comparators.size() - 1);
        for (int i = 0; i < this.comparators.size(); i++) {
            if (this.comparators.get(i) == Order.shuffle)
                this.startIndex = i + 1;
        }
    }

    @Override
    public int compare(final C objectA, final C objectB) {
        if (this.comparators.isEmpty()) {
            return Order.asc.compare(objectA, objectB);
        } else {
            for (int i = this.startIndex; i < this.comparators.size(); i++) {
                final int comparison = this.comparators.get(i).compare(this.getObject(objectA, i), this.getObject(objectB, i));
                if (comparison != 0)
                    return comparison;
            }
            return 0;
        }
    }

    public boolean isShuffle() {
        return this.isShuffle;
    }

    private final Object getObject(final C object, final int index) {
        if (object instanceof ProjectedTraverser)
            return ((ProjectedTraverser) object).getProjections().get(index);
        else
            return object;
    }
}
