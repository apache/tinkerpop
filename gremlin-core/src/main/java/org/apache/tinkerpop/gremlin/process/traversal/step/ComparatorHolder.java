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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.javatuples.Pair;

import java.util.Comparator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ComparatorHolder<S, C extends Comparable> {

    /**
     * Add a {@link Traversal}-based {@link Comparator} to the holder.
     * If no traversal is needed, use {@link IdentityTraversal}.
     *
     * @param traversal  the traversal to pre-process the object by.
     * @param comparator the comparator to compare the result of the object after traversal processing
     */
    public void addComparator(final Traversal.Admin<S, C> traversal, final Comparator<C> comparator);

    /**
     * Get the comparators associated with this holder.
     * The comparators are ordered according to their oder of operation.
     *
     * @return a list of {@link Traversal}/{@link Comparator}-pairs
     */
    public List<Pair<Traversal.Admin<S, C>, Comparator<C>>> getComparators();

}
