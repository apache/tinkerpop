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
package com.apache.tinkerpop.gremlin.process.traversal.util;

import com.apache.tinkerpop.gremlin.process.Traversal;
import com.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalRing<A, B> {

    private final IdentityTraversal<A, B> identityTraversal = new IdentityTraversal<>();

    private List<Traversal.Admin<A, B>> traversals = new ArrayList<>();
    private int currentTraversal = -1;

    public TraversalRing(final Traversal.Admin<A,B>... traversals) {
        this.traversals = new ArrayList<>(Arrays.asList(traversals));
    }

    public Traversal.Admin<A, B> next() {
        if (this.traversals.size() == 0) {
            return this.identityTraversal;
        } else {
            this.currentTraversal = (this.currentTraversal + 1) % this.traversals.size();
            return this.traversals.get(this.currentTraversal);
        }
    }

    public boolean isEmpty() {
        return this.traversals.isEmpty();
    }

    public void reset() {
        this.currentTraversal = -1;
    }

    public int size() {
        return this.traversals.size();
    }

    public void addTraversal(final Traversal.Admin<A, B> traversal) {
        this.traversals.add(traversal);
    }

    public List<Traversal.Admin<A, B>> getTraversals() {
        return Collections.unmodifiableList(this.traversals);
    }

    @Override
    public String toString() {
        return this.traversals.toString();
    }
}
