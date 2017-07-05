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
package org.apache.tinkerpop.gremlin.process.traversal.lambda;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LoopTraversal<S> extends AbstractLambdaTraversal<S, S> {

    private final long maxLoops;
    private boolean allow = false;

    public LoopTraversal(final long maxLoops) {
        this.maxLoops = maxLoops;
    }

    public long getMaxLoops() {
        return maxLoops;
    }

    @Override
    public boolean hasNext() {
        return this.allow;
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {
        this.allow = start.loops() >= this.maxLoops;
    }

    @Override
    public String toString() {
        return "loops(" + this.maxLoops + ')';
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ Long.hashCode(this.maxLoops);
    }
}