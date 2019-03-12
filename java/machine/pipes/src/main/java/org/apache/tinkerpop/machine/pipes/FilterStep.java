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
package org.apache.tinkerpop.machine.pipes;

import org.apache.tinkerpop.machine.functions.FilterFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FilterStep<C, S> extends AbstractStep<C, S, S> {

    private final FilterFunction<C, S> filterFunction;
    private Traverser<C, S> nextTraverser = null;

    public FilterStep(final AbstractStep<C, ?, S> previousStep, final FilterFunction<C, S> filterFunction) {
        super(previousStep, filterFunction);
        this.filterFunction = filterFunction;
    }

    @Override
    public Traverser<C, S> next() {
        if (null != this.nextTraverser) {
            final Traverser<C, S> traverser = this.nextTraverser;
            this.nextTraverser = null;
            return traverser;
        } else {
            Traverser<C, S> traverser;
            while (true) {
                traverser = this.getPreviousTraverser();
                if (traverser.filter(this.filterFunction))
                    return traverser;
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (null != this.nextTraverser)
            return true;
        else {
            Traverser<C, S> traverser;
            while (super.hasNext()) {
                traverser = this.getPreviousTraverser();
                if (traverser.filter(this.filterFunction)) {
                    this.nextTraverser = traverser;
                    return true;
                }
            }
            return false;
        }
    }
}
