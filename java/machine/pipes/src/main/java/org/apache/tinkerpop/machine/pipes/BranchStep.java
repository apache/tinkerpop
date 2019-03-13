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

import org.apache.tinkerpop.machine.functions.BranchFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BranchStep<C, S, E, M> extends AbstractStep<C, S, E> {

    private final BranchFunction<C, S, E, M> branchFunction;
    private Iterator<Traverser<C, E>> iterator = Collections.emptyIterator();

    public BranchStep(final AbstractStep<C, ?, S> previousStep, final BranchFunction<C, S, E, M> branchFunction) {
        super(previousStep, branchFunction);
        this.branchFunction = branchFunction;
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (this.iterator.hasNext())
                return true;
            else if (super.hasNext())
                this.iterator = super.getPreviousTraverser().branch(this.branchFunction);
            else
                return false;
        }
    }

    @Override
    public Traverser<C, E> next() {
        if (!this.iterator.hasNext())
            this.iterator = super.getPreviousTraverser().branch(this.branchFunction);
        return this.iterator.next();
    }
}