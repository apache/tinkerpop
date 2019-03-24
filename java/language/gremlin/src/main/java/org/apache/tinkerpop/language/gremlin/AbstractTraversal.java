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
package org.apache.tinkerpop.language.gremlin;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractTraversal<C, S, E> implements Traversal<C, S, E> {

    protected Coefficient<C> currentCoefficient;
    protected final Bytecode<C> bytecode;
    private Compilation<C, S, E> compilation;
    private boolean locked = false; // TODO: when a traversal has been submitted, we need to make sure new modulations can't happen.

    // iteration helpers
    private long lastCount = 0L;
    private E lastObject = null;

    public AbstractTraversal(final Bytecode<C> bytecode, final Coefficient<C> unity) {
        this.bytecode = bytecode;
        this.currentCoefficient = unity.clone();
    }

    ///////

    private final void prepareTraversal() {
        if (null == this.compilation)
            this.compilation = Compilation.compile(this.bytecode);
    }

    public Traverser<C, E> nextTraverser() {
        this.prepareTraversal();
        return this.compilation.getProcessor().next(); // TODO: interaction with hasNext/next and counts
    }

    @Override
    public boolean hasNext() {
        this.prepareTraversal();
        return this.lastCount > 0 || this.compilation.getProcessor().hasNext();
    }

    @Override
    public E next() {
        this.prepareTraversal();
        if (this.lastCount > 0) {
            this.lastCount--;
            return this.lastObject;
        } else {
            final Traverser<C, E> traverser = this.compilation.getProcessor().next();
            if (traverser.coefficient().count() > 1) {
                this.lastObject = traverser.object();
                this.lastCount = traverser.coefficient().count() - 1L;
            }
            return traverser.object();
        }
    }

    public List<E> toList() {
        final List<E> list = new ArrayList<>();
        while (this.hasNext()) {
            list.add(this.next());
        }
        return list;
    }

    @Override
    public String toString() {
        return this.bytecode.toString();
    }
}
