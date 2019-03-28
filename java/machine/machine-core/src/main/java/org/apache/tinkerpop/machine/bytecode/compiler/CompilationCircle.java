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
package org.apache.tinkerpop.machine.bytecode.compiler;

import org.apache.tinkerpop.machine.traverser.Traverser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CompilationCircle<C, S, E> implements Serializable, Cloneable {

    private List<Compilation<C, S, E>> compilations;
    private final boolean hasCompilations;
    private int currentCompilation = -1;

    public CompilationCircle(final List<Compilation<C, S, E>> compilations) {
        this.compilations = compilations;
        this.hasCompilations = !this.compilations.isEmpty();
    }

    public E processObject(final S object) {
        if (this.hasCompilations) {
            this.currentCompilation = (this.currentCompilation + 1) % this.compilations.size();
            return this.compilations.get(this.currentCompilation).mapObject(object).object();
        } else {
            return (E) object;
        }
    }

    public E processTraverser(final Traverser<C, S> traverser) {
        if (this.hasCompilations) {
            this.currentCompilation = (this.currentCompilation + 1) % this.compilations.size();
            return this.compilations.get(this.currentCompilation).mapTraverser(traverser).object();
        } else {
            return (E) traverser.object();
        }
    }

    public boolean isEmpty() {
        return this.compilations.isEmpty();
    }

    public void reset() {
        this.currentCompilation = -1;
    }

    public int size() {
        return this.compilations.size();
    }

    @Override
    public int hashCode() {
        return this.compilations.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof CompilationCircle && this.compilations.equals(((CompilationCircle) object).compilations);
    }

    @Override
    public String toString() {
        return this.compilations.toString();
    }

    @Override
    public CompilationCircle<C, S, E> clone() {
        try {
            final CompilationCircle<C, S, E> clone = (CompilationCircle<C, S, E>) super.clone();
            clone.compilations = new ArrayList<>(this.compilations.size());
            for (final Compilation<C, S, E> compilation : this.compilations) {
                clone.compilations.add(compilation.clone());
            }
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
