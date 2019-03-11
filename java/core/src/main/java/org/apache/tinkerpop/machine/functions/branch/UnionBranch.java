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
package org.apache.tinkerpop.machine.functions.branch;

import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.functions.AbstractFunction;
import org.apache.tinkerpop.machine.functions.BranchFunction;
import org.apache.tinkerpop.machine.functions.CFunction;
import org.apache.tinkerpop.machine.processor.Processor;
import org.apache.tinkerpop.machine.processor.ProcessorFactory;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.machine.traversers.TraverserFactory;
import org.apache.tinkerpop.util.MultiIterator;
import org.apache.tinkerpop.util.StringFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class UnionBranch<C, S, E> extends AbstractFunction<C, S, Iterator<Traverser<C, E>>> implements BranchFunction<C, S, E> {
    private final List<List<CFunction<C>>> branchFunctions;


    private transient List<Processor<C, S, E>> processors;
    private TraverserFactory<C> traverserFactory;
    private ProcessorFactory processorFactory;

    public UnionBranch(final Coefficient<C> coefficient, final Set<String> labels, final List<List<CFunction<C>>> branchFunctions) {
        super(coefficient, labels);
        this.branchFunctions = branchFunctions;
    }

    @Override
    public Iterator<Traverser<C, E>> apply(final Traverser<C, S> traverser) {
        if (null == this.processors) {
            this.processors = new ArrayList<>(this.branchFunctions.size());
            for (final List<CFunction<C>> functions : this.branchFunctions) {
                this.processors.add(processorFactory.mint(traverserFactory, functions));
            }
        }
        final MultiIterator<Traverser<C, E>> iterator = new MultiIterator<>();
        for (final Processor<C, S, E> processor : this.processors) {
            processor.reset();
            processor.addStart(traverser.clone());
            iterator.addIterator(processor);
        }
        return iterator;
    }

    @Override
    public void setProcessor(final TraverserFactory<C> traverserFactory, final ProcessorFactory processorFactory) {
        this.traverserFactory = traverserFactory;
        this.processorFactory = processorFactory;
    }

    @Override
    public List<List<CFunction<C>>> getBranches() {
        return this.branchFunctions;
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.branchFunctions.toArray());
    }
}
