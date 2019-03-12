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

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.functions.AbstractFunction;
import org.apache.tinkerpop.machine.functions.BranchFunction;
import org.apache.tinkerpop.machine.traversers.Traverser;
import org.apache.tinkerpop.util.StringFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RepeatBranch<C, S> extends AbstractFunction<C, S, Iterator<Traverser<C, S>>> implements BranchFunction<C, S, S> {

    private final Compilation<C, S, S> repeat;
    private final Compilation<C, S, ?> until;

    public RepeatBranch(final Coefficient<C> coefficient, final Set<String> labels, final List<Compilation<C, S, ?>> repeatCompilations) {
        super(coefficient, labels);
        this.repeat = (Compilation<C, S, S>) repeatCompilations.get(0);
        this.until = repeatCompilations.get(1);
    }

    @Override
    public Iterator<Traverser<C, S>> apply(final Traverser<C, S> traverser) {
        this.repeat.addTraverser(traverser);
        final List<Traverser<C, S>> toGo = new ArrayList<>();
        while (this.repeat.getProcessor().hasNext()) {
            Traverser<C, S> t = this.repeat.getProcessor().next();
            boolean loop = !this.until.filterTraverser(t);
            if (loop)
                this.repeat.getProcessor().addStart(t);
            else
                toGo.add(t);
        }
        return toGo.iterator();
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.repeat, this.until);
    }

    @Override
    public List<Compilation<C, ?, ?>> getInternals() {
        return Arrays.asList(this.repeat, this.until);
    }
}
