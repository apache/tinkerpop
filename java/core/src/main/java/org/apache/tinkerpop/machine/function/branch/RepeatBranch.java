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
package org.apache.tinkerpop.machine.function.branch;

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.BranchFunction;
import org.apache.tinkerpop.machine.function.branch.selector.Selector;
import org.apache.tinkerpop.util.StringFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class RepeatBranch<C, S> extends AbstractFunction<C> implements BranchFunction<C, S, S, Boolean> {

    private final Map<Character, Compilation<C, S, S>> compilations;
    private final Compilation<C, S, S> repeat;
    private final Compilation<C, S, S> until;

    public RepeatBranch(final Coefficient<C> coefficient, final Set<String> labels, final List<Object> arguments) {
        super(coefficient, labels);
        this.compilations = new LinkedHashMap<>();
        for (int i = 0; i < arguments.size(); i = i + 2) {
            this.compilations.put((Character) arguments.get(i), (Compilation<C, S, S>) arguments.get(i + 1));
        }

        this.repeat = this.compilations.get('r');
        this.until = this.compilations.get('u');
    }

    @Override
    public String toString() {
        return StringFactory.makeFunctionString(this, this.compilations);
    }

    public Compilation<C, S, S> getRepeat() {
        return this.repeat;
    }

    public Compilation<C, S, ?> getUntil() {
        return this.until;
    }

    public Map<Character, Compilation<C, S, S>> getCompilations() {
        return this.compilations;
    }

    @Override
    public Selector<C, S, Boolean> getBranchSelector() {
        return null;
    }

    @Override
    public Map<Boolean, List<Compilation<C, S, S>>> getBranches() {
        return null;
    }
}
