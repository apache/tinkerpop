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
package org.apache.tinkerpop.machine.processor;

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.strategy.Strategy;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.FastNoSuchElementException;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class SimpleProcessor<C, S, E> implements Processor<C, S, E>, ProcessorFactory {

    protected Traverser<C, E> traverser = null;

    @Override
    public Traverser<C, E> next() {
        if (null == this.traverser)
            throw FastNoSuchElementException.instance();
        else {
            final Traverser<C, E> temp = this.traverser;
            this.traverser = null;
            return temp;
        }
    }

    @Override
    public boolean hasNext() {
        return null != this.traverser;
    }

    @Override
    public void reset() {
        this.traverser = null;
    }

    @Override
    public <D, T, F> Processor<D, T, F> mint(final Compilation<D, T, F> compilation) {
        return (Processor<D, T, F>) this;
    }
}
