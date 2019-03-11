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
package org.apache.tinkerpop.machine.beam;

import org.apache.tinkerpop.machine.coefficients.LongCoefficient;
import org.apache.tinkerpop.machine.functions.InitialFunction;
import org.apache.tinkerpop.machine.traversers.CompleteTraverser;
import org.apache.tinkerpop.machine.traversers.Traverser;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class InitialFn<C, S> extends AbstractFn<C, S, S> {

    private final InitialFunction<C, S> initialFunction;

    public InitialFn(final InitialFunction<C, S> initialFunction) {
        super(initialFunction);
        this.initialFunction = initialFunction;
    }

    @ProcessElement
    public void processElement(final @Element Traverser<C, S> traverser, final OutputReceiver<Traverser<C, S>> output) {
        final Iterator<S> iterator = this.initialFunction.get();
        while (iterator.hasNext()) {
            output.output(new CompleteTraverser(LongCoefficient.create(), iterator.next()));
        }
    }
}