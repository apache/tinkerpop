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

package org.apache.tinkerpop.gremlin.groovy.function;

import groovy.lang.Closure;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;

import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GBiFunction<A, B, C> implements BiFunction<A, B, C>, LambdaHolder {

    private final Closure closure;

    public GBiFunction(final Closure closure) {
        this.closure = closure;
    }

    public static GBiFunction[] make(final Closure... closures) {
        final GBiFunction[] functions = new GBiFunction[closures.length];
        for (int i = 0; i < closures.length; i++) {
            functions[i] = new GBiFunction(closures[i]);
        }
        return functions;
    }

    @Override
    public String toString() {
        return "lambda";
    }

    @Override
    public C apply(final A a, final B b) {
        return (C) closure.call(a, b);
    }
}
