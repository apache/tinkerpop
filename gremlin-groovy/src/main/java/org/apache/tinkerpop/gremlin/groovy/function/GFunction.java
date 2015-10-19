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

import java.util.function.Function;

/**
 * @deprecated As of release 3.1.0, use {@code as Function} in Groovy.
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Deprecated
public final class GFunction<A, B> implements Function<A, B>, LambdaHolder {

    private final Closure closure;

    public GFunction(final Closure closure) {
        this.closure = closure;
    }

    @Override
    public B apply(A a) {
        return (B) this.closure.call(a);
    }

    public static GFunction[] make(final Closure... closures) {
        final GFunction[] functions = new GFunction[closures.length];
        for (int i = 0; i < closures.length; i++) {
            functions[i] = new GFunction(closures[i]);
        }
        return functions;
    }

    @Override
    public String toString() {
        return "lambda";
    }
}
