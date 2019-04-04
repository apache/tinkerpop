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

import org.apache.tinkerpop.machine.structure.data.TElement;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.util.IteratorUtils;
import org.apache.tinkerpop.machine.util.NumberHelper;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MethodArgument<E> implements Argument<E> {

    private final String method;
    private final Argument[] arguments;

    public MethodArgument(final String method, final Object... arguments) {
        this.method = method.split("::")[1];
        this.arguments = new Argument[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            this.arguments[i] = Argument.create(arguments[i]);
        }
    }

    @Override
    public <C, S> E mapArg(final Traverser<C, S> traverser) {
        if (this.method.equals("object"))
            return (E) traverser.object();
        else if (this.method.equals("count"))
            return (E) traverser.coefficient().count();
        else if (this.method.equals("keys"))
            return (E) ((Map) traverser.object()).keySet();
        else if (this.method.equals("get"))
            return (E) (traverser.object() instanceof Map ?
                    ((Map) traverser.object()).get(this.arguments[0].mapArg(traverser)) :
                    ((TElement) traverser.object()).get(this.arguments[0].mapArg(traverser)));
        else if (this.method.equals("add"))
            return (E) NumberHelper.add((Number) traverser.object(), (Number) this.arguments[0].mapArg(traverser));
        else
            throw new RuntimeException("Unknown method");
    }

    @Override
    public <C, S> Iterator<E> flatMapArg(final Traverser<C, S> traverser) {
        return IteratorUtils.of(this.mapArg(traverser));
    }

    @Override
    public <C, S> boolean filterArg(final Traverser<C, S> traverser) {
        return false;
    }

    @Override
    public int hashCode() {
        return this.method.hashCode() ^ Arrays.hashCode(this.arguments);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof MethodArgument &&
                this.method.equals(((MethodArgument) object).method) &&
                Arrays.equals(this.arguments, ((MethodArgument) object).arguments);
    }

    @Override
    public MethodArgument<E> clone() {
        return this;
    }

    @Override
    public String toString() {
        return this.method + "(" + Arrays.toString(this.arguments) + ")";
    }
}
