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
package org.apache.tinkerpop.machine.bytecode;

import org.apache.tinkerpop.machine.traverser.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MethodArgument<E> implements Argument<E> {

    private final String method;

    public MethodArgument(final String method) {
        this.method = method.split("::")[1];
    }

    @Override
    public <C, S> E mapArg(final Traverser<C, S> traverser) {
        if (this.method.equals("object"))
            return (E) traverser.object();
        else if (this.method.equals("count"))
            return (E) traverser.coefficient().count();
        else
            throw new RuntimeException("Unknown method");
    }

    @Override
    public <C, S> boolean filterArg(final Traverser<C, S> traverser) {
        return false;
    }
}
