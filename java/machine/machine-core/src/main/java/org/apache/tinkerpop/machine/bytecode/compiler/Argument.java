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

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Argument<E> extends Serializable, Cloneable {

    public <C, S> E mapArg(final Traverser<C, S> traverser);

    public <C, S> Iterator<E> flatMapArg(final Traverser<C, S> traverser);

    public <C, S> boolean filterArg(final Traverser<C, S> traverser);

    public static <C, S, E> Argument<E> create(final Object... args) {
        if (args[0] instanceof Bytecode)
            return new BytecodeArgument<>((Bytecode<C>) args[0]);
        else if (args[0] instanceof String && ((String) args[0]).contains("::"))
            return new MethodArgument<>((String) args[0], Arrays.copyOfRange(args, 1, args.length));
        else
            return new ConstantArgument<>((E) args[0]);
    }

    public Argument<E> clone();

}
