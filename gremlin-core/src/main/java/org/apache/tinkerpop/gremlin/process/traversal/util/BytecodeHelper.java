/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Utility class for parsing {@link Bytecode}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class BytecodeHelper {

    private BytecodeHelper() {
        // public static methods only
    }

    /**
     * Parses {@link Bytecode} to find {@link TraversalStrategy} objects added in the source instructions.
     */
    public static <A extends TraversalStrategy> Iterator<A> findStrategies(final Bytecode bytecode, final Class<A> clazz) {
        return IteratorUtils.map(
                IteratorUtils.filter(bytecode.getSourceInstructions().iterator(),
                        s -> s.getOperator().equals(TraversalSource.Symbols.withStrategies) && clazz.isAssignableFrom(s.getArguments()[0].getClass())),
                os -> (A) os.getArguments()[0]);
    }

    public static Bytecode filterInstructions(final Bytecode bytecode, final Predicate<Bytecode.Instruction> predicate) {
        final Bytecode clone = new Bytecode();
        for (final Bytecode.Instruction instruction : bytecode.getSourceInstructions()) {
            if (predicate.test(instruction))
                clone.addSource(instruction.getOperator(), instruction.getArguments());
        }
        for (final Bytecode.Instruction instruction : bytecode.getStepInstructions()) {
            if (predicate.test(instruction))
                clone.addStep(instruction.getOperator(), instruction.getArguments());
        }
        return clone;
    }

    /**
     * Checks if the bytecode is one of the standard {@link Bytecode#GRAPH_OPERATIONS}.
     */
    public static boolean isGraphOperation(final Bytecode bytecode) {
        return Bytecode.GRAPH_OPERATIONS.contains(bytecode);
    }

    public static Optional<String> getLambdaLanguage(final Bytecode bytecode) {
        for (final Bytecode.Instruction instruction : bytecode.getInstructions()) {
            for (Object object : instruction.getArguments()) {
                if (object instanceof Lambda)
                    return Optional.of(((Lambda) object).getLambdaLanguage());
                else if (object instanceof Bytecode) {
                    final Optional<String> temp = BytecodeHelper.getLambdaLanguage((Bytecode) object);
                    if (temp.isPresent())
                        return temp;
                }
            }
        }
        return Optional.empty();
    }

    public static void removeBindings(final Bytecode bytecode) {
        for (final Bytecode.Instruction instruction : bytecode.getInstructions()) {
            final Object[] arguments = instruction.getArguments();
            for (int i = 0; i < arguments.length; i++) {
                if (arguments[i] instanceof Bytecode.Binding)
                    arguments[i] = ((Bytecode.Binding) arguments[i]).value();
                else if (arguments[i] instanceof Bytecode)
                    removeBindings((Bytecode) arguments[i]);
            }
        }
    }

    public static void detachElements(final Bytecode bytecode) {
        for (final Bytecode.Instruction instruction : bytecode.getInstructions()) {
            final Object[] arguments = instruction.getArguments();
            for (int i = 0; i < arguments.length; i++) {
                if (arguments[i] instanceof Bytecode)
                    detachElements((Bytecode) arguments[i]);
                else if(arguments[i] instanceof List) {
                    final List<Object> list = new ArrayList<>();
                    for(final Object object : (List)arguments[i]) {
                        list.add(DetachedFactory.detach(object, false));
                    }
                    arguments[i] = list;
                }
                else
                    arguments[i] = DetachedFactory.detach(arguments[i], false);
            }
        }
    }
}
