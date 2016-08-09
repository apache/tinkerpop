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
import org.apache.tinkerpop.gremlin.util.function.Lambda;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class BytecodeHelper {

    private BytecodeHelper() {
        // public static methods only
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

    public static Optional<String> getLambdaLanguage(final Bytecode bytecode) {
        for (final Bytecode.Instruction instruction : bytecode.getSourceInstructions()) {
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
        for (final Bytecode.Instruction instruction : bytecode.getStepInstructions()) {
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
}
