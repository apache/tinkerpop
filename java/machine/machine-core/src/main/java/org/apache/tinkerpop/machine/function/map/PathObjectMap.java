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
package org.apache.tinkerpop.machine.function.map;

import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.bytecode.compiler.Compilation;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.MapFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.path.Path;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PathObjectMap<C, S, E> extends AbstractFunction<C> implements MapFunction<C, S, E> {

    private final String pathLabel;
    private Compilation<C, Object, Object> byCompilation;

    private PathObjectMap(final Coefficient<C> coefficient, final String label, final String pathLabel, final Compilation<C, Object, Object> byCompilation) {
        super(coefficient, label);
        this.pathLabel = pathLabel;
        this.byCompilation = byCompilation;
    }

    @Override
    public E apply(final Traverser<C, S> traverser) {
        final Object object = traverser.path().get(Path.Pop.last, this.pathLabel);
        return (E) (null == this.byCompilation ? object : this.byCompilation.mapObject(object).object());
    }


    @Override
    public int hashCode() {
        return super.hashCode() ^ this.pathLabel.hashCode() ^ (null == this.byCompilation ? 1 : this.byCompilation.hashCode());
    }

    @Override
    public PathObjectMap<C, S, E> clone() {
        final PathObjectMap<C, S, E> clone = (PathObjectMap<C, S, E>) super.clone();
        clone.byCompilation = null == this.byCompilation ? null : this.byCompilation.clone();
        return clone;
    }

    public static <C, S, E> PathObjectMap<C, S, E> compile(final Instruction<C> instruction) {
        return new PathObjectMap<>(instruction.coefficient(), instruction.label(), (String) instruction.args()[0], Compilation.compileOrNull(1, instruction.args()));
    }
}
