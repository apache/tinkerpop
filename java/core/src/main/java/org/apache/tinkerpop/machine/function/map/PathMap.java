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

import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.bytecode.CompilationCircle;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.function.AbstractFunction;
import org.apache.tinkerpop.machine.function.MapFunction;
import org.apache.tinkerpop.machine.traverser.Path;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathMap<C, S> extends AbstractFunction<C> implements MapFunction<C, S, Path> {

    private final CompilationCircle<C, Object, Object> compilationCircle;

    public PathMap(final Coefficient<C> coefficient, final Set<String> labels, final List<Compilation<C, Object, Object>> byMaps) {
        super(coefficient, labels);
        this.compilationCircle = new CompilationCircle<>(byMaps);
    }

    @Override
    public Path apply(final Traverser<C, S> traverser) {
        if (!this.compilationCircle.isEmpty()) {
            final Path oldPath = traverser.path();
            final Path newPath = new Path();
            for (int i = 0; i < oldPath.size(); i++) {
                newPath.add(oldPath.labels(i), this.compilationCircle.next().mapObject(oldPath.object(i)).object());
            }
            return newPath;
        } else
            return traverser.path();
    }
}
