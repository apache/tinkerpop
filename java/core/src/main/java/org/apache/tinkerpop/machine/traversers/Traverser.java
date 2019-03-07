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
package org.apache.tinkerpop.machine.traversers;

import org.apache.tinkerpop.machine.coefficients.Coefficient;

import java.util.Collections;
import java.util.HashSet;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Traverser<C, S> {

    private final Coefficient<C> coefficient;
    private final S object;
    private Path path = new Path();

    public Traverser(final Coefficient<C> coefficient, final S object) {
        this.coefficient = coefficient;
        this.object = object;
    }

    public Coefficient<C> coefficient() {
        return this.coefficient;
    }

    public S object() {
        return this.object;
    }

    public Path path() {
        return this.path;
    }

    public void addLabel(final String label) {
        this.path.addLabels(Collections.singleton(label));
    }

    public <B> Traverser<C, B> split(final B object) {
        final Traverser<C, B> traverser = new Traverser<>(this.coefficient.clone(), object);
        traverser.path = new Path(this.path);
        traverser.path.add(new HashSet<>(), object);
        return traverser;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof Traverser && ((Traverser<C, S>) other).object.equals(this.object);
    }

    @Override
    public String toString() {
        return this.object.toString();
    }
}
