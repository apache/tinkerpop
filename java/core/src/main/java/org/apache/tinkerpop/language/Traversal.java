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
package org.apache.tinkerpop.language;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.traversers.Path;

/**
 * @author Marko S. Rodriguez (http://markorodriguez.com)
 */
public class Traversal<C, S, E> {

    private final Bytecode<C> bytecode;
    private C currentCoefficient;

    public Traversal(final C unity) {
        this.currentCoefficient = unity;
        this.bytecode = new Bytecode<>();
    }

    public Traversal<C, S, E> as(final String label) {
        this.bytecode.lastInstruction().addLabel(label);
        return this;
    }

    public Traversal<C, S, E> by(final Traversal<C, E, ?> byTraversal) {
        this.bytecode.lastInstruction().addArg(byTraversal);
        return this;
    }

    public Traversal<C, S, E> identity() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.IDENTITY);
        return this;
    }

    public Traversal<C, S, E> is(final E object) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.IS, object);
        return this;
    }

    public Traversal<C, S, Long> incr() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.INCR);
        return (Traversal) this;
    }

    public <R> Traversal<C, S, R> inject(final R... objects) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.INJECT, objects);
        return (Traversal) this;
    }

    public <R> Traversal<C, S, R> map(final Traversal<C, E, R> mapTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.MAP, mapTraversal.getBytecode());
        return (Traversal) this;
    }

    public Traversal<C, S, Path> path() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.PATH);
        return (Traversal) this;
    }


    // create a utility class that directly access the class field so we don't have to do the Admin stuff in TP4.
    public Bytecode<C> getBytecode() {
        return this.bytecode;
    }

}
