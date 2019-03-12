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

import java.io.Serializable;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CompilationRing<C, S, E> implements Serializable {

    private List<Compilation<C, S, E>> compilations;
    private int currentCompilation = -1;

    public CompilationRing(final List<Compilation<C, S, E>> compilations) {
        this.compilations = compilations;
    }

    public Compilation<C, S, E> next() {
        if (this.compilations.isEmpty()) {
            return null;
        } else {
            this.currentCompilation = (this.currentCompilation + 1) % this.compilations.size();
            return this.compilations.get(this.currentCompilation);
        }
    }

    public boolean isEmpty() {
        return this.compilations.isEmpty();
    }

    public void reset() {
        this.currentCompilation = -1;
    }

    public int size() {
        return this.compilations.size();
    }


    @Override
    public String toString() {
        return this.compilations.toString();
    }
}
