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

import java.util.Arrays;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Instruction<C> {

    private final C coefficient;
    private final String op;
    private final Object[] args;

    public Instruction(final C coefficient, final String op, final Object... args) {
        this.coefficient = coefficient;
        this.op = op;
        this.args = args;
    }

    public C coefficient() {
        return this.coefficient;
    }

    public String op() {
        return this.op;
    }

    public Object[] args() {
        return this.args;
    }

    @Override
    public String toString() {
        return "[" + this.coefficient + "]" + this.op + ":" + Arrays.toString(this.args).replace("[", "").replace("]", "");
    }
}
