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
package org.apache.tinkerpop.gremlin.machine;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Instruction<C> {

    private final C coefficient;
    private final String op;
    private final Object[] args;

    public Instruction(final C coefficient, final String op, final Object... args) {
        this.coefficient = coefficient;
        this.op = op;
        this.args = args;
    }

    public C getCoefficient() {
        return this.coefficient;
    }

    public String getOp() {
        return this.op;
    }

    public Object[] getArgs() {
        return this.args;
    }
}
