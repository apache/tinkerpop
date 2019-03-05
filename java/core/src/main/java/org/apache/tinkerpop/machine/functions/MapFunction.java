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
package org.apache.tinkerpop.machine.functions;

import org.apache.tinkerpop.machine.Traverser;
import org.apache.tinkerpop.machine.coefficients.Coefficients;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapFunction<C, A, B> implements GFunction<C, A,B> {

    private final C coefficient;
    private final Coefficients<C> coefficients;
    private final Function<A, B> mapFunction;

    public MapFunction(final Coefficients<C> coefficients, final C coefficient, final Function<A, B> mapFunction) {
        this.coefficients = coefficients;
        this.coefficient = coefficient;
        this.mapFunction = mapFunction;
    }

    @Override
    public Traverser<C, B> apply(final Traverser<C, A> traverser) {
        return traverser.split(this.coefficients.multiply(traverser.getCoefficient(), this.coefficient), this.mapFunction.apply(traverser.getObject()));
    }
}
