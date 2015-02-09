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
package com.tinkerpop.gremlin.process.traversal.step;

import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Reducing<A, B> {

    public Reducer<A, B> getReducer();

    //////////

    public class Reducer<A, B> {
        private final Supplier<A> seedSupplier;
        private final BiFunction<A, B, A> biFunction;
        private final boolean onTraverser;

        public Reducer(final Supplier<A> seedSupplier, final BiFunction<A, B, A> biFunction, final boolean onTraverser) {
            this.seedSupplier = seedSupplier;
            this.biFunction = biFunction;
            this.onTraverser = onTraverser;
        }

        public boolean onTraverser() {
            return this.onTraverser;
        }

        public Supplier<A> getSeedSupplier() {
            return this.seedSupplier;
        }

        public BiFunction<A, B, A> getBiFunction() {
            return this.biFunction;
        }
    }

    //////////

    public interface FinalGet<A> {

        public A getFinal();

        public static <A> A tryFinalGet(final Object object) {
            return object instanceof FinalGet ? ((FinalGet<A>) object).getFinal() : (A) object;
        }
    }
}
