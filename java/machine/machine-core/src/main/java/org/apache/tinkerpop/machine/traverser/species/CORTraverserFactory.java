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
package org.apache.tinkerpop.machine.traverser.species;

import org.apache.tinkerpop.machine.function.CFunction;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.TraverserFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CORTraverserFactory<C> implements TraverserFactory<C> {

    private static final CORTraverserFactory INSTANCE = new CORTraverserFactory();

    private CORTraverserFactory() {
        // static instance
    }

    @Override
    public <S> Traverser<C, S> create(final CFunction<C> function, final S object) {
        return new CORTraverser<>(function.coefficient(), object);
    }

    public static <C> CORTraverserFactory<C> instance() {
        return INSTANCE;
    }
}
