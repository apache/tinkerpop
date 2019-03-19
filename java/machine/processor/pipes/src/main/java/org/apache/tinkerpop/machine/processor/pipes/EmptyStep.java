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
package org.apache.tinkerpop.machine.processor.pipes;

import org.apache.tinkerpop.machine.traverser.COPTraverser;
import org.apache.tinkerpop.util.FastNoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
final class EmptyStep<C, S, E> extends AbstractStep<C, S, E> {

    private static final EmptyStep INSTANCE = new EmptyStep<>();

    private EmptyStep() {
        super(null, null);
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public COPTraverser<C, E> next() {
        throw FastNoSuchElementException.instance();
    }

    static <C, S, E> EmptyStep<C, S, E> instance() {
        return INSTANCE;
    }

    @Override
    public void reset() {

    }
}
