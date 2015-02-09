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
package com.tinkerpop.gremlin.process.graph.traversal.step.map.match;

import java.util.Iterator;
import java.util.function.BiConsumer;

/**
 * An enumerator of at most one element
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SimpleEnumerator<T> implements Enumerator<T> {

    private final String name;
    private Iterator<T> iterator;
    private T element;

    public SimpleEnumerator(final String name,
                            final Iterator<T> iterator) {
        this.name = name;
        this.iterator = iterator;
    }

    @Override
    public int size() {
        return null == element ? 0 : 1;
    }

    @Override
    public boolean visitSolution(int index, BiConsumer<String, T> visitor) {
        if (0 != index) {
            return false;
        }

        if (null != iterator) {
            if (iterator.hasNext()) {
                element = iterator.next();
            }
            iterator = null;
        }

        if (null != element) {
            MatchStep.visit(name, element, visitor);
            return true;
        }

        return false;
    }
}
