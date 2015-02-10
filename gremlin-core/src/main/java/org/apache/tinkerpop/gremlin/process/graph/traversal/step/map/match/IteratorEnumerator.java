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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.match;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class IteratorEnumerator<T> implements Enumerator<T> {
    private final String name;
    private Iterator<T> iterator;
    private final List<T> memory = new ArrayList<>();

    public IteratorEnumerator(final String name,
                              final Iterator<T> iterator) {
        this.name = name;
        this.iterator = iterator;
    }

    public int size() {
        return memory.size();
    }

    public boolean visitSolution(final int index,
                                 final BiConsumer<String, T> visitor) {
        T value;

        if (index < memory.size()) {
            value = memory.get(index);
        } else do {
            if (null == iterator) {
                return false;
            } else if (!iterator.hasNext()) {
                // free up memory as soon as possible
                iterator = null;
                return false;
            }

            value = iterator.next();
            memory.add(value);
        } while (index >= memory.size());

        MatchStep.visit(name, value, visitor);

        return true;
    }
}
