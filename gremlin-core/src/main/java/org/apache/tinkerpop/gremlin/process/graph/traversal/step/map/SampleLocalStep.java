/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.util.BulkSet;

import java.util.*;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class SampleLocalStep<S> extends MapStep<S, S> {

    private static final Random RANDOM = new Random();

    private final int amountToSample;

    public SampleLocalStep(final Traversal.Admin traversal, final int amountToSample) {
        super(traversal);
        this.amountToSample = amountToSample;
    }

    @Override
    protected S map(final Traverser.Admin<S> traverser) {
        final S start = traverser.get();
        if (start instanceof Map) {
            return mapMap((Map) start);
        } else if (start instanceof Collection) {
            return mapCollection((Collection) start);
        } else if (RANDOM.nextDouble() > 0.5d) {
            throw FastNoSuchElementException.instance();
        }
        return start;
    }

    private S mapCollection(final Collection collection) {
        final int size = collection.size();
        if (size <= this.amountToSample) {
            return (S) collection;
        }
        final double individualWeight = 1.0d / size;
        final Collection result = new BulkSet();
        double runningWeight = 0.0d;
        int runningAmountToSample = 0;
        while (runningAmountToSample < this.amountToSample) {
            for (final Object item : collection) {
                runningWeight = runningWeight + individualWeight;
                if (RANDOM.nextDouble() <= (runningWeight / size)) {
                    result.add(item);
                    if (++runningAmountToSample == this.amountToSample) {
                        break;
                    }
                }
            }
        }
        return (S) result;
    }

    private S mapMap(final Map map) {
        final int size = map.size();
        if (size <= this.amountToSample) {
            return (S) map;
        }
        final double individualWeight = 1.0d / size;
        final Map result = new HashMap(this.amountToSample);
        double runningWeight = 0.0d;
        while (result.size() < this.amountToSample) {
            for (final Object obj : map.entrySet()) {
                runningWeight = runningWeight + individualWeight;
                final Map.Entry entry = (Map.Entry) obj;
                if (RANDOM.nextDouble() <= (runningWeight / size)) {
                    result.put(entry.getKey(), entry.getValue());
                    if (result.size() == this.amountToSample) {
                        break;
                    }
                }
            }
        }
        return (S) result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
