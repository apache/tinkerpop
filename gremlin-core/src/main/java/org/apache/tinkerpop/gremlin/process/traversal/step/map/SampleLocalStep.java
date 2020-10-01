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

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Seedable;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SampleLocalStep<S> extends ScalarMapStep<S, S> implements Seedable {

    private final Random random = new Random();

    private final int amountToSample;

    public SampleLocalStep(final Traversal.Admin traversal, final int amountToSample) {
        super(traversal);
        this.amountToSample = amountToSample;
    }

    @Override
    public void resetSeed(final long seed) {
        this.random.setSeed(seed);
    }

    @Override
    protected S map(final Traverser.Admin<S> traverser) {
        final S start = traverser.get();
        if (start instanceof Map) {
            return mapMap((Map) start);
        } else if (start instanceof Collection) {
            return mapCollection((Collection) start);
        } else {
            return start;
        }
    }

    private S mapCollection(final Collection collection) {
        if (collection.size() <= this.amountToSample)
            return (S) collection;

        final List<S> original = new ArrayList<>(collection);
        final List<S> target = new ArrayList<>();
        while (target.size() < this.amountToSample) {
            target.add(original.remove(random.nextInt(original.size())));
        }
        return (S) target;
    }

    private S mapMap(final Map map) {
        if (map.size() <= this.amountToSample)
            return (S) map;

        final List<Map.Entry> original = new ArrayList<>(map.entrySet());
        final Map target = new LinkedHashMap<>(this.amountToSample);
        while (target.size() < this.amountToSample) {
            final Map.Entry entry = original.remove(random.nextInt(original.size()));
            target.put(entry.getKey(), entry.getValue());
        }
        return (S) target;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.amountToSample;
    }
}
