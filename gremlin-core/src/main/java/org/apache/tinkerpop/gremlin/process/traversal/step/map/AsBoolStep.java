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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.Set;

/**
 * Reference implementation for boolean parsing step.
 */
public final class AsBoolStep<S> extends ScalarMapStep<S, Boolean> {

    public AsBoolStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Boolean map(final Traverser.Admin<S> traverser) {
        final Object object = traverser.get();
        if (object == null)  throw new IllegalArgumentException("Can't parse null as Boolean.");
        if (object instanceof Boolean) return (Boolean) object;
        if (object instanceof Number) {
            final double d = ((Number) object).doubleValue();
            if (Double.isNaN(d)) return false;
            return d != 0d;
        }
        if (object instanceof String) {
            final String str = ((String) object).trim();
            if (str.equalsIgnoreCase("true")) return true;
            if (str.equalsIgnoreCase("false")) return false;
            throw new IllegalArgumentException("Can't parse " + object + " as Boolean.");
        }
        throw new IllegalArgumentException("Can't parse " + object.getClass().getName() + " as Boolean.");
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

}
