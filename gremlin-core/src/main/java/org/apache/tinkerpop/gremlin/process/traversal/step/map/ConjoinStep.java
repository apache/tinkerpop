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
import org.apache.tinkerpop.gremlin.process.traversal.util.ListFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A map step that returns the result of joining every element in the traverser using the delimiter argument.
 */
public final class ConjoinStep<S> extends ScalarMapStep<S, String> implements ListFunction {
    private String delimiter;

    public ConjoinStep(final Traversal.Admin traversal, final String delimiter) {
        super(traversal);
        if (null == delimiter) { throw new IllegalArgumentException("Input delimiter to conjoin step can't be null."); }
        this.delimiter = delimiter;
    }

    @Override
    public String getStepName() { return "conjoin"; }

    @Override
    protected String map(Traverser.Admin<S> traverser) {
        final Collection elements = convertTraverserToCollection(traverser);
        if (elements.isEmpty()) { return ""; }

        final StringBuilder joinResult = new StringBuilder();

        for (Object elem : elements) {
            if (elem != null) {
                joinResult.append(String.valueOf(elem)).append(delimiter);
            }
        }

        if (joinResult.length() != 0) {
            joinResult.delete(joinResult.length() - delimiter.length(), joinResult.length());
            return joinResult.toString();
        } else {
            return null;
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() { return Collections.singleton(TraverserRequirement.OBJECT); }

    @Override
    public ConjoinStep<S> clone() {
        final ConjoinStep<S> clone = (ConjoinStep<S>) super.clone();
        clone.delimiter = this.delimiter;
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        return Objects.hash(result, delimiter);
    }
}
