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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reference implementation for String concatenation step, a mid-traversal step which concatenates one or more
 * String values together to the incoming String traverser. If the incoming traverser is a non-String value then an
 * {@code IllegalArgumentException} will be thrown.
 *
 * @author Yang Xia (http://github.com/xiazcy)
 */
public final class ConcatStep<S> extends ScalarMapStep<S, String> implements TraversalParent {

    private List<Traversal.Admin<S, String>> concatTraversals;
    private List<String> concatStrings;
    private String stringArgsResult;

    // flag used to propagate the null value through if all strings to be concatenated are null
    private boolean isNullTraverser = true;
    private boolean isNullTraversal = true;
    private boolean isNullString = true;

    public ConcatStep(final Traversal.Admin traversal, final String... concatStrings) {
        super(traversal);
        this.concatStrings = null == concatStrings? null : Collections.unmodifiableList(Arrays.asList(concatStrings));
        this.stringArgsResult = processStrings(concatStrings);
    }

    public ConcatStep(final Traversal.Admin traversal, final Traversal<?, String> concatTraversal, final Traversal<?, String>... otherConcatTraversals) {
        super(traversal);
        this.concatTraversals = null == concatTraversal? new ArrayList<>() : new ArrayList<>(Collections.singletonList((Traversal.Admin<S, String>) concatTraversal.asAdmin()));
        if (null != otherConcatTraversals) {
            this.concatTraversals.addAll(Stream.of(otherConcatTraversals).filter(Objects::nonNull).map(ct -> (Traversal.Admin<S, String>) ct.asAdmin()).collect(Collectors.toList()));
        }
        this.concatTraversals.forEach(this::integrateChild);
    }

    @Override
    protected String map(final Traverser.Admin<S> traverser) {
        // throws when incoming traverser isn't a string
        if (null != traverser.get() && !(traverser.get() instanceof String)) {
            throw new IllegalArgumentException(
                    String.format("String concat() can only take string as argument, encountered %s", traverser.get().getClass()));
        }

        final StringBuilder sb = new StringBuilder();
        // all null values are skipped during appending, as StringBuilder will otherwise append "null" as a string
        if (null != traverser.get()) {
            // we know there is one non-null part in the string we concat
            this.isNullTraverser = false;
            sb.append(traverser.get());
        }

        if (null != this.concatTraversals) {
            this.concatTraversals.forEach(ct -> {
                final String result = TraversalUtil.apply(traverser, ct);
                if (null != result) {
                    this.isNullTraversal = false;
                    sb.append(result);
                }
            });
        }

        if (!this.isNullString) {
            sb.append(this.stringArgsResult);
        }

        if (this.isNullTraverser && this.isNullTraversal && this.isNullString) {
            return null;
        } else {
            this.isNullTraverser = true;
            this.isNullTraversal = true;
            return sb.toString();
        }
    }

    private String processStrings(final String[] concatStrings) {
        final StringBuilder sb = new StringBuilder();
        if (null != concatStrings && concatStrings.length != 0) {
            for (final String s : concatStrings) {
                if (null != s) {
                    // we know there is one non-null part in the string we concat
                    this.isNullString = false;
                    sb.append(s);
                }
            }
        }
        return this.isNullString? null : sb.toString();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (null != this.concatTraversals)
            for (final Traversal.Admin<S, String> traversal : this.concatTraversals) {
                this.integrateChild(traversal);
            }
    }

    @Override
    public ConcatStep<S> clone() {
        final ConcatStep<S> clone = (ConcatStep<S>) super.clone();
        if (null != this.concatTraversals) {
            clone.concatTraversals = new ArrayList<>();
            for (final Traversal.Admin<S, String> concatTraversals : this.concatTraversals) {
                clone.concatTraversals.add(concatTraversals.clone());
            }
        }
        if (null != this.concatStrings) {
            clone.concatStrings = new ArrayList<>();
            clone.concatStrings.addAll(this.concatStrings);
        }
        return clone;
    }

    @Override
    public List<Traversal.Admin<S, String>> getLocalChildren() {
        return null == this.concatTraversals ? Collections.emptyList() : this.concatTraversals;
    }

    @Override
    public String toString() {
        if (null != this.concatTraversals)
            return StringFactory.stepString(this, this.concatTraversals);
        return StringFactory.stepString(this, this.concatStrings);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (null != this.concatTraversals) {
            for (final Traversal t : this.concatTraversals) {
                result = 31 * result + (null != t ? t.hashCode() : 0);
            }
        }
        if (null != this.concatStrings) {
            for (final String s : this.concatStrings) {
                result = 31 * result + (null != s ? s.hashCode() : 0);
            }
        }
        return result;
    }
}
