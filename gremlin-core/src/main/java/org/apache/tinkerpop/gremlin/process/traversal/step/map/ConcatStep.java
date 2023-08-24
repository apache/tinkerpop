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
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Reference implementation for String concatenation step, a mid-traversal step which concatenates one or more
 * String values together to the incoming String traverser. If the incoming traverser is a non-String value then an
 * {@code IllegalArgumentException} will be thrown.
 *
 * @author Yang Xia (http://github.com/xiazcy)
 */
public final class ConcatStep<S> extends ScalarMapStep<S, String> implements TraversalParent {

    private String stringArgsResult;

    private Traversal.Admin<S, String> concatTraversal;

    // flag used to propagate the null value through if all strings to be concatenated are null
    private boolean isNullTraverser = true;
    private boolean isNullTraversal = true;
    private boolean isNullString = true;

    public ConcatStep(final Traversal.Admin traversal, final String... concatStrings) {
        super(traversal);
        this.stringArgsResult = processStrings(concatStrings);
    }

    public ConcatStep(final Traversal.Admin traversal, final Traversal<S, String> concatTraversal) {
        super(traversal);
        this.concatTraversal = this.integrateChild(concatTraversal.asAdmin());
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

        if (null != this.concatTraversal) {
            if (this.concatTraversal.getStartStep() instanceof InjectStep) { // inject as start step is processed separately
                if (this.concatTraversal.hasNext()) {
                    final String result = this.concatTraversal.next();
                    if (null != result) {
                        this.isNullTraversal = false;
                        sb.append(result);
                    }
                }
            } else {
                sb.append(TraversalUtil.apply(traverser, this.concatTraversal));
            }
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
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.concatTraversal);
    }

    @Override
    public ConcatStep<S> clone() {
        final ConcatStep<S> clone = (ConcatStep<S>) super.clone();
        if (null != this.concatTraversal)
            clone.concatTraversal = this.concatTraversal.clone();
        return clone;
    }

    @Override
    public List<Traversal.Admin<S, String>> getLocalChildren() {
        return null == this.concatTraversal ? Collections.emptyList() : Collections.singletonList(this.concatTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.concatTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (null != this.concatTraversal)
            result = super.hashCode() ^ this.concatTraversal.hashCode();
        if (null != this.stringArgsResult)
            result = super.hashCode() ^ this.stringArgsResult.hashCode();
        return result;
    }
}
