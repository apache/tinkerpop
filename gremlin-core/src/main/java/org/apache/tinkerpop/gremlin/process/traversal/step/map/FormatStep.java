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

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reference implementation for String format step, a mid-traversal step which will handle result formatting
 * to string values. If the incoming traverser is a non-String value then an {@code IllegalArgumentException}
 * will be thrown.
 *
 * @author Valentyn Kahamlyk
 */
public final class FormatStep<S> extends MapStep<S, String> implements ByModulating, TraversalParent, Scoping, PathProcessor {

    private static final String FROM_BY = "_";
    /* Negative Lookbehind (?<!%)
     * Assert that the Regex does not match %
     * 1st Capturing Group (.*?).
     * . matches any character (except for line terminators)
     * *? matches the previous token between zero and unlimited times, as few times as possible, expanding as needed (lazy)
     */
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("(?<!%)%\\{(.*?)\\}");

    private String format;
    private Set<String> variables;
    private TraversalRing<S, String> traversalRing = new TraversalRing<>();
    private Set<String> keepLabels;

    public FormatStep(final Traversal.Admin traversal, final String format) {
        super(traversal);

        if (null == format) {
            throw new IllegalArgumentException("Format string for Format step can't be null.");
        }
        this.format = format;
        this.variables = getVariables();
    }

    @Override
    protected Traverser.Admin<String> processNextStart() {
        final Traverser.Admin traverser = this.starts.next();

        boolean productive = true;
        int lastIndex = 0;
        final StringBuilder output = new StringBuilder();
        final Matcher matcher = VARIABLE_PATTERN.matcher(format);
        final Object current = traverser.get();

        while (matcher.find()) {
            final String varName = matcher.group(1);
            if (varName == null) continue;

            if (!varName.equals(FROM_BY) && current instanceof Element) {
                final Property prop = ((Element) current).property(varName);
                if (prop != null && prop.isPresent()) {
                    output.append(format, lastIndex, matcher.start()).append(prop.value());
                    lastIndex = matcher.end();
                    continue;
                }
            }

            final TraversalProduct product = varName.equals(FROM_BY) ?
                    TraversalUtil.produce(traverser, this.traversalRing.next()) :
                    TraversalUtil.produce((S) this.getNullableScopeValue(Pop.last, varName, traverser), null);

            if (!product.isProductive() || product.get() == null) {
                productive = false;
                break;
            }

            output.append(format, lastIndex, matcher.start()).append(product.get());
            lastIndex = matcher.end();
        }

        if (lastIndex < format.length()) {
            output.append(format, lastIndex, format.length());
        }

        this.traversalRing.reset();

        return productive ?
                PathProcessor.processTraverserPathLabels(traverser.split(output.toString(), this), this.keepLabels) :
                EmptyTraverser.instance();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.format, this.traversalRing);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        return Objects.hash(result, format, traversalRing);
    }

    @Override
    public List<Traversal.Admin<S, String>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public FormatStep<S> clone() {
        final FormatStep<S> clone = (FormatStep<S>) super.clone();
        clone.format = this.format;
        clone.variables = this.variables;
        clone.traversalRing = this.traversalRing;
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.traversalRing.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public Set<String> getScopeKeys() {
        return variables;
    }

    @Override
    public Set<ScopingInfo> getScopingInfo() {
        final Set<String> labels = this.getScopeKeys();
        final Set<ScopingInfo> scopingInfoSet = new HashSet<>();
        for (String label : labels) {
            final ScopingInfo scopingInfo = new ScopingInfo();
            scopingInfo.label = label;
            scopingInfo.pop = Pop.last;
            scopingInfoSet.add(scopingInfo);
        }
        return scopingInfoSet;
    }

    @Override
    public void setKeepLabels(final Set<String> labels) {
        this.keepLabels = labels;
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> selectTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(selectTraversal));
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        this.traversalRing.replaceTraversal(
                (Traversal.Admin<S, String>) oldTraversal,
                this.integrateChild(newTraversal));
    }

    // private methods

    private Set<String> getVariables() {
        final Matcher matcher = VARIABLE_PATTERN.matcher(format);
        final Set<String> variables = new LinkedHashSet<>();
        while (matcher.find()) {
            final String varName = matcher.group(1);
            if (varName != null && !varName.equals(FROM_BY)) {
                variables.add(varName);
            }
        }
        return variables;
    }
}
