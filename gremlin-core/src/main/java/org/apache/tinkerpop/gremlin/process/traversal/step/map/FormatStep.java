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

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
public final class FormatStep<S> extends MapStep<S, String> implements TraversalParent, Scoping, PathProcessor {

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("%\\{(.*?)\\}");

    private String format;
    private Set<String> variables;
    private TraversalRing<S, String> traversalRing = new TraversalRing<>();
    private Set<String> keepLabels;

    public FormatStep(final Traversal.Admin traversal, final String format) {
        super(traversal);
        this.format = format;
        this.variables = getVariables();
    }

    @Override
    protected Traverser.Admin<String> processNextStart() {
        final Map<String, Object> values = new HashMap<>();

        final Traverser.Admin traverser = this.starts.next();

        boolean productive = true;
        for (final String var : variables) {
            final Object current = traverser.get();
            // try to get property value
            if (current instanceof Element) {
                final Property prop = ((Element) current).property(var);
                if (prop != null && prop.isPresent()) {
                    values.put(var, prop.value());
                    continue;
                }
            }

            final TraversalProduct product =
                    TraversalUtil.produce((S) this.getNullableScopeValue(Pop.last, var, traverser), this.traversalRing.next());

            if (!product.isProductive() || product.get() == null) {
                productive = false;
                break;
            }

            values.put(var, product.get());
        }
        this.traversalRing.reset();

        return productive ?
                PathProcessor.processTraverserPathLabels(traverser.split(evaluate(values), this), this.keepLabels) :
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
    public void setKeepLabels(final Set<String> labels) {
        this.keepLabels = labels;
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }

    // private methods

    private Set<String> getVariables() {
        final Matcher matcher = VARIABLE_PATTERN.matcher(format);
        final Set<String> variables = new LinkedHashSet<>();
        while (matcher.find()) {
            final String varName = matcher.group(1);
            if (varName != null) {
                variables.add(matcher.group(1));
            }
        }
        return variables;
    }

    private String evaluate(final Map<String, Object> values) {
        int lastIndex = 0;
        final StringBuilder output = new StringBuilder();
        final Matcher matcher = VARIABLE_PATTERN.matcher(format);
        matcher.reset();

        while (matcher.find()) {
            final String varName = matcher.group(1);
            if (varName == null) continue;

            final Object value = values.get(varName);
            output.append(format, lastIndex, matcher.start()).append(value);

            lastIndex = matcher.end();
        }

        if (lastIndex < format.length()) {
            output.append(format, lastIndex, format.length());
        }
        return output.toString();
    }
}
