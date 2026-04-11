/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A placeholder step that represents a declarative pattern-matching query (e.g. GQL {@code MATCH}).
 * This step is not directly executable; a graph provider must register an execution strategy
 * that replaces it with a concrete implementation.
 *
 * <p>The query language can be configured via the {@code with("queryLanguage", value)} modulator.
 * The default query language is {@code "gql"}.</p>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @since 4.0.0
 */
public final class DeclarativeMatchStep<S> extends AbstractStep<S, Optional> implements Configuring {

    /**
     * The default query language used when none is explicitly set.
     */
    public static final String DEFAULT_QUERY_LANGUAGE = "gql";

    protected Parameters parameters = new Parameters();

    private final String gqlQuery;
    private final Map<String, Object> params;
    private String queryLanguage;

    /**
     * Constructs a {@code DeclarativeMatchStep} with the given query and optional parameters,
     * defaulting the query language to {@link #DEFAULT_QUERY_LANGUAGE}.
     *
     * @param traversal the parent traversal
     * @param gqlQuery  the declarative query string
     * @param params    optional query parameters (may be {@code null})
     */
    public DeclarativeMatchStep(final Traversal.Admin traversal, final String gqlQuery,
                                final Map<String, Object> params) {
        this(traversal, gqlQuery, params, DEFAULT_QUERY_LANGUAGE);
    }

    /**
     * Constructs a {@code DeclarativeMatchStep} with the given query, optional parameters,
     * and an explicit query language.
     *
     * @param traversal     the parent traversal
     * @param gqlQuery      the declarative query string
     * @param params        optional query parameters (may be {@code null})
     * @param queryLanguage the query language identifier (e.g. {@code "gql"})
     */
    public DeclarativeMatchStep(final Traversal.Admin traversal, final String gqlQuery,
                                final Map<String, Object> params, final String queryLanguage) {
        super(traversal);
        this.gqlQuery = gqlQuery;
        this.params = params;
        this.queryLanguage = queryLanguage;
    }

    /**
     * Always throws {@link UnsupportedOperationException}. A provider strategy must replace this
     * step before the traversal is executed.
     */
    @Override
    protected Traverser.Admin<Optional> processNextStart() {
        throw new UnsupportedOperationException(
                "No GQL execution engine registered for this graph — a provider strategy must replace this step");
    }

    /**
     * Accepts configuration via the {@code with()} step modulator. Recognises the
     * {@link WithOptions#queryLanguage} key to override the query language; all other
     * key/value pairs are stored in the step's {@link Parameters}.
     */
    @Override
    public void configure(final Object... keyValues) {
        if (keyValues.length == 2 && WithOptions.queryLanguage.equals(keyValues[0])) {
            this.queryLanguage = (String) keyValues[1];
        } else {
            this.parameters.set(null, keyValues);
        }
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.emptySet();
    }

    /**
     * Returns the declarative query string passed to this step.
     */
    public String getGqlQuery() {
        return this.gqlQuery;
    }

    /**
     * Returns the query parameters, or {@code null} if none were provided.
     */
    public Map<String, Object> getParams() {
        return this.params;
    }

    /**
     * Returns the query language identifier in use for this step.
     */
    public String getQueryLanguage() {
        return this.queryLanguage;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.gqlQuery, this.queryLanguage);
    }
}
