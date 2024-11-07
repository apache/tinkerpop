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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.verification;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base {@link TraversalStrategy} class that is configurable to throw warnings or exceptions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractWarningVerificationStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy>
        implements TraversalStrategy.VerificationStrategy  {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractWarningVerificationStrategy.class);

    public static final String THROW_EXCEPTION = "throwException";
    public static final String LOG_WARNING = "logWarning";

    protected final boolean throwException;
    protected final boolean logWarning;

    AbstractWarningVerificationStrategy(final Builder builder) {
        this.throwException = builder.throwException;
        this.logWarning = builder.logWarning;
    }

    /**
     * Implementations should check the traversal and throw a standard {@link VerificationException} as it would if
     * it had directly implemented {@link #apply(Traversal.Admin)}. The message provided to the exception will be
     * used to log a warning and/or the same exception thrown if configured to do so.
     */
    abstract void verify(final Traversal.Admin<?, ?> traversal) throws VerificationException;

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        try {
            verify(traversal);
        } catch (VerificationException ve) {
            if (logWarning)
                LOGGER.warn(ve.getMessage());

            if (throwException)
                throw ve;
        }
    }

    @Override
    public Configuration getConfiguration() {
        final Map<String, Object> m = new LinkedHashMap<>(2);
        m.put(THROW_EXCEPTION, this.throwException);
        m.put(LOG_WARNING, this.logWarning);
        return new MapConfiguration(m);
    }

    public static abstract class Builder<T extends AbstractWarningVerificationStrategy, B extends Builder> {

        protected boolean throwException;
        protected boolean logWarning;

        Builder() { }

        public B throwException(final boolean throwException) {
            this.throwException = throwException;
            return (B) this;
        }

        public B throwException() {
            return this.throwException(true);
        }

        public B logWarning(final boolean logWarning) {
            this.logWarning = logWarning;
            return (B) this;
        }

        public B logWarning() {
            return this.logWarning(true);
        }

        public abstract T create();
    }
}
