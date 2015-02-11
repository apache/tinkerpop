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
package org.apache.tinkerpop.gremlin.structure.strategy;

import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * A pass through implementation of {@link GraphStrategy} where all strategy functions are simply executed as
 * they were originally implemented.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class IdentityStrategy implements GraphStrategy {
    private static final IdentityStrategy instance = new IdentityStrategy();

    private IdentityStrategy() {
    }

    @Override
    public String toString() {
        return StringFactory.graphStrategyString(this);
    }

    public static final IdentityStrategy instance() {
        return instance;
    }
}
