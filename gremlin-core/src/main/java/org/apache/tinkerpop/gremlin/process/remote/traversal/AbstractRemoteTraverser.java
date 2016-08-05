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
package org.apache.tinkerpop.gremlin.process.remote.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.AbstractTraverser;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractRemoteTraverser<T> extends AbstractTraverser<T> implements RemoteTraverser<T> {
    private final long bulk;

    public AbstractRemoteTraverser(final T t, final long bulk) {
        super(t);
        this.bulk = bulk;
    }

    @Override
    public long bulk() {
        return this.bulk;
    }
}
