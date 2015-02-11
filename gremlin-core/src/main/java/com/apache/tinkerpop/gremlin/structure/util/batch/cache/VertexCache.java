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
package com.apache.tinkerpop.gremlin.structure.util.batch.cache;

import com.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Matthias Broecheler (http://www.matthiasb.com)
 */
public interface VertexCache {
    public Object getEntry(final Object externalId);

    public void set(final Vertex vertex, final Object externalId);

    public void setId(final Object vertexId, final Object externalId);

    public boolean contains(final Object externalId);

    public void newTransaction();
}
