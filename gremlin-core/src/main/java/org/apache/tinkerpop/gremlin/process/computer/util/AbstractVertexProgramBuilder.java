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
package org.apache.tinkerpop.gremlin.process.computer.util;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractVertexProgramBuilder<B extends VertexProgram.Builder> implements VertexProgram.Builder {

    protected final BaseConfiguration configuration = new BaseConfiguration();

    public AbstractVertexProgramBuilder(final Class<? extends VertexProgram> vertexProgramClass) {
        this.configuration.setProperty(VertexProgram.VERTEX_PROGRAM, vertexProgramClass.getName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public B configure(final Object... keyValues) {
        VertexProgramHelper.legalConfigurationKeyValueArray(keyValues);
        for (int i = 0; i < keyValues.length; i = i + 2) {
            this.configuration.setProperty((String) keyValues[i], keyValues[i + 1]);
        }
        return (B) this;
    }

    @Override
    public <P extends VertexProgram> P create(final Graph graph) {
        return (P) VertexProgram.createVertexProgram(graph, this.configuration);
    }
}
