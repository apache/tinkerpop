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
package org.apache.tinkerpop.gremlin.server.op.standard;

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.op.AbstractEvalOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Simple {@link OpProcessor} implementation that handles {@code ScriptEngine} script evaluation outside the context
 * of a session.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StandardOpProcessor extends AbstractEvalOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(StandardOpProcessor.class);
    public static final String OP_PROCESSOR_NAME = "";

    public StandardOpProcessor() {
       super(true);
    }

    @Override
    public String getName() {
        return OP_PROCESSOR_NAME;
    }

    @Override
    public ThrowingConsumer<Context> getEvalOp() {
        return this::evalOp;
    }

    @Override
    public void close() throws Exception {
        // do nothing = no resources to release
    }

    private void evalOp(final Context context) throws OpProcessorException {
        final RequestMessage msg = context.getRequestMessage();

        logger.debug("Sessionless request {} for eval in thread {}", msg.getRequestId(), Thread.currentThread().getName());

        super.evalOpInternal(context, context::getGremlinExecutor, () -> {
            final Bindings bindings = new SimpleBindings();

            // rebind any global bindings to a different variable.
            if (msg.getArgs().containsKey(Tokens.ARGS_REBINDINGS)) {
                final Map<String, String> rebinds = (Map<String, String>) msg.getArgs().get(Tokens.ARGS_REBINDINGS);
                for (Map.Entry<String,String> kv : rebinds.entrySet()) {
                    boolean found = false;
                    final Map<String, Graph> graphs = context.getGraphManager().getGraphs();
                    if (graphs.containsKey(kv.getValue())) {
                        bindings.put(kv.getKey(), graphs.get(kv.getValue()));
                        found = true;
                    }

                    if (!found) {
                        final Map<String, TraversalSource> traversalSources = context.getGraphManager().getTraversalSources();
                        if (traversalSources.containsKey(kv.getValue())) {
                            bindings.put(kv.getKey(), traversalSources.get(kv.getValue()));
                            found = true;
                        }
                    }

                    if (!found) {
                        final String error = String.format("Could not rebind [%s] to [%s] as [%s] not in the Graph or TraversalSource global bindings",
                                kv.getKey(), kv.getValue(), kv.getValue());
                        throw new OpProcessorException(error, ResponseMessage.build(msg).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(error).create());
                    }
                }
            }

            // add any bindings to override any other supplied
            Optional.ofNullable((Map<String, Object>) msg.getArgs().get(Tokens.ARGS_BINDINGS)).ifPresent(bindings::putAll);
            return bindings;
        });
    }
}
