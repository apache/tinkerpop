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
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Simple {@link OpProcessor} implementation that handles {@code ScriptEngine} script evaluation outside the context
 * of a session.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StandardOpProcessor extends AbstractEvalOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(StandardOpProcessor.class);
    public static final String OP_PROCESSOR_NAME = "";

    protected final Function<Context, BindingSupplier> bindingMaker;

    public StandardOpProcessor() {
        super(true);
        bindingMaker = getBindingMaker();
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
    public Optional<ThrowingConsumer<Context>> selectOther(final RequestMessage requestMessage)  throws OpProcessorException {
        return Optional.empty();
    }

    @Override
    public void close() throws Exception {
        // do nothing = no resources to release
    }

    private void evalOp(final Context context) throws OpProcessorException {
        if (logger.isDebugEnabled()) {
            final RequestMessage msg = context.getRequestMessage();
            logger.debug("Sessionless request {} for eval in thread {}", msg.getRequestId(), Thread.currentThread().getName());
        }

        evalOpInternal(context, context::getGremlinExecutor, bindingMaker.apply(context));
    }

    /**
     * A useful method for those extending this class, where the means for binding construction can be supplied
     * to this class.  This function is used in {@link #evalOp(Context)} to create the final argument to
     * {@link super#evalOpInternal(Context, Supplier, BindingSupplier)}. In this way an extending class can use
     * the default {@link BindingSupplier} which carries a lot of re-usable functionality or provide a new one to
     * override the existing approach.
     */
    protected Function<Context, BindingSupplier> getBindingMaker() {
        return context -> () -> {
            final RequestMessage msg = context.getRequestMessage();
            final Bindings bindings = new SimpleBindings();

            // don't allow both rebindings and aliases parameters as they are the same thing. aliases were introduced
            // as of 3.1.0 as a replacement for rebindings. this check can be removed when rebindings are completely
            // removed from the protocol
            final boolean hasRebindings = msg.getArgs().containsKey(Tokens.ARGS_REBINDINGS);
            final boolean hasAliases = msg.getArgs().containsKey(Tokens.ARGS_ALIASES);
            if (hasRebindings && hasAliases) {
                final String error = "Prefer use of the 'aliases' parameter over 'rebindings' and do not use both";
                throw new OpProcessorException(error, ResponseMessage.build(msg)
                        .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(error).create());
            }

            final String rebindingOrAliasParameter = hasRebindings ? Tokens.ARGS_REBINDINGS : Tokens.ARGS_ALIASES;

            // alias any global bindings to a different variable.
            if (msg.getArgs().containsKey(rebindingOrAliasParameter)) {
                final Map<String, String> aliases = (Map<String, String>) msg.getArgs().get(rebindingOrAliasParameter);
                for (Map.Entry<String,String> aliasKv : aliases.entrySet()) {
                    boolean found = false;

                    // first check if the alias refers to a Graph instance
                    final Map<String, Graph> graphs = context.getGraphManager().getGraphs();
                    if (graphs.containsKey(aliasKv.getValue())) {
                        bindings.put(aliasKv.getKey(), graphs.get(aliasKv.getValue()));
                        found = true;
                    }

                    // if the alias wasn't found as a Graph then perhaps it is a TraversalSource - it needs to be
                    // something
                    if (!found) {
                        final Map<String, TraversalSource> traversalSources = context.getGraphManager().getTraversalSources();
                        if (traversalSources.containsKey(aliasKv.getValue())) {
                            bindings.put(aliasKv.getKey(), traversalSources.get(aliasKv.getValue()));
                            found = true;
                        }
                    }

                    // this validation is important to calls to GraphManager.commit() and rollback() as they both
                    // expect that the aliases supplied are valid
                    if (!found) {
                        final String error = String.format("Could not alias [%s] to [%s] as [%s] not in the Graph or TraversalSource global bindings",
                                aliasKv.getKey(), aliasKv.getValue(), aliasKv.getValue());
                        throw new OpProcessorException(error, ResponseMessage.build(msg)
                                .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(error).create());
                    }
                }
            } else {
                // there's no bindings so determine if that's ok with Gremlin Server
                if (context.getSettings().strictTransactionManagement) {
                    final String error = "Gremlin Server is configured with strictTransactionManagement as 'true' - the 'aliases' arguments must be provided";
                    throw new OpProcessorException(error, ResponseMessage.build(msg)
                            .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(error).create());
                }
            }

            // add any bindings to override any other supplied
            Optional.ofNullable((Map<String, Object>) msg.getArgs().get(Tokens.ARGS_BINDINGS)).ifPresent(bindings::putAll);
            return bindings;
        };
    }
}
