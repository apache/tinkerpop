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
package org.apache.tinkerpop.gremlin.server;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.AbstractGryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.handler.OpExecutorHandler;
import org.apache.tinkerpop.gremlin.server.handler.OpSelectorHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

/**
 * A base implementation for the {@code Channelizer} which does a basic configuration of the pipeline, one that
 * is generally common to virtually any Gremlin Server operation (i.e. where the server's purpose is to process
 * Gremlin scripts).
 * <p/>
 * Implementers need only worry about determining how incoming data is converted to a
 * {@link org.apache.tinkerpop.gremlin.driver.message.RequestMessage} and outgoing data is converted from a
 * {@link org.apache.tinkerpop.gremlin.driver.message.ResponseMessage} to whatever expected format is needed by the pipeline.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractChannelizer extends ChannelInitializer<SocketChannel> implements Channelizer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractChannelizer.class);
    protected static final List<Settings.SerializerSettings> DEFAULT_SERIALIZERS = Arrays.asList(
            new Settings.SerializerSettings(GryoMessageSerializerV1d0.class.getName(), Collections.emptyMap()),
            new Settings.SerializerSettings(GryoMessageSerializerV1d0.class.getName(), new HashMap<String,Object>(){{
                put(AbstractGryoMessageSerializerV1d0.TOKEN_SERIALIZE_RESULT_TO_STRING, true);
            }}),
            new Settings.SerializerSettings(GraphSONMessageSerializerV2d0.class.getName(), Collections.emptyMap())
    );

    protected Settings settings;
    protected GremlinExecutor gremlinExecutor;
    protected Optional<SslContext> sslContext;
    protected GraphManager graphManager;
    protected ExecutorService gremlinExecutorService;
    protected ScheduledExecutorService scheduledExecutorService;

    protected static final String PIPELINE_SSL = "ssl";
    protected static final String PIPELINE_OP_SELECTOR = "op-selector";
    protected static final String PIPELINE_OP_EXECUTOR = "op-executor";
    protected static final String PIPELINE_AUTHENTICATOR = "authenticator";

    protected final Map<String, MessageSerializer> serializers = new HashMap<>();

    private OpSelectorHandler opSelectorHandler;
    private OpExecutorHandler opExecutorHandler;

    protected Authenticator authenticator;

    /**
     * This method is called from within {@link #initChannel(io.netty.channel.socket.SocketChannel)} just after
     * the SSL handler is put in the pipeline.  Modify the pipeline as needed here.
     */
    public abstract void configure(final ChannelPipeline pipeline);

    /**
     * This method is called after the pipeline is completely configured.  It can be overridden to make any
     * final changes to the pipeline before it goes into use.
     */
    public void finalize(final ChannelPipeline pipeline) {
        // do nothing
    }

    @Override
    public void init(final ServerGremlinExecutor<EventLoopGroup> serverGremlinExecutor) {
        settings = serverGremlinExecutor.getSettings();
        gremlinExecutor = serverGremlinExecutor.getGremlinExecutor();
        graphManager = serverGremlinExecutor.getGraphManager();
        gremlinExecutorService = serverGremlinExecutor.getGremlinExecutorService();
        scheduledExecutorService = serverGremlinExecutor.getScheduledExecutorService();

        // instantiate and configure the serializers that gremlin server will use - could error out here
        // and fail the server startup
        configureSerializers();

        // configure ssl if present
        sslContext = settings.optionalSsl().isPresent() && settings.ssl.enabled ?
                Optional.ofNullable(createSSLContext(settings)) : Optional.empty();
        if (sslContext.isPresent()) logger.info("SSL enabled");

        authenticator = createAuthenticator(settings.authentication);

        // these handlers don't share any state and can thus be initialized once per pipeline
        opSelectorHandler = new OpSelectorHandler(settings, graphManager, gremlinExecutor, scheduledExecutorService);
        opExecutorHandler = new OpExecutorHandler(settings, graphManager, gremlinExecutor, scheduledExecutorService);
    }

    @Override
    public void initChannel(final SocketChannel ch) throws Exception {
        final ChannelPipeline pipeline = ch.pipeline();

        if (sslContext.isPresent()) pipeline.addLast(PIPELINE_SSL, sslContext.get().newHandler(ch.alloc()));

        // the implementation provides the method by which Gremlin Server will process requests.  the end of the
        // pipeline must decode to an incoming RequestMessage instances and encode to a outgoing ResponseMessage
        // instance
        configure(pipeline);

        pipeline.addLast(PIPELINE_OP_SELECTOR, opSelectorHandler);
        pipeline.addLast(PIPELINE_OP_EXECUTOR, opExecutorHandler);

        finalize(pipeline);
    }

    private Authenticator createAuthenticator(final Settings.AuthenticationSettings config) {
        try {
            final Class<?> clazz = Class.forName(config.className);
            final Authenticator authenticator = (Authenticator) clazz.newInstance();
            authenticator.setup(config.config);
            return authenticator;
        } catch (Exception ex) {
            logger.warn(ex.getMessage());
            throw new IllegalStateException(String.format("Could not create/configure Authenticator %s", config.className), ex);
        }
    }

    private void configureSerializers() {
        // grab some sensible defaults if no serializers are present in the config
        final List<Settings.SerializerSettings> serializerSettings =
                (null == this.settings.serializers || this.settings.serializers.isEmpty()) ? DEFAULT_SERIALIZERS : settings.serializers;

        serializerSettings.stream().map(config -> {
            try {
                final Class clazz = Class.forName(config.className);
                if (!MessageSerializer.class.isAssignableFrom(clazz)) {
                    logger.warn("The {} serialization class does not implement {} - it will not be available.", config.className, MessageSerializer.class.getCanonicalName());
                    return Optional.<MessageSerializer>empty();
                }

                final MessageSerializer serializer = (MessageSerializer) clazz.newInstance();
                if (config.config != null)
                    serializer.configure(config.config, graphManager.getGraphs());

                return Optional.ofNullable(serializer);
            } catch (ClassNotFoundException cnfe) {
                logger.warn("Could not find configured serializer class - {} - it will not be available", config.className);
                return Optional.<MessageSerializer>empty();
            } catch (Exception ex) {
                logger.warn("Could not instantiate configured serializer class - {} - it will not be available. {}", config.className, ex.getMessage());
                return Optional.<MessageSerializer>empty();
            }
        }).filter(Optional::isPresent).map(Optional::get).flatMap(serializer ->
                        Stream.of(serializer.mimeTypesSupported()).map(mimeType -> Pair.with(mimeType, serializer))
        ).forEach(pair -> {
            final String mimeType = pair.getValue0();
            final MessageSerializer serializer = pair.getValue1();
            if (serializers.containsKey(mimeType))
                logger.info("{} already has {} configured - it will not be replaced by {}.",
                        mimeType, serializers.get(mimeType).getClass().getName(), serializer.getClass().getName());
            else {
                logger.info("Configured {} with {}", mimeType, pair.getValue1().getClass().getName());
                serializers.put(mimeType, serializer);
            }
        });

        if (serializers.size() == 0) {
            logger.error("No serializers were successfully configured - server will not start.");
            throw new RuntimeException("Serialization configuration error.");
        }
    }

    private SslContext createSSLContext(final Settings settings)  {
        final Settings.SslSettings sslSettings = settings.ssl;

        if (sslSettings.getSslContext().isPresent()) {
            logger.info("Using the SslContext override");
            return sslSettings.getSslContext().get();
        }

        final SslProvider provider = SslProvider.JDK;

        final SslContextBuilder builder;

        // if the config doesn't contain a cert or key then use a self signed cert - not suitable for production
        if (null == sslSettings.keyCertChainFile || null == sslSettings.keyFile) {
            try {
                logger.warn("Enabling SSL with self-signed certificate (NOT SUITABLE FOR PRODUCTION)");
                final SelfSignedCertificate ssc = new SelfSignedCertificate();
                builder = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
            } catch (CertificateException ce) {
                logger.error("There was an error creating the self-signed certificate for SSL - SSL is not enabled", ce);
                return null;
            }
        } else {
            final File keyCertChainFile = new File(sslSettings.keyCertChainFile);
            final File keyFile = new File(sslSettings.keyFile);
            final File trustCertChainFile = null == sslSettings.trustCertChainFile ? null : new File(sslSettings.trustCertChainFile);

            // note that keyPassword may be null here if the keyFile is not password-protected. passing null to
            // trustManager is also ok (default will be used)
            builder = SslContextBuilder.forServer(keyCertChainFile, keyFile, sslSettings.keyPassword)
                    .trustManager(trustCertChainFile);
        }
        
        

        builder.clientAuth(sslSettings.needClientAuth).sslProvider(provider);

        try {
            return builder.build();
        } catch (SSLException ssle) {
            logger.error("There was an error enabling SSL", ssle);
            return null;
        }
    }
}