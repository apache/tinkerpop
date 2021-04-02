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

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV2d0;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.server.handler.AbstractAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.handler.OpExecutorHandler;
import org.apache.tinkerpop.gremlin.server.handler.OpSelectorHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
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
 * {@link RequestMessage} and outgoing data is converted from a  {@link ResponseMessage} to whatever expected format is
 * needed by the pipeline.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractChannelizer extends ChannelInitializer<SocketChannel> implements Channelizer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractChannelizer.class);
    protected static final List<Settings.SerializerSettings> DEFAULT_SERIALIZERS = Arrays.asList(
            new Settings.SerializerSettings(GraphSONMessageSerializerV2d0.class.getName(), Collections.emptyMap()),
            new Settings.SerializerSettings(GraphBinaryMessageSerializerV1.class.getName(), Collections.emptyMap()),
            new Settings.SerializerSettings(GraphBinaryMessageSerializerV1.class.getName(), new HashMap<String,Object>(){{
                put(GraphBinaryMessageSerializerV1.TOKEN_SERIALIZE_RESULT_TO_STRING, true);
            }})
    );

    protected Settings settings;
    protected GremlinExecutor gremlinExecutor;
    protected Optional<SslContext> sslContext;
    protected GraphManager graphManager;
    protected ExecutorService gremlinExecutorService;
    protected ScheduledExecutorService scheduledExecutorService;


    public static final String PIPELINE_AUTHENTICATOR = "authenticator";
    public static final String PIPELINE_AUTHORIZER = "authorizer";
    public static final String PIPELINE_REQUEST_HANDLER = "request-handler";
    public static final String PIPELINE_HTTP_RESPONSE_ENCODER = "http-response-encoder";
    public static final String PIPELINE_WEBSOCKET_SERVER_COMPRESSION = "web-socket-server-compression-handler";

    protected static final String PIPELINE_SSL = "ssl";
    protected static final String PIPELINE_OP_SELECTOR = "op-selector";
    protected static final String PIPELINE_OP_EXECUTOR = "op-executor";
    protected static final String PIPELINE_HTTP_REQUEST_DECODER = "http-request-decoder";

    protected static final String GREMLIN_ENDPOINT = "/gremlin";

    protected final Map<String, MessageSerializer<?>> serializers = new HashMap<>();

    private OpSelectorHandler opSelectorHandler;
    private OpExecutorHandler opExecutorHandler;

    protected Authenticator authenticator;
    protected Authorizer authorizer;

    /**
     * This method is called from within {@link #initChannel(SocketChannel)} just after the SSL handler is put in the pipeline.
     * Modify the pipeline as needed here.
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
    public void init(final ServerGremlinExecutor serverGremlinExecutor) {
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
        authorizer = createAuthorizer(settings.authorization);

        // these handlers don't share any state and can thus be initialized once per pipeline
        opSelectorHandler = new OpSelectorHandler(settings, graphManager, gremlinExecutor, scheduledExecutorService, this);
        opExecutorHandler = new OpExecutorHandler(settings, graphManager, gremlinExecutor, scheduledExecutorService);
    }

    @Override
    public void initChannel(final SocketChannel ch) throws Exception {
        final ChannelPipeline pipeline = ch.pipeline();

        sslContext.ifPresent(sslContext -> pipeline.addLast(PIPELINE_SSL, sslContext.newHandler(ch.alloc())));

        // checks for no activity on a channel and triggers an event that is consumed by the OpSelectorHandler
        // and either closes the connection or sends a ping to see if the client is still alive
        if (supportsIdleMonitor()) {
            final int idleConnectionTimeout = (int) (settings.idleConnectionTimeout / 1000);
            final int keepAliveInterval = (int) (settings.keepAliveInterval / 1000);
            pipeline.addLast(new IdleStateHandler(idleConnectionTimeout, keepAliveInterval, 0));
        }

        // the implementation provides the method by which Gremlin Server will process requests.  the end of the
        // pipeline must decode to an incoming RequestMessage instances and encode to a outgoing ResponseMessage
        // instance
        configure(pipeline);

        pipeline.addLast(PIPELINE_OP_SELECTOR, opSelectorHandler);
        pipeline.addLast(PIPELINE_OP_EXECUTOR, opExecutorHandler);

        finalize(pipeline);
    }

    protected AbstractAuthenticationHandler createAuthenticationHandler(final Settings settings) {
        try {
            final Class<?> clazz = Class.forName(settings.authentication.authenticationHandler);
            final Class[] constructorArgs = new Class[2];
            constructorArgs[0] = Authenticator.class;
            constructorArgs[1] = Settings.class;
            return (AbstractAuthenticationHandler) clazz.getDeclaredConstructor(constructorArgs).newInstance(authenticator, settings);
        } catch (Exception ex) {
            logger.warn(ex.getMessage());
            throw new IllegalStateException(String.format("Could not create/configure AuthenticationHandler %s", settings.authentication.authenticationHandler), ex);
        }
    }

    private Authenticator createAuthenticator(final Settings.AuthenticationSettings config) {
        final String authenticatorClass = config.authenticator;
        try {
            final Class<?> clazz = Class.forName(authenticatorClass);
            final Authenticator authenticator = (Authenticator) clazz.newInstance();
            authenticator.setup(config.config);
            return authenticator;
        } catch (Exception ex) {
            logger.warn(ex.getMessage());
            throw new IllegalStateException(String.format("Could not create/configure Authenticator %s", authenticator), ex);
        }
    }

    private Authorizer createAuthorizer(final Settings.AuthorizationSettings config) {
        final String authorizerClass = config.authorizer;
        if (null == authorizerClass) {
            return null;
        }
        try {
            final Class<?> clazz = Class.forName(authorizerClass);
            final Authorizer authorizer = (Authorizer) clazz.newInstance();
            authorizer.setup(config.config);
            return authorizer;
        } catch (Exception ex) {
            logger.warn(ex.getMessage());
            throw new IllegalStateException(String.format("Could not create/configure Authorizer %s", authorizer), ex);
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

                if (clazz.getAnnotation(Deprecated.class) != null)
                    logger.warn("The {} serialization class is deprecated.", config.className);

                final MessageSerializer<?> serializer = (MessageSerializer) clazz.newInstance();
                final Map<String, Graph> graphsDefinedAtStartup = new HashMap<>();
                for (String graphName : settings.graphs.keySet()) {
                    graphsDefinedAtStartup.put(graphName, graphManager.getGraph(graphName));
                }

                if (config.config != null)
                    serializer.configure(config.config, graphsDefinedAtStartup);

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
            final MessageSerializer<?> serializer = pair.getValue1();
            if (serializers.containsKey(mimeType))
                logger.info("{} already has {} configured - it will not be replaced by {}, change order of serialization configuration if this is not desired.",
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

    private SslContext createSSLContext(final Settings settings) {
        final Settings.SslSettings sslSettings = settings.ssl;

        if (sslSettings.getSslContext().isPresent()) {
            logger.info("Using the SslContext override");
            return sslSettings.getSslContext().get();
        }

        final SslProvider provider = SslProvider.JDK;

        final SslContextBuilder builder;

        // Build JSSE SSLContext
        try {
            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

            // Load private key and signed cert
            if (null != sslSettings.keyStore) {
                final String keyStoreType = null == sslSettings.keyStoreType ? KeyStore.getDefaultType() : sslSettings.keyStoreType;
                final KeyStore keystore = KeyStore.getInstance(keyStoreType);
                final char[] password = null == sslSettings.keyStorePassword ? null : sslSettings.keyStorePassword.toCharArray();
                try (final InputStream in = new FileInputStream(sslSettings.keyStore)) {
                    keystore.load(in, password);
                }
                kmf.init(keystore, password);
            } else {
                throw new IllegalStateException("keyStore must be configured when SSL is enabled.");
            }

            builder = SslContextBuilder.forServer(kmf);

            // Load custom truststore for client auth certs
            if (null != sslSettings.trustStore) {
                final String trustStoreType = null != sslSettings.trustStoreType ? sslSettings.trustStoreType
                            : sslSettings.keyStoreType != null ? sslSettings.keyStoreType : KeyStore.getDefaultType();

                final KeyStore truststore = KeyStore.getInstance(trustStoreType);
                final char[] password = null == sslSettings.trustStorePassword ? null : sslSettings.trustStorePassword.toCharArray();
                try (final InputStream in = new FileInputStream(sslSettings.trustStore)) {
                    truststore.load(in, password);
                }
                final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(truststore);
                builder.trustManager(tmf);
            }

        } catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException | CertificateException | IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException("There was an error enabling SSL.", e);
        }

        if (null != sslSettings.sslCipherSuites && !sslSettings.sslCipherSuites.isEmpty()) {
            builder.ciphers(sslSettings.sslCipherSuites);
        }

        if (null != sslSettings.sslEnabledProtocols && !sslSettings.sslEnabledProtocols.isEmpty()) {
            builder.protocols(sslSettings.sslEnabledProtocols.toArray(new String[] {}));
        }
        
        if (null != sslSettings.needClientAuth && ClientAuth.OPTIONAL == sslSettings.needClientAuth) {
            logger.warn("needClientAuth = OPTIONAL is not a secure configuration. Setting to REQUIRE.");
            sslSettings.needClientAuth = ClientAuth.REQUIRE;
        }

        builder.clientAuth(sslSettings.needClientAuth).sslProvider(provider);

        try {
            return builder.build();
        } catch (SSLException ssle) {
            logger.error(ssle.getMessage());
            throw new RuntimeException("There was an error enabling SSL.", ssle);
        }
    }
}
