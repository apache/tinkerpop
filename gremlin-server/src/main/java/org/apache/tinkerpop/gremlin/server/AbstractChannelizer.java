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

import io.netty.channel.group.ChannelGroup;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.timeout.IdleStateHandler;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.exception.GenericSecurityException;
import nl.altindag.ssl.netty.util.NettySslUtils;
import nl.altindag.ssl.util.SSLFactoryUtils;
import org.apache.tinkerpop.gremlin.server.util.SSLStoreFilesModificationWatcher;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.server.handler.AbstractAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.structure.Graph;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV4;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * A base implementation for the {@code Channelizer} which does a basic configuration of the pipeline, one that
 * is generally common to virtually any Gremlin Server operation (i.e. where the server's purpose is to process
 * Gremlin scripts).
 * <p/>
 * Implementers need only worry about determining how incoming data is converted to a
 * {@link RequestMessage} and outgoing data is converted from a {@link ResponseMessage} to whatever expected format is
 * needed by the pipeline.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractChannelizer extends ChannelInitializer<SocketChannel> implements Channelizer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractChannelizer.class);
    protected static final List<Settings.SerializerSettings> DEFAULT_SERIALIZERS = Arrays.asList(
            new Settings.SerializerSettings(GraphSONMessageSerializerV4.class.getName(), Collections.emptyMap()),
            new Settings.SerializerSettings(GraphBinaryMessageSerializerV4.class.getName(), Collections.emptyMap())
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
    public static final String PIPELINE_HTTP_AGGREGATOR = "http-aggregator";
    public static final String PIPELINE_HTTP_USER_AGENT_HANDLER = "http-user-agent-handler";

    protected static final String PIPELINE_SSL = "ssl";
    protected static final String PIPELINE_HTTP_REQUEST_DECODER = "http-request-decoder";
    protected static final String GREMLIN_ENDPOINT = "/gremlin";

    protected final Map<String, MessageSerializer<?>> serializers = new HashMap<>();

    protected ChannelGroup channels;

    protected Authenticator authenticator;
    protected Authorizer authorizer;

    /**
     * This method is called from within {@link #initChannel(SocketChannel)} just after the SSL handler is put in the pipeline.
     * Modify the pipeline as needed here.
     */
    public abstract void configure(final ChannelPipeline pipeline);

    @Override
    public void init(final ServerGremlinExecutor serverGremlinExecutor) {
        settings = serverGremlinExecutor.getSettings();
        gremlinExecutor = serverGremlinExecutor.getGremlinExecutor();
        graphManager = serverGremlinExecutor.getGraphManager();
        gremlinExecutorService = serverGremlinExecutor.getGremlinExecutorService();
        scheduledExecutorService = serverGremlinExecutor.getScheduledExecutorService();
        channels = serverGremlinExecutor.getChannels();

        // instantiate and configure the serializers that gremlin server will use - could error out here
        // and fail the server startup
        configureSerializers();

        // configure ssl if present
        if (settings.optionalSsl().isPresent() && settings.ssl.enabled) {
            if (settings.ssl.getSslContext().isPresent()) {
                logger.info("Using the SslContext override");
                this.sslContext = settings.ssl.getSslContext();
            } else {
                final SSLFactory sslFactory = createSSLFactoryBuilder(settings).withSwappableTrustMaterial().withSwappableIdentityMaterial().build();
                this.sslContext = Optional.of(createSSLContext(sslFactory));

                if (settings.ssl.refreshInterval > 0) {
                    // At the scheduled refreshInterval, check whether the keyStore or trustStore has been modified. If they were,
                    // reload the SSLFactory which will reload the underlying KeyManager/TrustManager that Netty SSLHandler uses.
                    scheduledExecutorService.scheduleAtFixedRate(
                            new SSLStoreFilesModificationWatcher(settings.ssl.keyStore, settings.ssl.trustStore, () -> {
                                SSLFactory newSslFactory = createSSLFactoryBuilder(settings).build();
                                try {
                                    SSLFactoryUtils.reload(sslFactory, newSslFactory);
                                } catch (RuntimeException e) {
                                    logger.error("Failed to reload SSLFactory", e);
                                }
                            }),
                            settings.ssl.refreshInterval, settings.ssl.refreshInterval, TimeUnit.MILLISECONDS
                    );
                }
            }
        } else {
            this.sslContext = Optional.empty();
        }

        if (sslContext.isPresent()) logger.info("SSL enabled");

        authenticator = createAuthenticator(settings.authentication);
        authorizer = createAuthorizer(settings.authorization);
    }

    /**
     * It is best not to override this method as it sets up some core parts to the server. Prefer implementing the
     * {@link #configure(ChannelPipeline)} methods to alter the pipeline.
     */
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

        // track the newly created channel in the channel group
        channels.add(ch);
    }

    protected AbstractAuthenticationHandler createAuthenticationHandler(final Settings settings) {
        try {
            final Class<?> clazz = Class.forName(settings.authentication.authenticationHandler);
            AbstractAuthenticationHandler aah;
            // the three arg constructor is the new form as a handler may need the authorizer in some cases
            final Class<?>[] threeArgForm = new Class[]{Authenticator.class, Authorizer.class, Settings.class};
            final Constructor<?> threeArgConstructor = clazz.getDeclaredConstructor(threeArgForm);
            return (AbstractAuthenticationHandler) threeArgConstructor.newInstance(authenticator, authorizer, settings);
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

    private SSLFactory.Builder createSSLFactoryBuilder(final Settings settings) {
        final Settings.SslSettings sslSettings = settings.ssl;

        final SSLFactory.Builder builder = SSLFactory.builder();
        try {
            if (null != sslSettings.keyStore) {
                final String keyStoreType = null == sslSettings.keyStoreType ? KeyStore.getDefaultType() : sslSettings.keyStoreType;
                final char[] password = null == sslSettings.keyStorePassword ? null : sslSettings.keyStorePassword.toCharArray();
                try (final InputStream in = new FileInputStream(sslSettings.keyStore)) {
                    builder.withIdentityMaterial(in, password, keyStoreType);
                }
            } else {
                throw new IllegalStateException("keyStore must be configured when SSL is enabled.");
            }

            // Load custom truststore for client auth certs
            if (null != sslSettings.trustStore) {
                final String trustStoreType = null != sslSettings.trustStoreType ? sslSettings.trustStoreType
                    : sslSettings.keyStoreType != null ? sslSettings.keyStoreType : KeyStore.getDefaultType();
                final char[] password = null == sslSettings.trustStorePassword ? null : sslSettings.trustStorePassword.toCharArray();
                try (final InputStream in = new FileInputStream(sslSettings.trustStore)) {
                    builder.withTrustMaterial(in, password, trustStoreType);
                }
            }
        } catch (GenericSecurityException | IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException("There was an error enabling SSL.", e);
        }

        if (null != sslSettings.sslCipherSuites && !sslSettings.sslCipherSuites.isEmpty()) {
            builder.withCiphers(sslSettings.sslCipherSuites.toArray(new String[] {}));
        }

        if (null != sslSettings.sslEnabledProtocols && !sslSettings.sslEnabledProtocols.isEmpty()) {
            builder.withProtocols(sslSettings.sslEnabledProtocols.toArray(new String[] {}));
        }

        if (null != sslSettings.needClientAuth && ClientAuth.OPTIONAL == sslSettings.needClientAuth) {
            logger.warn("needClientAuth = OPTIONAL is not a secure configuration. Setting to REQUIRE.");
            sslSettings.needClientAuth = ClientAuth.REQUIRE;
        }

        if (sslSettings.needClientAuth == ClientAuth.REQUIRE) {
            builder.withNeedClientAuthentication(true);
        }

        return builder;
    }

    private static SslContext createSSLContext(final SSLFactory sslFactory) {
        try {
            final SslProvider provider = SslProvider.JDK;
            return NettySslUtils.forServer(sslFactory).sslProvider(provider).build();
        } catch (SSLException ssle) {
            logger.error(ssle.getMessage());
            throw new RuntimeException("There was an error enabling SSL.", ssle);
        }
    }
}
