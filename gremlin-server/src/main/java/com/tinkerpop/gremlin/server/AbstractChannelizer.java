package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.driver.MessageSerializer;
import com.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import com.tinkerpop.gremlin.server.handler.IteratorHandler;
import com.tinkerpop.gremlin.server.handler.OpExecutorHandler;
import com.tinkerpop.gremlin.server.handler.OpSelectorHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

/**
 * A base implementation for the {@code Channelizer} which does a basic configuration of the the pipeline, one that
 * is generally common to virtually any Gremlin Server operation (i.e. where the server's purpose is to process
 * Gremlin scripts).
 * <br/>
 * Implementers need only worry about determining how incoming data is converted to a
 * {@link com.tinkerpop.gremlin.driver.message.RequestMessage} and outgoing data is converted from a
 * {@link com.tinkerpop.gremlin.driver.message.ResponseMessage} to whatever expected format is needed by the pipeline.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractChannelizer extends ChannelInitializer<SocketChannel> implements Channelizer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractChannelizer.class);
    protected Settings settings;
    protected GremlinExecutor gremlinExecutor;
    protected Optional<SSLEngine> sslEngine;
    protected Graphs graphs;
    protected EventExecutorGroup gremlinGroup;
    protected ScheduledExecutorService scheduledExecutorService;

    protected static final String PIPELINE_SSL = "ssl";
    protected static final String PIPELINE_OP_SELECTOR = "op-selector";
    protected static final String PIPELINE_RESULT_ITERATOR_HANDLER = "result-iterator-handler";
    protected static final String PIPELINE_OP_EXECUTOR = "op-executor";

    protected final Map<String, MessageSerializer> serializers = new HashMap<>();

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
    public void init(final Settings settings, final GremlinExecutor gremlinExecutor,
                     final EventExecutorGroup gremlinGroup,
                     final Graphs graphs, final ScheduledExecutorService scheduledExecutorService) {
        this.settings = settings;
        this.gremlinExecutor = gremlinExecutor;
        this.graphs = graphs;
        this.gremlinGroup = gremlinGroup;
        this.scheduledExecutorService = scheduledExecutorService;

        // instantiate and configure the serializers that gremlin server will use - could error out here
        // and fail the server startup
        configureSerializers();

        this.sslEngine = settings.optionalSsl().isPresent() && settings.ssl.enabled ? Optional.ofNullable(createSslEngine()) : Optional.empty();
    }

    @Override
    public void initChannel(final SocketChannel ch) throws Exception {
        final ChannelPipeline pipeline = ch.pipeline();

        sslEngine.ifPresent(ssl -> pipeline.addLast(PIPELINE_SSL, new SslHandler(ssl)));

        // the implementation provides the method by which Gremlin Server will process requests.  the end of the
        // pipeline must decode to an incoming RequestMessage instances and encode to a outgoing ResponseMessage
        // instance
        configure(pipeline);

        pipeline.addLast(PIPELINE_OP_SELECTOR, new OpSelectorHandler(settings, graphs, gremlinExecutor, scheduledExecutorService));

        pipeline.addLast(gremlinGroup, PIPELINE_RESULT_ITERATOR_HANDLER, new IteratorHandler(settings));
        pipeline.addLast(gremlinGroup, PIPELINE_OP_EXECUTOR, new OpExecutorHandler(settings, graphs, gremlinExecutor, scheduledExecutorService));

        finalize(pipeline);
    }

    private void configureSerializers() {
        this.settings.serializers.stream().map(config -> {
            try {
                final Class clazz = Class.forName(config.className);
                if (!MessageSerializer.class.isAssignableFrom(clazz)) {
                    logger.warn("The {} serialization class does not implement {} - it will not be available.", config.className, MessageSerializer.class.getCanonicalName());
                    return Optional.<MessageSerializer>empty();
                }

                final MessageSerializer serializer = (MessageSerializer) clazz.newInstance();
                if (config.config != null)
                    serializer.configure(config.config);

                return Optional.ofNullable(serializer);
            } catch (ClassNotFoundException cnfe) {
                logger.warn("Could not find configured serializer class - {} - it will not be available", config.className);
                return Optional.<MessageSerializer>empty();
            } catch (Exception ex) {
                logger.warn("Could not instantiate configured serializer class - {} - it will not be available.", config.className);
                return Optional.<MessageSerializer>empty();
            }
        }).filter(Optional::isPresent).map(Optional::get).flatMap(serializer ->
                        Stream.of(serializer.mimeTypesSupported()).map(mimeType -> Pair.with(mimeType, serializer))
        ).forEach(pair -> {
            final String mimeType = pair.getValue0().toString();
            final MessageSerializer serializer = pair.getValue1();
            if (serializers.containsKey(mimeType))
                logger.warn("{} already has {} configured.  It will not be replaced by {}. Check configuration for serializer duplication or other issues.",
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

    private SSLEngine createSslEngine() {
        try {
            logger.info("SSL was enabled.  Initializing SSLEngine instance...");
            final SSLEngine engine = createSSLContext(settings).createSSLEngine();
            engine.setUseClientMode(false);
            logger.info("SSLEngine was properly configured and initialized.");
            return engine;
        } catch (Exception ex) {
            logger.warn("SSL could not be enabled.  Check the ssl section of the configuration file.", ex);
            return null;
        }
    }

    private SSLContext createSSLContext(final Settings settings) throws Exception {
        final Settings.SslSettings sslSettings = settings.ssl;

        TrustManager[] managers = null;
        if (sslSettings.trustStoreFile != null) {
            final KeyStore ts = KeyStore.getInstance(Optional.ofNullable(sslSettings.trustStoreFormat).orElseThrow(() -> new IllegalStateException("The trustStoreFormat is not set")));
            try (final InputStream trustStoreInputStream = new FileInputStream(Optional.ofNullable(sslSettings.trustStoreFile).orElseThrow(() -> new IllegalStateException("The trustStoreFile is not set")))) {
                ts.load(trustStoreInputStream, sslSettings.trustStorePassword.toCharArray());
            }

            final String trustStoreAlgorithm = Optional.ofNullable(sslSettings.trustStoreAlgorithm).orElse(TrustManagerFactory.getDefaultAlgorithm());
            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(trustStoreAlgorithm);
            tmf.init(ts);
            managers = tmf.getTrustManagers();
        }

        final KeyStore ks = KeyStore.getInstance(Optional.ofNullable(sslSettings.keyStoreFormat).orElseThrow(() -> new IllegalStateException("The keyStoreFormat is not set")));
        try (final InputStream keyStoreInputStream = new FileInputStream(Optional.ofNullable(sslSettings.keyStoreFile).orElseThrow(() -> new IllegalStateException("The keyStoreFile is not set")))) {
            ks.load(keyStoreInputStream, sslSettings.keyStorePassword.toCharArray());
        }

        final String keyManagerAlgorithm = Optional.ofNullable(sslSettings.keyManagerAlgorithm).orElse(KeyManagerFactory.getDefaultAlgorithm());
        final KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyManagerAlgorithm);
        kmf.init(ks, Optional.ofNullable(sslSettings.keyManagerPassword).orElseThrow(() -> new IllegalStateException("The keyManagerPassword is not set")).toCharArray());

        final SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(kmf.getKeyManagers(), managers, null);
        return serverContext;
    }
}