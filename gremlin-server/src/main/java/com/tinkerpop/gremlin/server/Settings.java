package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Server settings as configured by a YAML file.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Settings {

    /**
     * Host to bind the server to. Defaults to localhost.
     */
    public String host = "localhost";

    /**
     * Port to bind the server to.  Defaults to 8182.
     */
    public int port = 8182;

    /**
     * Size of the worker thread pool.   Defaults to 1 and should generally not exceed 2 * number of cores. A
     * worker thread performs non-blocking read and write for one or more Channels in a non-blocking mode.
     */
    public int threadPoolWorker = 1;

    /**
     * Size of the Gremlin thread pool. This pool handles Gremlin script execution and other related "long-run"
     * processing.  This setting should be sufficiently large to ensure that requests processed by the non-blocking
     * worker threads are processed with limited queuing.  Defaults to 8.
     */
    public int gremlinPool = 8;

    /**
     * Size of the boss thread pool.  Defaults to 1 and should likely stay at 1.  The bossy thread accepts incoming
     * connections on a port until it is unbound. Once a connection is accepted successfully, the boss thread
     * passes processing to the worker threads.
     */
    public int threadPoolBoss = 1;

    /**
     * Time in milliseconds to wait for a script to complete execution.  Defaults to 30000.
     */
    public long scriptEvaluationTimeout = 30000l;

    /**
     * Time in milliseconds to wait while an evaluated script serializes its results. This value represents the
     * total serialization time for the request.  Defaults to 30000.
     */
    public long serializedResponseTimeout = 30000l;

    /**
     * Number of items in a particular resultset to iterate and serialize prior to pushing the data down the wire
     * to the client.
     */
    public int resultIterationBatchSize = 64;

    /**
     * The maximum length of the initial line (e.g. {@code "GET / HTTP/1.0"}). Can reduce excessive sideEffects consumption.
     * This setting ties to the Netty {@code HttpRequestDecoder}
     */
    public int maxInitialLineLength = 4096;

    /**
     * The maximum length of all headers.  If the sum of the length of each header exceeds this value. Can reduce
     * excessive sideEffects consumption. This setting ties to the Netty {@code HttpRequestDecoder}
     */
    public int maxHeaderSize = 8192;

    /**
     * The maximum length of the content or each chunk.  If the content length exceeds this value, the transfer
     * encoding of the decoded request will be converted to 'chunked' and the content will be split into multiple
     * {@code HttpContent}s.  If the transfer encoding of the HTTP request is 'chunked' already, each chunk will be
     * split into smaller chunks if the length of the chunk exceeds this value. Can reduce excessive sideEffects
     * consumption. This setting ties to the Netty {@code HttpRequestDecoder}
     */
    public int maxChunkSize = 8192;

    /**
     * The maximum length of the aggregated content for a message.  Works in concert with {@link #maxChunkSize} where
     * chunked requests are accumulated back into a single message.  A request exceeding this size will
     * return a 413 - Request Entity Too Large status code.  A response exceeding this size will raise an internal
     * exception.
     */
    public int maxContentLength = 65536;

    /**
     * Maximum number of request components that can be aggregated for a message.
     */
    public int maxAccumulationBufferComponents = 1024;

    /**
     * If the number of bytes in the network send buffer exceeds this value then the channel is no longer writeable,
     * accepting no additional writes until buffer is drained and the low water mark is met.
     */
    public int writeBufferHighWaterMark = 1024 * 64;

    /**
     * Once the number of bytes queued in the network send buffer exceeds the high water mark, the channel will not
     * become writeable again until the buffer is drained and it drops below this value.
     */
    public int writeBufferLowWaterMark = 1024 * 32;

    /**
     * The full class name of the {@link Channelizer} to use in
     * Gremlin Server.
     */
    public String channelizer = WebSocketChannelizer.class.getName();

    /**
     * Configured metrics for Gremlin Server.
     */
    public ServerMetrics metrics = null;

    /**
     * {@link Map} of {@link com.tinkerpop.gremlin.structure.Graph} objects keyed by their binding name.
     */
    public Map<String, String> graphs;

    /**
     * {@link Map} of settings for {@code ScriptEngine} setting objects keyed by the name of the {@code ScriptEngine}
     * implementation.
     */
    public Map<String, ScriptEngineSettings> scriptEngines;

    /**
     * List of {@link com.tinkerpop.gremlin.driver.MessageSerializer} to configure.
     */
    public List<SerializerSettings> serializers;

    /**
     * Configures settings for SSL.
     */
    public SslSettings ssl = null;

    /**
     * The list of plugins to enable for the server.  Plugins may be available on the classpath, but with this
     * configuration it is possible to explicitly include or omit them.
     */
    public List<String> plugins = new ArrayList<>();

    /**
     * Custom settings for {@link com.tinkerpop.gremlin.server.OpProcessor} implementations.
     */
    public List<ProcessorSettings> processors = new ArrayList<>();

    /**
     * Settings must be instantiated from {@link #read(String)} or {@link #read(java.io.InputStream)} methods.
     */
    private Settings() {
    }

    public Optional<ServerMetrics> optionalMetrics() {
        return Optional.ofNullable(metrics);
    }

    public Optional<SslSettings> optionalSsl() {
        return Optional.ofNullable(ssl);
    }

    /**
     * Read configuration from a file into a new {@link Settings} object.
     *
     * @param file the location of a Gremlin Server YAML configuration file
     * @return a new {@link Optional} object wrapping the created {@link Settings}
     */
    public static Settings read(final String file) throws Exception {
        final InputStream input = new FileInputStream(new File(file));
        return read(input);
    }

    /**
     * Read configuration from a file into a new {@link Settings} object.
     *
     * @param stream an input stream containing a Gremlin Server YAML configuration
     * @return a new {@link Optional} object wrapping the created {@link Settings}
     */
    public static Settings read(final InputStream stream) {
        Objects.requireNonNull(stream);

        final Constructor constructor = new Constructor(Settings.class);
        final TypeDescription settingsDescription = new TypeDescription(Settings.class);
        settingsDescription.putMapPropertyType("graphs", String.class, String.class);
        settingsDescription.putMapPropertyType("scriptEngines", String.class, ScriptEngineSettings.class);
        settingsDescription.putListPropertyType("serializers", SerializerSettings.class);
        settingsDescription.putListPropertyType("plugins", String.class);
        settingsDescription.putListPropertyType("processors", ProcessorSettings.class);
        constructor.addTypeDescription(settingsDescription);

        final TypeDescription serializerSettingsDescription = new TypeDescription(SerializerSettings.class);
        serializerSettingsDescription.putMapPropertyType("config", String.class, Object.class);
        constructor.addTypeDescription(serializerSettingsDescription);

        final TypeDescription scriptEngineSettingsDescription = new TypeDescription(ScriptEngineSettings.class);
        scriptEngineSettingsDescription.putListPropertyType("imports", String.class);
        scriptEngineSettingsDescription.putListPropertyType("staticImports", String.class);
        scriptEngineSettingsDescription.putListPropertyType("scripts", String.class);
        scriptEngineSettingsDescription.putMapPropertyType("config", String.class, Object.class);
        constructor.addTypeDescription(scriptEngineSettingsDescription);

        final TypeDescription sslSettings = new TypeDescription(SslSettings.class);
        constructor.addTypeDescription(sslSettings);

        final TypeDescription serverMetricsDescription = new TypeDescription(ServerMetrics.class);
        constructor.addTypeDescription(serverMetricsDescription);

        final TypeDescription consoleReporterDescription = new TypeDescription(ConsoleReporterMetrics.class);
        constructor.addTypeDescription(consoleReporterDescription);

        final TypeDescription csvReporterDescription = new TypeDescription(CsvReporterMetrics.class);
        constructor.addTypeDescription(csvReporterDescription);

        final TypeDescription jmxReporterDescription = new TypeDescription(JmxReporterMetrics.class);
        constructor.addTypeDescription(jmxReporterDescription);

        final TypeDescription slf4jReporterDescription = new TypeDescription(Slf4jReporterMetrics.class);
        constructor.addTypeDescription(slf4jReporterDescription);

        final TypeDescription gangliaReporterDescription = new TypeDescription(GangliaReporterMetrics.class);
        constructor.addTypeDescription(gangliaReporterDescription);

        final TypeDescription graphiteReporterDescription = new TypeDescription(GraphiteReporterMetrics.class);
        constructor.addTypeDescription(graphiteReporterDescription);

        final Yaml yaml = new Yaml(constructor);
        return yaml.loadAs(stream, Settings.class);
    }

    public static class ProcessorSettings {
        public String className;
        public Map<String, Object> config;
    }

    /**
     * Settings for the {@code ScriptEngine}.
     */
    public static class ScriptEngineSettings {
        public List<String> imports = new ArrayList<>();
        public List<String> staticImports = new ArrayList<>();
        public List<String> scripts = new ArrayList<>();
        public Map<String, Object> config = null;
    }

    public static class SerializerSettings {
        public String className;
        public Map<String, Object> config = null;
    }

    /**
     * Settings to configure SSL support.
     */
    public static class SslSettings {
        public boolean enabled = false;
        public String keyManagerAlgorithm = "SunX509";
        public String keyStoreFormat = "JKS";
        public String keyStoreFile = null;
        public String keyStorePassword = null;
        public String keyManagerPassword = null;
        public String trustStoreFile = null;
        public String trustStoreFormat = null;
        public String trustStorePassword = null;
        public String trustStoreAlgorithm = null;
    }

    /**
     * Settings for {@code Metrics} recorded by Gremlin Server.
     */
    public static class ServerMetrics {
        public ConsoleReporterMetrics consoleReporter = null;
        public CsvReporterMetrics csvReporter = null;
        public JmxReporterMetrics jmxReporter = null;
        public Slf4jReporterMetrics slf4jReporter = null;
        public GangliaReporterMetrics gangliaReporter = null;
        public GraphiteReporterMetrics graphiteReporter = null;

        public Optional<ConsoleReporterMetrics> optionalConsoleReporter() {
            return Optional.ofNullable(consoleReporter);
        }

        public Optional<CsvReporterMetrics> optionalCsvReporter() {
            return Optional.ofNullable(csvReporter);
        }

        public Optional<JmxReporterMetrics> optionalJmxReporter() {
            return Optional.ofNullable(jmxReporter);
        }

        public Optional<Slf4jReporterMetrics> optionalSlf4jReporter() {
            return Optional.ofNullable(slf4jReporter);
        }

        public Optional<GangliaReporterMetrics> optionalGangliaReporter() {
            return Optional.ofNullable(gangliaReporter);
        }

        public Optional<GraphiteReporterMetrics> optionalGraphiteReporter() {
            return Optional.ofNullable(graphiteReporter);
        }
    }

    /**
     * Settings for a {@code Metrics} reporter that writes to the console.
     */
    public static class ConsoleReporterMetrics extends IntervalMetrics {
    }


    /**
     * Settings for a {@code Metrics} reporter that writes to a CSV file.
     */
    public static class CsvReporterMetrics extends IntervalMetrics {
        public String fileName = "metrics.csv";
    }

    /**
     * Settings for a {@code Metrics} reporter that writes to JMX.
     */
    public static class JmxReporterMetrics extends BaseMetrics {
        public String domain = null;
        public String agentId = null;
    }

    /**
     * Settings for a {@code Metrics} reporter that writes to the SL4J output.
     */
    public static class Slf4jReporterMetrics extends IntervalMetrics {
        public String loggerName = Slf4jReporterMetrics.class.getName();
    }

    /**
     * Settings for a {@code Metrics} reporter that writes to Ganglia.
     */
    public static class GangliaReporterMetrics extends HostPortIntervalMetrics {
        public String addressingMode = null;
        public int ttl = 1;
        public boolean protocol31 = true;
        public UUID hostUUID = null;
        public String spoof = null;

        public GangliaReporterMetrics() {
            // default ganglia port
            this.port = 8649;
        }

        public GMetric.UDPAddressingMode optionalAddressingMode() {
            if (null == addressingMode)
                return null;

            try {
                return GMetric.UDPAddressingMode.valueOf(addressingMode);
            } catch (IllegalArgumentException iae) {
                return null;
            }
        }
    }

    /**
     * Settings for a {@code Metrics} reporter that writes to Graphite.
     */
    public static class GraphiteReporterMetrics extends HostPortIntervalMetrics {
        public String prefix = "";

        public GraphiteReporterMetrics() {
            // default graphite port
            this.port = 2003;
        }
    }

    public static abstract class HostPortIntervalMetrics extends IntervalMetrics {
        public String host = "localhost";
        public int port;
    }

    public static abstract class IntervalMetrics extends BaseMetrics {
        public long interval = 60000;
    }

    public static abstract class BaseMetrics {
        public boolean enabled = false;
    }
}
