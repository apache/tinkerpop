package com.tinkerpop.gremlin.console

import com.tinkerpop.gremlin.driver.Client
import com.tinkerpop.gremlin.driver.Cluster
import com.tinkerpop.gremlin.driver.Item
import com.tinkerpop.gremlin.driver.MessageSerializer
import com.tinkerpop.gremlin.driver.exception.ResponseException
import com.tinkerpop.gremlin.driver.message.ResultCode
import com.tinkerpop.gremlin.driver.ser.KryoMessageSerializerV1d0
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor
import org.codehaus.groovy.tools.shell.Groovysh

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class DriverRemoteAcceptor implements RemoteAcceptor {
    private Cluster currentCluster
    private Cluster.Builder lastBuilder

    private int timeout = 180000

    private static final String TOKEN_TIMEOUT = "timeout"
    private static final String TOKEN_AS = "as"
    private static final String TOKEN_TEXT = "text"
    private static final String TOKEN_OBJECTS = "objects"
    private static final String TOKEN_CUSTOM = "custom"

    private static final Map<String, MessageSerializer> serializers = [:].withDefault { null }
    private static final MessageSerializer AS_OBJECTS = new KryoMessageSerializerV1d0()
    private static final MessageSerializer AS_TEXT = new KryoMessageSerializerV1d0()

    private String serializerType = TOKEN_TEXT

    private final Groovysh shell;

    static {
        AS_OBJECTS.configure([serializeResultToString: "false"])
        AS_TEXT.configure([serializeResultToString: "true"])

        serializers[TOKEN_TEXT] = AS_TEXT
        serializers[TOKEN_OBJECTS] = AS_OBJECTS
    }

    public DriverRemoteAcceptor(final Groovysh shell) {
        // initialize with a localhost connection. uses toString serialization by default which lets everything
        // come back over the wire which is easy/nice for beginners
        lastBuilder = Cluster.create().addContactPoint("localhost").serializer(AS_TEXT)

        this.shell = shell
    }

    @Override
    public Object connect(final List<String> args) {
        final String line = String.join(" ", args)
        if (!line.isEmpty()) {
            Cluster.Builder builder
            try {
                final InetAddress addy = InetAddress.getByName(line)
                builder = Cluster.create(addy.getHostAddress())
                lastBuilder = builder
            } catch (UnknownHostException e) {
                // not a hostname - try to treat it as a property file
                try {
                    builder = Cluster.create(new File(line))
                    lastBuilder = builder
                } catch (FileNotFoundException ignored) {
                    return "the 'connect' option must be a resolvable host or a configuration file";
                }
            }
        }

        makeCluster()

        return String.format("connected - " + currentCluster)
    }

    @Override
    public Object configure(final List<String> args) {
        final def option = args.size() == 0 ? "" : args[0]
        if (!(option in [TOKEN_AS, TOKEN_TIMEOUT]))
            return "the 'config' option expects one of ['$TOKEN_AS', '$TOKEN_TIMEOUT'] as an argument"

        final def arguments = args.tail()

        if (option == TOKEN_AS) {
            this.serializerType = arguments[0]
            if (serializerType == TOKEN_CUSTOM) {
                if (arguments.size() != 2) return "when specifying '$TOKEN_CUSTOM' a ${MessageSerializer.class.getSimpleName()} instance should be specified after it"

                final String serializerBinding = arguments.get(1)
                final def suspectedSerializer = arguments[serializerBinding]

                if (null == suspectedSerializer) return "$serializerBinding is not a variable instantiated in the console"
                if (!(suspectedSerializer instanceof MessageSerializer)) return "$serializerBinding is not a ${MessageSerializer.class.getSimpleName()} instance"

                serializers[TOKEN_CUSTOM] = suspectedSerializer
            }
        } else if (option == TOKEN_TIMEOUT) {
            final String errorMessage = "the timeout option expects a positive integer representing milliseconds or 'max' as an argument"
            if (arguments.size() != 1) return errorMessage
            try {
                final int to = arguments.get(0).equals("max") ? Integer.MAX_VALUE : Integer.parseInt(arguments.get(0))
                if (to <= 0) return errorMessage

                timeout = to
                return "set remote timeout to ${to}ms"
            } catch (Exception ignored) {
                return errorMessage
            }
        }

        makeCluster()

        return resultsAsMessage()
    }

    @Override
    public Object submit(final List<String> args) {
        final String line = String.join(" ", args)

        try {
            final List<Item> resultSet = send(line)
            shell.getInterp().getContext().setProperty("_l", resultSet)
            return resultSet
        } catch (Exception ex) {
            final Optional<ResponseException> inner = findResponseException(ex)
            if (inner.isPresent()) {
                final ResponseException responseException = inner.get();
                if (responseException.getResultCode() == ResultCode.SERVER_ERROR_SERIALIZATION)
                    return String.format("Server could not serialize the result requested. Server error - %s. Note that the class must be serializable by the client and server for proper operation.", responseException.getMessage());
                else
                    return responseException.getMessage();
            } else if (ex.getCause() != null)
                return ex.getCause().getMessage();
            else
                return ex.getMessage();
        }
    }

    @Override
    void close() throws IOException {
        this.currentCluster.close()
    }

    private def List<Item> send(final String gremlin) {
        final Client client = currentCluster.connect();
        try {
            return client.submit(gremlin).all().get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException toe) {
            throw new RuntimeException("request timed out while processing - increase the timeout with the :remote command");
        } finally {
            try {
                client.close();
            } catch (Exception ex) {
                // empty
            }
        }
    }

    @Override
    String toString() {
        final String resultsAs = resultsAsMessage()
        return "$resultsAs [${currentCluster}]"
    }

    private def String resultsAsMessage() {
        return "gremlin server - results as $serializerType"
    }

    private def makeCluster() {
        lastBuilder.serializer(serializers[serializerType])
        if (currentCluster != null) currentCluster.close()
        currentCluster = lastBuilder.build()
        currentCluster.init()
    }

    private Optional<ResponseException> findResponseException(final Throwable ex) {
        if (ex instanceof ResponseException)
            return Optional.of((ResponseException) ex);

        if (null == ex.getCause())
            return Optional.empty();

        return findResponseException(ex.getCause());
    }
}
