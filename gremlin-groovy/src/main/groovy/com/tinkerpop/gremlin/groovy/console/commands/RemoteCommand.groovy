package com.tinkerpop.gremlin.groovy.console.commands

import com.tinkerpop.gremlin.driver.Cluster
import com.tinkerpop.gremlin.driver.MessageSerializer
import com.tinkerpop.gremlin.driver.ser.KryoMessageSerializerV1d0
import com.tinkerpop.gremlin.groovy.console.Mediator
import org.codehaus.groovy.tools.shell.ComplexCommandSupport
import org.codehaus.groovy.tools.shell.Groovysh

/**
 * Configure a remote connection to a Gremlin Server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class RemoteCommand extends ComplexCommandSupport {
    private final Mediator mediator
    private Cluster currentCluster
    private boolean toStringResults = true
    private Cluster.Builder lastBuilder

    private static final String TOKEN_TEXT = "text"
    private static final String TOKEN_OBJECTS = "objects"

    private static final MessageSerializer AS_OBJECTS = new KryoMessageSerializerV1d0()
    private static final MessageSerializer AS_TEXT = new KryoMessageSerializerV1d0()

    static {
        AS_OBJECTS.configure([serializeResultToString: "false"])
        AS_TEXT.configure([serializeResultToString: "true"])
    }

    public RemoteCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":remote", ":rem", ["current", "connect", "as", "timeout"], "current")
        this.mediator = mediator

        // initialize with a localhost connection. uses toString serialization by default which lets everything
        // come back over the wire which is easy/nice for beginners
        lastBuilder = Cluster.create().addContactPoint("localhost").serializer(AS_TEXT)
        makeCluster()
    }

    def Object do_timeout = { List<String> arguments ->
        final String errorMessage = "the timeout option expects a positive integer representing milliseconds or 'max' as an argument"
        if (arguments.size() != 1) return errorMessage
        try {
            final int to = arguments.get(0).equals("max") ? Integer.MAX_VALUE : Integer.parseInt(arguments.get(0))
            if (to <= 0) return errorMessage

            mediator.remoteTimeout = to
            return "set remote timeout to ${to}ms"
        } catch (Exception ex) {
            return errorMessage
        }
    }

    def Object do_connect = { List<String> arguments ->
        Cluster.Builder builder
        final String line = String.join(" ", arguments)

        try {
            final InetAddress addy = InetAddress.getByName(line)
            builder = Cluster.create(addy.getHostAddress());
        } catch (UnknownHostException e) {
            // not a hostname - try to treat it as a property file
            try {
                builder = Cluster.create(new File(line))
            } catch (FileNotFoundException fnfe) {
                return "the 'connect' option must be a resolvable host or a configuration file";
            }
        }

        lastBuilder = builder
        makeCluster()

        return String.format("connected - " + currentCluster)
    }

    def Object do_as = { List<String> arguments ->
        if (!(arguments.contains(TOKEN_TEXT) || arguments.contains(TOKEN_OBJECTS)))
            return "the 'as' option expects '$TOKEN_TEXT' or '$TOKEN_OBJECTS' as an argument"

        this.toStringResults = arguments.contains(TOKEN_TEXT)
        makeCluster()

        return resultsAsMessage()
    }

    def Object do_current = {
        final resultsAs = resultsAsMessage()
        return "remote - $resultsAs [${currentCluster}]"
    }

    private def makeCluster() {
        lastBuilder.serializer(chooseSerializer())
        if (currentCluster != null) currentCluster.close()
        currentCluster = lastBuilder.build();
        currentCluster.init()
        this.mediator.clusterSelected(currentCluster);
    }

    private def String resultsAsMessage() {
        final resultsAs = toStringResults ? TOKEN_TEXT : TOKEN_OBJECTS
        return "results as $resultsAs"
    }

    private def MessageSerializer chooseSerializer() {
        return toStringResults ? AS_TEXT : AS_OBJECTS
    }
}
