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
    private boolean toStringResults = false
    private Cluster.Builder lastBuilder

    public RemoteCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":remote", ":rem", ["current", "connect", "as"], "current")
        this.mediator = mediator

        // uses toString serialization by default which lets everything come back over the wire which is easy/nice
        // for beginners
        final KryoMessageSerializerV1d0 serializer = new KryoMessageSerializerV1d0()
        serializer.configure([serializeResultToString: "true"])

        // initialize with a localhost connection
        lastBuilder = Cluster.create().addContactPoint("localhost").serializer(serializer)
        currentCluster = lastBuilder.build()
        currentCluster.init()
        this.mediator.clusterSelected(currentCluster)
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
                return "could not configure remote - arguments must be a resolvable host or a configuration file";
            }
        }

        final KryoMessageSerializerV1d0 serializer = new KryoMessageSerializerV1d0()
        serializer.configure([serializeResultToString: toStringResults.toString()])
        builder.serializer(serializer)

        currentCluster.close()
        currentCluster = builder.build();
        currentCluster.init()

        lastBuilder = builder;

        this.mediator.clusterSelected(currentCluster);

        return String.format("connected - " + currentCluster)
    }

    def Object do_as = { List<String> arguments ->
        if (!(arguments.contains("text") || arguments.contains("objects")))
            return "the 'as' option expects 'text' or 'objects' as an argument"

        def msg;
        if (arguments.contains("text")) {
            this.toStringResults = true
            msg = "results as text"
        } else {
            this.toStringResults = false
            msg = "results as objects"
        }

        final KryoMessageSerializerV1d0 serializer = new KryoMessageSerializerV1d0()
        serializer.configure([serializeResultToString: toStringResults.toString()])
        lastBuilder.serializer(serializer)

        currentCluster.close()
        currentCluster = lastBuilder.build();
        currentCluster.init()
        this.mediator.clusterSelected(currentCluster);

        return msg
    }

    def Object do_current = {
        return String.format("remote - " + currentCluster)
    }
}
