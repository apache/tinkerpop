package com.tinkerpop.gremlin.console.plugin

import com.tinkerpop.gremlin.driver.Client
import com.tinkerpop.gremlin.driver.Cluster
import com.tinkerpop.gremlin.driver.Item
import com.tinkerpop.gremlin.driver.exception.ResponseException
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode
import com.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor
import org.codehaus.groovy.tools.shell.Groovysh

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class DriverRemoteAcceptor implements RemoteAcceptor {
    private Cluster currentCluster
    private Client currentClient

    private int timeout = 180000

    private static final String TOKEN_TIMEOUT = "timeout"
    private final Groovysh shell

    public DriverRemoteAcceptor(final Groovysh shell) {
        this.shell = shell
    }

    @Override
    public Object connect(final List<String> args) {
        if (args.size() != 1) return "expects the location of a configuration file as an argument"

        try {
            currentCluster = Cluster.open(args.first())
            currentClient = currentCluster.connect()
            currentClient.init()

            return String.format("connected - " + currentCluster)
        } catch (FileNotFoundException ignored) {
            return "the 'connect' option must be accompanied by a valid configuration file";
        } catch (Exception ex) {
            return "error during 'connect' - ${ex.message}"
        }
    }

    @Override
    public Object configure(final List<String> args) {
        final def option = args.size() == 0 ? "" : args[0]
        if (!(option in [TOKEN_TIMEOUT]))
            return "the 'config' option expects one of ['$TOKEN_TIMEOUT'] as an argument"

        final def arguments = args.tail()

        if (option == TOKEN_TIMEOUT) {
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

        return this.toString()
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
                if (responseException.getResponseStatusCode() == ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
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
        this.currentClient.close()
        this.currentCluster.close()
    }

    private def List<Item> send(final String gremlin) {
        try {
            return currentClient.submit(gremlin).all().get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ignored) {
            throw new RuntimeException("Request timed out while processing - increase the timeout with the :remote command");
        }
    }

    @Override
    String toString() {
        return "gremlin server - [${currentCluster}]"
    }

    private Optional<ResponseException> findResponseException(final Throwable ex) {
        if (ex instanceof ResponseException)
            return Optional.of((ResponseException) ex);

        if (null == ex.getCause())
            return Optional.empty();

        return findResponseException(ex.getCause());
    }
}
