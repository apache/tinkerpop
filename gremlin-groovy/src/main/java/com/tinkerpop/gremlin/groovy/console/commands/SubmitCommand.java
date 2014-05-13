package com.tinkerpop.gremlin.groovy.console.commands;

import com.tinkerpop.gremlin.driver.Item;
import com.tinkerpop.gremlin.driver.exception.ResponseException;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.groovy.console.Mediator;
import org.codehaus.groovy.tools.shell.CommandSupport;
import org.codehaus.groovy.tools.shell.Groovysh;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * Submit a script to a Gremlin Server instance.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SubmitCommand extends CommandSupport {

    private final Mediator mediator;

    public SubmitCommand(final Groovysh shell, final Mediator mediator) {
        super(shell, ":submit", ":>");
        this.mediator = mediator;
    }

    @Override
    public Object execute(final List<String> arguments) {
        final String line = String.join(" ", arguments);

        try {
            final List<Item> resultSet = mediator.submit(line);
            shell.getInterp().getContext().setProperty("_l", resultSet);
            return resultSet;
        } catch (Exception ex) {
            final Optional<ResponseException> inner = findResponseException(ex);
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

    public Optional<ResponseException> findResponseException(final Throwable ex) {
        if (ex instanceof ResponseException)
            return Optional.of((ResponseException) ex);

        if (null == ex.getCause())
            return Optional.empty();

        return findResponseException(ex.getCause());
    }
}