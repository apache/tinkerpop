/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.util;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParserException;
import org.apache.tinkerpop.gremlin.process.traversal.Failure;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.util.Set;

/**
 * Exception utility class that generates exceptions in the form expected in a {@code ResponseStatus} for different
 * issues that the server can encounter.
 */
public class GremlinError {
    private final HttpResponseStatus code;
    private final String message;
    private final String exception;

    private GremlinError(HttpResponseStatus code, String message, String exception) {
        this.code = code;
        this.message = message;
        this.exception = exception;
    }

    public HttpResponseStatus getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public String getException() {
        return exception;
    }

    // ------------ request validation errors

    // script type errors
    public static GremlinError invalidGremlinType(final RequestMessage requestMessage ) {
        final String message = String.format("Message could not be parsed. Check the format of the request. [%s]",
                requestMessage);
        return new GremlinError(HttpResponseStatus.BAD_REQUEST, message, "InvalidRequestException");
    }

    // script errors
    public static GremlinError binding() {
        final String message = String.format("The message is using one or more invalid binding keys - they must be of type String and cannot be null");
        return new GremlinError(HttpResponseStatus.BAD_REQUEST, message, "InvalidRequestException");
    }

    public static GremlinError binding(final Set<String> badBindings) {
        final String message = String.format("The message supplies one or more invalid parameters key of [%s] - these are reserved names.",
                badBindings);
        return new GremlinError(HttpResponseStatus.BAD_REQUEST, message, "InvalidRequestException");
    }

    public static GremlinError binding(final int bindingsCount, final int allowedSize) {
        final String message = String.format("The message contains %s bindings which is more than is allowed by the server %s configuration",
                bindingsCount, allowedSize);
        return new GremlinError(HttpResponseStatus.BAD_REQUEST, message, "InvalidRequestException");
    }

    public static GremlinError binding(final String aliased) {
        final String message = String.format("Could not alias [%s] to [%s] as [%s] not in the Graph or TraversalSource global bindings",
                Tokens.ARGS_G, aliased, aliased);
        return new GremlinError(HttpResponseStatus.BAD_REQUEST, message, "InvalidRequestException");
    }

    public static GremlinError parsing(final GremlinParserException error) {
        return new GremlinError(HttpResponseStatus.BAD_REQUEST, error.getMessage(), "MalformedQueryException");
    }

    // execution errors
    public static GremlinError timeout(final RequestMessage requestMessage ) {
        final String message = String.format("A timeout occurred during traversal evaluation of [%s] - consider increasing the limit given to evaluationTimeout",
                requestMessage);
        return new GremlinError(HttpResponseStatus.INTERNAL_SERVER_ERROR, message, "ServerTimeoutExceededException");
    }

    public static GremlinError timedInterruptTimeout() {
        return new GremlinError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Timeout during script evaluation triggered by TimedInterruptCustomizerProvider",
                "ServerTimeoutExceededException");
    }

    public static GremlinError rateLimiting() {
        return new GremlinError(HttpResponseStatus.TOO_MANY_REQUESTS,
                "Too many requests have been sent in a given amount of time.", "TooManyRequestsException");
    }

    public static GremlinError serialization(Exception ex) {
        final String message = String.format("Error during serialization: %s", ExceptionHelper.getMessageFromExceptionOrCause(ex));
        return new GremlinError(HttpResponseStatus.INTERNAL_SERVER_ERROR, message, "ServerSerializationException");
    }

    public static GremlinError wrongSerializer(Exception ex) {
        final String message = String.format("Error during serialization: %s", ExceptionHelper.getMessageFromExceptionOrCause(ex));
        return new GremlinError(HttpResponseStatus.INTERNAL_SERVER_ERROR, message, "ServerSerializationException");
    }

    public static GremlinError longFrame(Throwable t) {
        final String message = t.getMessage() + " - increase the maxRequestContentLength";
        // todo: ResponseEntityTooLargeException? !!!
        return new GremlinError(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, message, "RequestEntityTooLargeException");
    }

    public static GremlinError longRequest(final RequestMessage requestMessage ) {
        final String message = String.format("The Gremlin statement that was submitted exceeds the maximum compilation size allowed by the JVM, please split it into multiple smaller statements - %s", requestMessage.trimMessage(1021));
        return new GremlinError(HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, message, "RequestEntityTooLargeException");
    }

    public static GremlinError temporary(final Throwable t) {
        return new GremlinError(HttpResponseStatus.INTERNAL_SERVER_ERROR, t.getMessage(), "ServerEvaluationException");
    }

    public static GremlinError failStep(final Failure failure) {
        // todo: double check message
        return new GremlinError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                failure.getMessage(), "ServerFailStepException");
    }

    public static GremlinError general(final Throwable t) {
        final String message = (t.getMessage() == null) ? t.toString() : t.getMessage();
        return new GremlinError(HttpResponseStatus.INTERNAL_SERVER_ERROR, message, "ServerErrorException");
    }

    /**
     * Creates an error for when a transaction is not found on the server.
     * This typically occurs when:
     * <ul>
     *   <li>The transaction ID was never registered (client didn't call begin)</li>
     *   <li>The transaction timed out and was automatically rolled back</li>
     *   <li>The transaction was already committed or rolled back</li>
     * </ul>
     *
     * @param transactionId The transaction ID that was not found
     * @return A GremlinError with appropriate message and status code
     */
    public static GremlinError transactionNotFound(final String transactionId) {
        final String message = String.format(
            "Transaction not found: %s. The transaction may have timed out, already been committed/rolled back, " +
            "or was never started. Call g.tx().begin() to start a new transaction.", transactionId);
        return new GremlinError(HttpResponseStatus.NOT_FOUND, message, "TransactionException");
    }

    /**
     * Creates an error for when commit or rollback is sent without a transaction ID.
     *
     * @return A GremlinError with appropriate message and status code
     */
    public static GremlinError transactionalControlRequiresTransaction() {
        final String message = "g.tx().commit() and g.tx().rollback() are only allowed in transactional requests.";
        return new GremlinError(HttpResponseStatus.BAD_REQUEST, message, "TransactionException");
    }

    /**
     * Creates an error for when a begin request is sent with a user-supplied transaction ID.
     * The server generates transaction IDs; clients should not provide them on begin.
     *
     * @return A GremlinError with appropriate message and status code
     */
    public static GremlinError beginHasTransactionId() {
        final String message = "Begin transaction request cannot have a user-supplied transactionId";
        return new GremlinError(HttpResponseStatus.BAD_REQUEST, message, "TransactionException");
    }

    /**
     * Creates an error for when the maximum number of concurrent transactions is exceeded.
     *
     * @param exceededErrorMessage The error message containing a maximum number of concurrent transactions
     * @return A GremlinError with appropriate message and status code
     */
    public static GremlinError maxTransactionsExceeded(String exceededErrorMessage) {
        final String message = exceededErrorMessage +
                " The server has reached its transaction limit. " +
                "Please wait for existing transactions to complete or increase the server's maxConcurrentTransactions setting.";
        return new GremlinError(HttpResponseStatus.SERVICE_UNAVAILABLE, message, "TransactionException");
    }

    /**
     * Creates an error for when a transaction operation times out.
     *
     * @param transactionId The transaction ID that timed out
     * @param operation The operation that timed out (e.g., "commit", "rollback", "execute")
     * @return A GremlinError with appropriate message and status code
     */
    public static GremlinError transactionTimeout(final String transactionId, final String operation) {
        final String message = String.format(
            "Transaction %s timed out during %s operation. The transaction has been rolled back. " +
            "Consider increasing the transaction timeout or breaking the operation into smaller parts.",
            transactionId, operation);
        return new GremlinError(HttpResponseStatus.GATEWAY_TIMEOUT, message, "TransactionException");
    }

    /**
     * Creates an error for when the requested graph does not support transactions.
     *
     * @param uoe The exception stating that transactions aren't supported
     * @return A GremlinError with appropriate message and status code
     */
    public static GremlinError transactionNotSupported(final UnsupportedOperationException uoe) {
        return new GremlinError(HttpResponseStatus.BAD_REQUEST, uoe.getMessage(), "TransactionException");
    }

    /**
     * Creates an error for when the transaction couldn't begin.
     *
     * @param message The error message (likely from a TransactionException)
     * @return A GremlinError with appropriate message and status code
     */
    public static GremlinError transactionUnableToStart(final String message) {
        return new GremlinError(HttpResponseStatus.INTERNAL_SERVER_ERROR, message, "TransactionException");
    }
}
