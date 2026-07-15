#region License

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

#endregion

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     Provides extension methods for the <see cref="IGremlinClient" /> interface.
    /// </summary>
    public static class GremlinClientExtensions
    {
        /// <summary>
        ///     Submits a request message that consists of a script with parameters as an asynchronous operation where only a single
        ///     result gets returned.
        /// </summary>
        /// <remarks>
        ///     If multiple results are received from Gremlin Server, then only the first gets returned. Use
        ///     <see cref="SubmitAsync{T}" /> instead when you expect a collection of results.
        /// </remarks>
        /// <typeparam name="T">The type of the expected result.</typeparam>
        /// <param name="gremlinClient">The <see cref="IGremlinClient" /> that submits the request.</param>
        /// <param name="requestScript">The Gremlin request script to send.</param>
        /// <param name="parameters">Parameters used in the requestScript.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A single result received from the Gremlin Server.</returns>
        /// <exception cref="Exceptions.ResponseException">
        ///     Thrown when a response is received from Gremlin Server that indicates
        ///     that an error occurred.
        /// </exception>
        public static async Task<T?> SubmitWithSingleResultAsync<T>(this IGremlinClient gremlinClient,
            string requestScript,
            Dictionary<string, object>? parameters = null,
            CancellationToken cancellationToken = default)
        {
            await using var resultSet = await gremlinClient.SubmitAsync<T>(requestScript, parameters, cancellationToken)
                .ConfigureAwait(false);
            await foreach (var item in resultSet.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                return item;
            }
            return default;
        }

        /// <summary>
        ///     Submits a request message as an asynchronous operation where only a single result gets returned.
        /// </summary>
        /// <remarks>
        ///     If multiple results are received from Gremlin Server, then only the first gets returned. Use
        ///     <see cref="SubmitAsync{T}" /> instead when you expect a collection of results.
        /// </remarks>
        /// <typeparam name="T">The type of the expected result.</typeparam>
        /// <param name="gremlinClient">The <see cref="IGremlinClient" /> that submits the request.</param>
        /// <param name="requestMessage">The <see cref="RequestMessage" /> to send.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A single result received from the Gremlin Server.</returns>
        /// <exception cref="Exceptions.ResponseException">
        ///     Thrown when a response is received from Gremlin Server that indicates
        ///     that an error occurred.
        /// </exception>
        public static async Task<T?> SubmitWithSingleResultAsync<T>(this IGremlinClient gremlinClient,
            RequestMessage requestMessage, CancellationToken cancellationToken = default)
        {
            await using var resultSet =
                await gremlinClient.SubmitAsync<T>(requestMessage, cancellationToken).ConfigureAwait(false);
            await foreach (var item in resultSet.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                return item;
            }
            return default;
        }

        /// <summary>
        ///     Submits a request message that consists of a script with parameters as an asynchronous operation without returning
        ///     the result received from the Gremlin Server.
        /// </summary>
        /// <param name="gremlinClient">The <see cref="IGremlinClient" /> that submits the request.</param>
        /// <param name="requestScript">The Gremlin request script to send.</param>
        /// <param name="parameters">Parameters used in the requestScript.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        /// <exception cref="Exceptions.ResponseException">
        ///     Thrown when a response is received from Gremlin Server that indicates
        ///     that an error occurred.
        /// </exception>
        public static async Task SubmitAsync(this IGremlinClient gremlinClient, string requestScript,
            Dictionary<string, object>? parameters = null, CancellationToken cancellationToken = default)
        {
            await using var resultSet = await gremlinClient.SubmitAsync<object>(requestScript, parameters, cancellationToken).ConfigureAwait(false);
            // Drain the stream to ensure the background task completes and resources are released.
            await foreach (var _ in resultSet.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
            }
        }

        /// <summary>
        ///     Submits a request message as an asynchronous operation without returning the result received from the Gremlin
        ///     Server.
        /// </summary>
        /// <param name="gremlinClient">The <see cref="IGremlinClient" /> that submits the request.</param>
        /// <param name="requestMessage">The <see cref="RequestMessage" /> to send.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>The task object representing the asynchronous operation.</returns>
        /// <exception cref="Exceptions.ResponseException">
        ///     Thrown when a response is received from Gremlin Server that indicates
        ///     that an error occurred.
        /// </exception>
        public static async Task SubmitAsync(this IGremlinClient gremlinClient, RequestMessage requestMessage,
            CancellationToken cancellationToken = default)
        {
            await using var resultSet = await gremlinClient.SubmitAsync<object>(requestMessage, cancellationToken).ConfigureAwait(false);
            // Drain the stream to ensure the background task completes and resources are released.
            await foreach (var _ in resultSet.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
            }
        }

        /// <summary>
        ///     Submits a request message that consists of a script with parameters as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T">The type of the expected results.</typeparam>
        /// <param name="gremlinClient">The <see cref="IGremlinClient" /> that submits the request.</param>
        /// <param name="requestScript">The Gremlin request script to send.</param>
        /// <param name="parameters">Parameters used in the requestScript.</param>
        /// <param name="cancellationToken">The token to cancel the operation. The default value is None.</param>
        /// <returns>A <see cref="ResultSet{T}"/> containing the data and status attributes returned from the server.</returns>
        /// <exception cref="Exceptions.ResponseException">
        ///     Thrown when a response is received from Gremlin Server that indicates
        ///     that an error occurred.
        /// </exception>
        public static async Task<ResultSet<T>> SubmitAsync<T>(this IGremlinClient gremlinClient,
            string requestScript,
            Dictionary<string, object>? parameters = null,
            CancellationToken cancellationToken = default)
        {
            var msgBuilder = RequestMessage.Build(requestScript);
            if (parameters != null)
                msgBuilder.AddParameters(parameters);
            var msg = msgBuilder.Create();
            return await gremlinClient.SubmitAsync<T>(msg, cancellationToken).ConfigureAwait(false);
        }
    }
}