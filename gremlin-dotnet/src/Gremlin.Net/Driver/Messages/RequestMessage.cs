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

using System;
using System.Collections.Generic;

namespace Gremlin.Net.Driver.Messages
{
    /// <summary>
    ///     The model for a 4.0 request message sent to the server.
    /// </summary>
    public class RequestMessage
    {
        private RequestMessage(string gremlin, Dictionary<string, object> fields)
        {
            Gremlin = gremlin ?? throw new ArgumentNullException(nameof(gremlin));
            Fields = fields;
            if (!Fields.ContainsKey(Tokens.ArgsLanguage))
            {
                Fields[Tokens.ArgsLanguage] = "gremlin-lang";
            }
        }

        /// <summary>
        ///     Gets the Gremlin query string.
        /// </summary>
        public string Gremlin { get; }

        /// <summary>
        ///     Gets the fields map containing language, g, bindings, and other options.
        /// </summary>
        public Dictionary<string, object> Fields { get; }

        /// <summary>
        ///     Initializes a <see cref="Builder" /> to build a <see cref="RequestMessage" />.
        /// </summary>
        /// <param name="gremlin">The Gremlin query string.</param>
        /// <returns>A <see cref="Builder" /> to build a <see cref="RequestMessage" />.</returns>
        public static Builder Build(string gremlin)
        {
            return new Builder(gremlin);
        }

        /// <summary>
        ///     Allows to build <see cref="RequestMessage" /> objects.
        /// </summary>
        public class Builder
        {
            private readonly string _gremlin;
            private readonly Dictionary<string, object> _fields = new Dictionary<string, object>();
            private readonly Dictionary<string, object> _bindings = new Dictionary<string, object>();

            internal Builder(string gremlin)
            {
                _gremlin = gremlin;
            }

            /// <summary>
            ///     Sets the traversal source name.
            /// </summary>
            /// <param name="g">The traversal source name.</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddG(string g)
            {
                _fields[Tokens.ArgsG] = g;
                return this;
            }

            /// <summary>
            ///     Adds a single binding parameter.
            /// </summary>
            /// <param name="key">The binding key.</param>
            /// <param name="val">The binding value.</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddBinding(string key, object val)
            {
                _bindings[key] = val;
                return this;
            }

            /// <summary>
            ///     Adds multiple binding parameters.
            /// </summary>
            /// <param name="bindings">The bindings to add.</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddBindings(Dictionary<string, object> bindings)
            {
                foreach (var kvp in bindings)
                {
                    _bindings[kvp.Key] = kvp.Value;
                }
                return this;
            }

            /// <summary>
            ///     Adds a field to the request message.
            /// </summary>
            /// <param name="key">The field key.</param>
            /// <param name="value">The field value.</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddField(string key, object value)
            {
                _fields[key] = value;
                return this;
            }

            /// <summary>
            ///     Checks whether a field has been set.
            /// </summary>
            /// <param name="key">The field key to check.</param>
            /// <returns>True if the field exists.</returns>
            public bool HasField(string key) => _fields.ContainsKey(key);

            /// <summary>
            ///     Sets the evaluation timeout for this request.
            /// </summary>
            /// <param name="timeout">The timeout value.</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddEvaluationTimeout(object timeout)
            {
                _fields[Tokens.ArgsEvalTimeout] = timeout;
                return this;
            }

            /// <summary>
            ///     Sets the batch size for this request.
            /// </summary>
            /// <param name="batchSize">The batch size value.</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddBatchSize(object batchSize)
            {
                _fields[Tokens.ArgsBatchSize] = batchSize;
                return this;
            }

            /// <summary>
            ///     Sets the materializeProperties option for this request.
            /// </summary>
            /// <param name="materializeProperties">The materializeProperties value (e.g. "all" or "tokens").</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddMaterializeProperties(string materializeProperties)
            {
                _fields[Tokens.ArgMaterializeProperties] = materializeProperties;
                return this;
            }

            /// <summary>
            ///     Sets the user agent for this request.
            /// </summary>
            /// <param name="userAgent">The user agent string.</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddUserAgent(string userAgent)
            {
                _fields[Tokens.ArgsUserAgent] = userAgent;
                return this;
            }

            /// <summary>
            ///     Sets the bulk results option for this request.
            /// </summary>
            /// <param name="bulkResults">The bulk results value.</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddBulkResults(object bulkResults)
            {
                _fields[Tokens.ArgsBulkResults] = bulkResults;
                return this;
            }

            /// <summary>
            ///     Sets the language for this request.
            /// </summary>
            /// <param name="language">The language identifier (e.g. "gremlin-lang").</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddLanguage(string language)
            {
                _fields[Tokens.ArgsLanguage] = language;
                return this;
            }

            /// <summary>
            ///     Creates the <see cref="RequestMessage" /> given the settings provided to the <see cref="Builder" />.
            /// </summary>
            /// <returns>The built <see cref="RequestMessage" />.</returns>
            public RequestMessage Create()
            {
                _fields[Tokens.ArgsBindings] = _bindings;
                return new RequestMessage(_gremlin, _fields);
            }
        }
    }
}
