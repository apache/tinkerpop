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
using Gremlin.Net.Process.Traversal;

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
        ///     Gets the fields map containing language, g, parameters, and other options.
        /// </summary>
        public Dictionary<string, object> Fields { get; }

        /// <summary>
        ///     Returns a copy of this message with <paramref name="key"/> set to
        ///     <paramref name="value"/>, without mutating this instance (and therefore without
        ///     mutating the caller-owned message). Used to fill connection-level defaults onto
        ///     the outgoing request only.
        /// </summary>
        /// <param name="key">The field key to set on the copy.</param>
        /// <param name="value">The field value to set on the copy.</param>
        /// <returns>A new <see cref="RequestMessage"/> carrying the added field.</returns>
        internal RequestMessage CloneWithField(string key, object value)
        {
            var copiedFields = new Dictionary<string, object>(Fields) { [key] = value };
            return new RequestMessage(Gremlin, copiedFields);
        }

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
            private readonly Dictionary<string, object> _parameters = new Dictionary<string, object>();
            private string? _parametersString;

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
            ///     Adds a single query parameter.
            /// </summary>
            /// <param name="key">The parameter key.</param>
            /// <param name="val">The parameter value.</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddParameter(string key, object val)
            {
                if (_parametersString != null)
                    throw new InvalidOperationException("Cannot mix AddParameter() with AddParametersString().");
                _parameters[key] = val;
                return this;
            }

            /// <summary>
            ///     Adds multiple query parameters from a dictionary. The values will be
            ///     converted to a gremlin-lang string when the message is created.
            ///     Cannot be mixed with <see cref="AddParametersString"/>.
            /// </summary>
            /// <param name="parameters">The parameters to add.</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddParameters(Dictionary<string, object> parameters)
            {
                if (_parametersString != null)
                    throw new InvalidOperationException("Cannot mix AddParameters() with AddParametersString().");
                foreach (var kvp in parameters)
                {
                    _parameters[kvp.Key] = kvp.Value;
                }
                return this;
            }

            /// <summary>
            ///     Sets the query parameters as a pre-serialized gremlin-lang map literal string.
            ///     Cannot be mixed with <see cref="AddParameter"/> or <see cref="AddParameters"/>.
            /// </summary>
            /// <param name="parametersString">The gremlin-lang parameters string.</param>
            /// <returns>The <see cref="Builder" />.</returns>
            public Builder AddParametersString(string parametersString)
            {
                if (_parameters.Count > 0)
                    throw new InvalidOperationException("Cannot mix AddParametersString() with AddParameter()/AddParameters().");
                _parametersString = parametersString;
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
                // prefer pre-serialized parameters string over raw map
                if (_parametersString != null)
                {
                    _fields[Tokens.ArgsParameters] = _parametersString;
                }
                else if (_parameters.Count > 0)
                {
                    _fields[Tokens.ArgsParameters] = GremlinLang.ConvertParametersToString(_parameters);
                }
                return new RequestMessage(_gremlin, _fields);
            }
        }
    }
}
