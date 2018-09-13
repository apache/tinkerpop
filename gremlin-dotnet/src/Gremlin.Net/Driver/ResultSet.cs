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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System.Collections;
using System.Collections.Generic;

namespace Gremlin.Net.Driver
{
    /// <summary>
    ///     A ResultSet is returned from the submission of a Gremlin script to the server and represents the results
    ///     provided by the server. ResultSet includes enumerable data and status attributes.
    /// </summary>
    /// <typeparam name="T">Type of the result elements</typeparam>
    public sealed class ResultSet<T> : IReadOnlyCollection<T>
    {
        private readonly IReadOnlyCollection<T> _data;

        /// <summary>
        ///     Gets or sets the status attributes from the gremlin response
        /// </summary>
        public IReadOnlyDictionary<string, object> StatusAttributes { get; }

        /// <summary>
        ///     Initializes a new instance of the ResultSet class for the specified data and status attributes.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="attributes"></param>
        public ResultSet(IReadOnlyCollection<T> data, IReadOnlyDictionary<string, object> attributes)
        {
            _data = data;
            this.StatusAttributes = attributes;
        }

        /// <inheritdoc cref="IReadOnlyCollection{T}"/>
        public IEnumerator<T> GetEnumerator()
        {
            return _data.GetEnumerator();
        }

        /// <inheritdoc cref="IReadOnlyCollection{T}"/>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return _data.GetEnumerator();
        }

        /// <inheritdoc cref="IReadOnlyCollection{T}"/>
        public int Count => _data.Count;
    }
}
