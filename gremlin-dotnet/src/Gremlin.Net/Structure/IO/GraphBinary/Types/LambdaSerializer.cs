﻿#region License

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

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A serializer for lambdas.
    /// </summary>
    public class LambdaSerializer : SimpleTypeSerializer<ILambda>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="LambdaSerializer" /> class.
        /// </summary>
        public LambdaSerializer() : base(DataType.Lambda)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(ILambda value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await writer.WriteNonNullableValueAsync(value.Language, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(value.LambdaExpression, stream, cancellationToken)
                .ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(value.Arguments, stream, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        protected override async Task<ILambda> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var language = (string)await reader.ReadNonNullableValueAsync<string>(stream, cancellationToken)
                .ConfigureAwait(false);
            var expression = (string)await reader.ReadNonNullableValueAsync<string>(stream, cancellationToken)
                .ConfigureAwait(false);
            
            // discard the arguments
            await reader.ReadNonNullableValueAsync<int>(stream, cancellationToken).ConfigureAwait(false);

            return new StringBasedLambda(expression, language);
        }
    }
}