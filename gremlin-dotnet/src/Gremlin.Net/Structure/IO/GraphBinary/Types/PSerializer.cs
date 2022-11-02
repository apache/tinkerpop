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
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A <see cref="P"/> serializer.
    /// </summary>
    public class PSerializer : SimpleTypeSerializer<P>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="PSerializer" /> class.
        /// </summary>
        public PSerializer(DataType typeOfP) : base(typeOfP)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(P value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            ICollection args = value.Value is ICollection ? value.Value : new List<object> {value.Value};

            var argsLength = value.Other == null ? args.Count : args.Count + 1;

            await writer.WriteNonNullableValueAsync(value.OperatorName, stream, cancellationToken).ConfigureAwait(false);
            await writer.WriteNonNullableValueAsync(argsLength, stream, cancellationToken).ConfigureAwait(false);

            foreach (var arg in args)
            {
                await writer.WriteAsync(arg, stream, cancellationToken).ConfigureAwait(false);
            }

            if (value.Other != null)
            {
                await writer.WriteAsync(value.Other, stream, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        protected override async Task<P> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var operatorName = (string)await reader.ReadNonNullableValueAsync<string>(stream, cancellationToken)
                .ConfigureAwait(false);
            var argsLength =
                (int)await reader.ReadNonNullableValueAsync<int>(stream, cancellationToken).ConfigureAwait(false);

            var args = new object?[argsLength];
            for (var i = 0; i < argsLength; i++)
            {
                args[i] = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            }

            if (operatorName == "and" || operatorName == "or")
            {
                
                return new P(operatorName, SafelyCastToP(args[0]), SafelyCastToP(args[1]));
            }

            if (operatorName == "not")
            {
                return new P(operatorName, SafelyCastToP(args[0]));
            }

            if (argsLength == 1)
            {
                if (DataType == DataType.TextP)
                {
                    return new TextP(operatorName,
                        (string?) args[0] ?? throw new IOException("Read null but expected a string"));
                }
                return new P(operatorName, args[0]);
            }
            return new P(operatorName, args);
        }
        
        private static P SafelyCastToP([NotNull] object? pObject)
        {
            if (pObject == null) throw new IOException("Read null but expected P");
            return (P) pObject;
        }
    }
}