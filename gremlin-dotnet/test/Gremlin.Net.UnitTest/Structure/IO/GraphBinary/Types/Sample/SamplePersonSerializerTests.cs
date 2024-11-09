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

using System;
using System.IO;
using System.Threading.Tasks;
using Gremlin.Net.Structure.IO.GraphBinary;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary.Types.Sample
{
    public class SamplePersonSerializerTests
    {
        [Fact]
        public async Task TestCustomSerializationWithPerson()
        {
            var expected = new SamplePerson("Olivia", new DateTimeOffset(2010, 4, 29, 5, 30, 3, TimeSpan.FromHours(1)));
            var registry = TypeSerializerRegistry.Build()
                .AddCustomType(typeof(SamplePerson), new SamplePersonSerializer()).Create();
            var writer = CreateGraphBinaryWriter(registry);
            var reader = CreateGraphBinaryReader(registry);
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(expected, serializationStream);
            serializationStream.Position = 0;
            var actual = (SamplePerson?) await reader.ReadAsync(serializationStream);
            
            Assert.Equal(expected, actual);
        }
        
        [Fact]
        public async Task ReadNullableValueAndWriteNullableValueShouldBeSymmetric()
        {
            var expected = new SamplePerson("Olivia", new DateTimeOffset(2010, 4, 29, 5, 30, 3, TimeSpan.FromHours(1)));
            var registry = TypeSerializerRegistry.Build()
                .AddCustomType(typeof(SamplePerson), new SamplePersonSerializer()).Create();
            var writer = CreateGraphBinaryWriter(registry);
            var reader = CreateGraphBinaryReader(registry);
            var serializationStream = new MemoryStream();

            await writer.WriteNullableValueAsync(expected, serializationStream).ConfigureAwait(false);
            serializationStream.Position = 0;
            var actual = (SamplePerson?)await reader.ReadNullableValueAsync<SamplePerson>(serializationStream)
                .ConfigureAwait(false);
            
            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task ReadNonNullableValueAndWriteNonNullableValueShouldBeSymmetric()
        {
            var expected = new SamplePerson("Olivia", new DateTimeOffset(2010, 4, 29, 5, 30, 3, TimeSpan.FromHours(1)));
            var registry = TypeSerializerRegistry.Build()
                .AddCustomType(typeof(SamplePerson), new SamplePersonSerializer()).Create();
            var writer = CreateGraphBinaryWriter(registry);
            var reader = CreateGraphBinaryReader(registry);
            var serializationStream = new MemoryStream();

            await writer.WriteNonNullableValueAsync(expected, serializationStream).ConfigureAwait(false);
            serializationStream.Position = 0;
            var actual = (SamplePerson)await reader.ReadNonNullableValueAsync<SamplePerson>(serializationStream)
                .ConfigureAwait(false);
            
            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task WriteThrowsExceptionWhenNoWriterIsRegisteredForCustomType()
        {
            var sample = new SamplePerson("Olivia", new DateTimeOffset(2010, 4, 29, 5, 30, 3, TimeSpan.FromHours(1)));
            var registryWithoutType = TypeSerializerRegistry.Build().Create();
            var writer = CreateGraphBinaryWriter(registryWithoutType);
            var serializationStream = new MemoryStream();
            
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await writer.WriteAsync(sample, serializationStream);
            });
        }

        [Fact]
        public async Task ReadThrowsExceptionWhenNoReaderIsRegisteredForCustomType()
        {
            var sample = new SamplePerson("Olivia", new DateTimeOffset(2010, 4, 29, 5, 30, 3, TimeSpan.FromHours(1)));
            var registryWithType = TypeSerializerRegistry.Build()
                .AddCustomType(typeof(SamplePerson), new SamplePersonSerializer()).Create();
            var writer = CreateGraphBinaryWriter(registryWithType);
            var serializationStream = new MemoryStream();
            
            await writer.WriteAsync(sample, serializationStream);
            serializationStream.Position = 0;
            
            var registryWithoutType = TypeSerializerRegistry.Build().Create();
            var reader = CreateGraphBinaryReader(registryWithoutType);
            
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await reader.ReadAsync(serializationStream);
            });
        }

        private static GraphBinaryWriter CreateGraphBinaryWriter(TypeSerializerRegistry registry) =>
            new GraphBinaryWriter(registry);

        private static GraphBinaryReader CreateGraphBinaryReader(TypeSerializerRegistry registry) =>
            new GraphBinaryReader(registry);
    }
}