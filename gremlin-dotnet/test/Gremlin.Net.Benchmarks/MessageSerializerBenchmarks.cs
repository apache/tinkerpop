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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure.IO.GraphBinary;
using Gremlin.Net.Structure.IO.GraphSON;
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;
using static Gremlin.Net.Process.Traversal.__;
using static Gremlin.Net.Process.Traversal.P;

namespace Gremlin.Net.Benchmarks
{
    [MemoryDiagnoser]
    public class MessageSerializerBenchmarks
    {
        private static readonly Bytecode EmptyBytecode = new Bytecode();

        private static readonly Bytecode SomeBytecode = Traversal().WithComputer().V().
            Has("Name", "marko").
            Where(
                Out("knows").
                Has("name", Within(new List<string> {"stephen", "peter", "josh"})).
                Count().
                Is(Gt(3))).
            Has("birthDate", Lt(DateTimeOffset.Parse("1980-01-01 00:00:00"))).
            Bytecode;

        private static readonly RequestMessage RequestMessageWithEmptyBytecode = RequestMessageFor(EmptyBytecode);
        
        private static readonly RequestMessage RequestMessage = RequestMessageFor(SomeBytecode);
        
        private static RequestMessage RequestMessageFor(Bytecode bytecode) => RequestMessage.Build(Tokens.OpsBytecode)
            .Processor(Tokens.ProcessorTraversal).OverrideRequestId(Guid.NewGuid())
            .AddArgument(Tokens.ArgsGremlin, bytecode).Create();

        private static readonly GraphBinaryMessageSerializer BinaryMessageSerializer =
            new GraphBinaryMessageSerializer();

        private static readonly GraphSON3MessageSerializer
            GraphSON3MessageSerializer = new GraphSON3MessageSerializer();
        
        [Benchmark]
        public async Task<byte[]> TestWriteEmptyBytecodeBinary() =>
            await BinaryMessageSerializer.SerializeMessageAsync(RequestMessageWithEmptyBytecode)
                .ConfigureAwait(false);
        
        [Benchmark]
        public async Task<byte[]> TestWriteEmptyBytecodeGraphSON3() =>
            await GraphSON3MessageSerializer.SerializeMessageAsync(RequestMessageWithEmptyBytecode)
                .ConfigureAwait(false);
        
        [Benchmark]
        public async Task<byte[]> TestWriteBytecodeBinary() =>
            await BinaryMessageSerializer.SerializeMessageAsync(RequestMessage)
                .ConfigureAwait(false);
        
        [Benchmark]
        public async Task<byte[]> TestWriteBytecodeGraphSON3() =>
            await GraphSON3MessageSerializer.SerializeMessageAsync(RequestMessage)
                .ConfigureAwait(false);

        [Benchmark]
        public async Task<ResponseMessage<List<object>>> TestReadSmallResponseMessageBinary() =>
            await BinaryMessageSerializer.DeserializeMessageAsync(TestMessages.SmallBinaryResponseMessageBytes)
                .ConfigureAwait(false);
        
        [Benchmark]
        public async Task<ResponseMessage<List<object>>> TestReadSmallResponseMessageGraphSON3() =>
            await GraphSON3MessageSerializer.DeserializeMessageAsync(TestMessages.SmallGraphSON3ResponseMessageBytes)
                .ConfigureAwait(false);
        
        [Benchmark]
        public async Task<ResponseMessage<List<object>>> TestReadBigResponseMessageBinary() =>
            await BinaryMessageSerializer.DeserializeMessageAsync(TestMessages.BigBinaryResponseMessageBytes)
                .ConfigureAwait(false);
        
        [Benchmark]
        public async Task<ResponseMessage<List<object>>> TestReadBigResponseMessageGraphSON3() =>
            await GraphSON3MessageSerializer.DeserializeMessageAsync(TestMessages.BigGraphSON3ResponseMessageBytes)
                .ConfigureAwait(false);
    }
}