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
using System.Threading.Tasks;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure.IO.GraphBinary;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphBinary
{
    public class GraphBinaryMessageSerializerTests
    {
        [Fact]
        public async Task ShouldSerializeRequestMessageToExpectedGraphBinary()
        {
            var expected = new byte[]
            {
                // header length
                0x20, 
                // header: application/vnd.graphbinary-v1.0
                0x61, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x76, 0x6e, 0x64, 0x2e,
                0x67, 0x72, 0x61, 0x70, 0x68, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x2d, 0x76, 0x31, 0x2e, 0x30,
                // version
                0x81,
                // uuid
                0x40, 0x05, 0xb3, 0x74, 0xb1, 0x21, 0x40, 0x1b, 0x91, 0x57, 0xab, 0x1f, 0x1e, 0xcc, 0x89, 0x4e,
                // op length
                0x00, 0x00, 0x00, 0x04,
                // op: "eval"
                0x65, 0x76, 0x61, 0x6c,
                // processor length
                0x00, 0x00, 0x00, 0x00,
                // args, map
                    // length
                    0x00, 0x00, 0x00, 0x01,
                    // key type: string
                    0x03,
                    // key length: 
                    0x00, 0x00, 0x00, 0x00, 0x07, 0x67, 0x72, 0x65, 0x6d, 0x6c, 0x69, 0x6e, 0x03, 0x00, 0x00, 0x00, 0x00, 0x11,
                    0x27, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x27, 0x20, 0x2b, 0x20, 0x27, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x27
            };
            var msg = RequestMessage.Build("eval").OverrideRequestId(Guid.Parse("4005b374-b121-401b-9157-ab1f1ecc894e"))
                .AddArgument("gremlin", "'Hello' + 'World'").Create();

            var serializer = new GraphBinaryMessageSerializer();

            var actual = await serializer.SerializeMessageAsync(msg);


            Assert.Equal(expected, actual);
        }
    }
}