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
using System.Text;
using System.Threading.Tasks;
using Gremlin.Net.Structure.IO.GraphSON;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class GraphSONMessageSerializerTests
    {
        [Fact]
        public async Task DeserializingNullThrows()
        {
            var sut = CreateMessageSerializer();

            await Assert.ThrowsAsync<ArgumentNullException>(()=> sut.DeserializeMessageAsync(null));
        }

        [Fact]
        public async Task EmptyArrayDeserializedIntoNull()
        {
            var sut = CreateMessageSerializer();

            var result = await sut.DeserializeMessageAsync(new byte[0]);
            
            Assert.Null(result);
        }

        [Fact]
        public async Task EmptyStringDeserializedIntoNull()
        {
            var sut = CreateMessageSerializer();
            var ofEmpty = Encoding.UTF8.GetBytes("");

            Assert.Null(await sut.DeserializeMessageAsync(ofEmpty));
        }

        [Fact]
        public async Task JsonNullDeserializedIntoNull()
        {
            var sut = CreateMessageSerializer();
            var ofNull = Encoding.UTF8.GetBytes("null");

            Assert.Null(await sut.DeserializeMessageAsync(ofNull));
        }

        private static GraphSONMessageSerializer CreateMessageSerializer()
        {
            return new GraphSON3MessageSerializer(new GraphSON3Reader(), new GraphSON3Writer());
        }
    }
}
