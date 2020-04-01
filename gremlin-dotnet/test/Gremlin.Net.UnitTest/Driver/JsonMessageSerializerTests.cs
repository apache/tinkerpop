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

using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using System;
using System.Text;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class JsonMessageSerializerTests
    {
        [Fact]
        public void DeserializingNullThrows()
        {
            var sut = new JsonMessageSerializer(GremlinClient.DefaultMimeType);

            Assert.Throws<ArgumentNullException>(()=> sut.DeserializeMessage<ResponseMessage>(null));
        }

        [Fact]
        public void EmptyArrayDeserializedIntoNull()
        {
            var sut = new JsonMessageSerializer(GremlinClient.DefaultMimeType);

            Assert.Null(sut.DeserializeMessage<ResponseMessage>(new byte[0]));            
        }

        [Fact]
        public void EmptyStringDeserializedIntoNull()
        {
            var sut = new JsonMessageSerializer(GremlinClient.DefaultMimeType);
            var ofEmpty = Encoding.UTF8.GetBytes("");

            Assert.Null(sut.DeserializeMessage<ResponseMessage>(ofEmpty));
        }

        [Fact]
        public void JsonNullDeserializedIntoNull()
        {
            var sut = new JsonMessageSerializer(GremlinClient.DefaultMimeType);
            var ofNull = Encoding.UTF8.GetBytes("null");

            Assert.Null(sut.DeserializeMessage<ResponseMessage>(ofNull));
        }
    }
}
