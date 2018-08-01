using System;
using Newtonsoft.Json.Linq;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    internal class ByteBufferDeserializer : IGraphSONDeserializer
    {
        public dynamic Objectify(JToken graphsonObject, GraphSONReader reader)
        {
            var base64String = graphsonObject.ToObject<string>();
            return Convert.FromBase64String(base64String);
        }
    }
}