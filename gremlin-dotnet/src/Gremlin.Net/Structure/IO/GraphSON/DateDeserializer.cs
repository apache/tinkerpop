using System;
using Newtonsoft.Json.Linq;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    internal class DateDeserializer : IGraphSONDeserializer
    {
        private static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        public dynamic Objectify(JToken graphsonObject, GraphSONReader reader)
        {
            var milliseconds = graphsonObject.ToObject<long>();
            return UnixStart.AddTicks(TimeSpan.TicksPerMillisecond * milliseconds);
        }
    }
}