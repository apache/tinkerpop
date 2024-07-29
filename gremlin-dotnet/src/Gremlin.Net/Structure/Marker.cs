using System;
using System.Collections.Generic;
using System.Text;

namespace Gremlin.Net.Structure
{

    /// <summary>
    /// 
    /// </summary>
    public class Marker
    {
        /// <summary>
        /// 
        /// </summary>
        public byte Value { get; private set; }
        /// <summary>
        /// 
        /// </summary>
        public static Marker END_OF_STREAM = new(0);

        private Marker(byte value)
        {
            Value = value;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Marker Of(byte value)
        {
            if (value != 0) throw new ArgumentException();
            return END_OF_STREAM;
        }
    }
}
