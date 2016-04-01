using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DAGBuilder
{
    internal class GlobalLock
    {
        private GlobalLock() { }
        public bool IsGraphLocked { get; set; }

        public static GlobalLock GetInstance()
        {
            return globalLock;
        }

        static GlobalLock globalLock = new GlobalLock();
    }
}
