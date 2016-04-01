using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using DAGBuilderInterface;

namespace DAGBuilder
{
    internal class GraphBuilderSettings
    { 
        public static int MaxVertexRetryCount = 2;
    }
    internal class GlobalIndex
    {
        #region VarsAndProps
        private static long number = 0;
        internal static long GetNextNumber()
        {
            return Interlocked.Increment(ref number);
        }
        #endregion VarsAndProps
    }   
}
