using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DAGBuilderCommunication;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace DAGBuilder
{
    /// <summary>
    /// Internal representation for connection between two vertexes.
    /// An edge has a start vertx and end vertex attached to it.
    /// The Channel in the edge represent that data structure
    /// used to specify type of communicatio between the two vertexes
    /// of the edge. 
    /// </summary>
    internal class JMEdge
    {
        public JMEdge()
        {
            this.EdgeId = GlobalIndex.GetNextNumber();
        }

        #region VarsAndProps

        public long EdgeId { get; private set; }
        
        JMVertex startVertex;

        
        public JMVertex StartVertex
        {
            get { return startVertex; }
            set { startVertex = value; }
        }


        
        JMVertex endVertex;

        
        public JMVertex EndVertex
        {
            get { return endVertex; }
            set { endVertex = value; }
        }

        public Stream SerializeChannel()
        {            
            BinaryFormatter formatter = new BinaryFormatter();
            formatter.Serialize(this.channelStream, channel);            
            return this.channelStream;
        }        

        MemoryStream channelStream = new MemoryStream();
        public Stream ChannelStream
        {
            get { return this.channelStream; }
        }        

        Channel channel;

        public Channel Channel
        {
            get { return channel; }
            set { channel = value; }
        }
        #endregion VarsAndProps
    }
}
