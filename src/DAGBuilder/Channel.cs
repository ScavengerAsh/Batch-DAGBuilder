using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DAGBuilder;


namespace DAGBuilderCommunication
{
    /// <summary>
    /// Vertex related information (Channel, URl of channel), gets serialized
    /// as VertexData. The input channels and output channels of the vertex 
    /// are serializedas two seperate Lists of VertexData.
    /// 
    /// The serialized List of VertexData are available when the vertex runs on 
    /// a for the is available at Compute Node. The vertex code can then use 
    /// that data to deserialize it and get informatio about the channels.
    /// See also: BatchClientUtils class gives helpers to deserialize and present
    /// this as a List of VertexData.
    /// </summary>
    [Serializable()]
    public class VertexData
    {
        /// <summary>
        /// VertexData class constructor
        /// </summary>
        public VertexData() { }
        /// <summary>
        /// The Channel associated with a given Vertex.
        /// The same Channel can be assiciated with multiple vertexes.
        /// On one vertex it will be an input channel, while on the other
        /// it will be an output channel. 
        /// In this context, the VertexChannel property represents associated
        /// with a particula Vertex. 
        /// </summary>
        public Channel VertexChannel { get; set; }
        /// <summary>
        /// The URL for the channel. The dataProvider uses this.
        /// </summary>
        public string ChannelURL { get; set; }
    }

    /// <summary>
    /// The types of channel
    /// </summary>
    public enum ChannelType
    { 
        /// <summary>
        /// This is an invalid channel. This means error
        /// </summary>
        Invalid,
        /// <summary>
        /// NULL channels are used to indicate dependencies between Vertexes. 
        /// It also indicatesthat there is no data dependency between those Vertexes
        /// A NULl channel data does not get serialized at all. Just that the 
        /// GraphManager uses it to order scheduling of vertexes.
        /// </summary>
        NULL,
        /// <summary>
        /// The system allows users to build thier own custom Channels. 
        /// In those cases the use would use the type of Channel as Custom
        /// </summary>
        Custom
    }

    /// <summary>
    /// Represents the data transfer connection between two vertexes
    /// </summary>
    [Serializable()]
    public class Channel
    {
        private static int ChannelIndexCounter = -1;
        private static int GetNextIndex()
        {
            return System.Threading.Interlocked.Increment(ref Channel.ChannelIndexCounter);
        }
        #region Constructor
        /// <summary>
        /// Channel constructor
        /// </summary>
        public Channel() 
        {
            this.TypeOfChannel = ChannelType.Invalid;            
        }
        /// <summary>
        /// Channel constructor
        /// </summary>
        /// <param name="channelType">Takes a type of channel. 
        /// See <typeparamref name="DAGBuilderCommunication.ChannelType"> for more details</typeparamref>/></param>
        public Channel(ChannelType channelType)
        {
            this.TypeOfChannel = channelType;                        
            this.ChannelIndex = GetNextIndex();
        }
        #endregion Constructor

        #region Public
        virtual internal Channel CloneSelf(JMVertex parent)
        {
            Channel clonedChannel = new Channel(this.TypeOfChannel /*, this.ChannelFileId, this.ChannelIndex*/);
            clonedChannel.ChannelIndex = this.ChannelIndex;
            return clonedChannel;
        }
        #endregion Public

        #region VarsAndProps_Public
        /// <summary>
        /// Channel Type
        /// </summary>
        public ChannelType TypeOfChannel { get; set; }
        /// <summary>
        /// Every channel gets assigned a unique index in a given graph.
        /// This property represents the index.
        /// </summary>
        public int ChannelIndex { get; private set; }
        #endregion VarsAndProps_Public
    }
   
}
