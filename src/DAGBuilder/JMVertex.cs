using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using DAGBuilderInterface;
using DAGBuilderCommunication;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Collections.Specialized;
using System.Xml.Serialization;

namespace DAGBuilder
{
    /// <summary>
    /// BugBug: Expose this as the execution related info.
    /// </summary>
    internal class VertexExecutionInfo
    {
        #region Constructor
        //TODO: expose all the execution Info
        internal VertexExecutionInfo(ComputeTaskExecutionInfo taskExecutionInfo)
        {
            this.taskExecutionInfo = taskExecutionInfo;
        }
        #endregion Constructor


        #region VarsAndProps

        private ComputeTaskExecutionInfo taskExecutionInfo;

        #endregion VarsAndProps
    }

    /// <summary>
    /// BugBug: expose this as vertex related statistics.
    /// </summary>
    internal class VertexStatistics
    {
        #region Constructor
        // TODO: Expose all the stats
        internal VertexStatistics(ComputeTaskStatistics taskStats)
        {
            this.taskStats = taskStats;
        }
        #endregion Constructor

        #region VarsAndProps

        private ComputeTaskStatistics taskStats;

        #endregion VarsAndProps
    }    

    /// <summary>
    /// Represents a vertex of the graph. 
    /// Vertex is the smallest execution unit of a graph. 
    /// 
    /// The vertex gives the user the ability to specify what gets
    /// executed as part of the vertex (Vertex code gets executed 
    /// on a compute node). The user can specify :
    /// Resources (Various files - Exes, Dlls, Inis, XMLs etc.)
    /// The command line associated with the vertex 
    /// Environment variables etc.
    /// </summary>
    public class JMVertex
    {
        #region internal
        
        internal JMVertex(long stageVertexId, JobStageManager parent)
        {
            this.StageVertexId = stageVertexId;
            this.GlobalVertexId = GlobalIndex.GetNextNumber();
            this.parentStageManager = parent;
            this.Name = this.GlobalVertexId.ToString();
            //string pattern = "MM_dd_yy_H_m_s_fff";
            //string uniqueName = DateTime.UtcNow.ToString(pattern);
            string uniqueName = this.ParentStageManager.StageName;
            uniqueName += "_" + this.StageVertexId.ToString();
            
            this.task = parent.StageComputeClusterToUse.CreateNewTask(/*null,*/ uniqueName);            
            this.inputEdges = new Dictionary<long, JMEdge>();
            this.outputEdges = new Dictionary<long, JMEdge>();                        
            Interlocked.Exchange(ref this.isAddedToEligibleList, 0);
            this.inputVertexIdToStatusMap = new Dictionary<long, bool>();            
            this.numInputVertexes = 0; ;
            this.numInputVertexesCompleted = 0;
            this.CurrentState = JMVertexState.JMVertexState_Unscheduled;
            this.PreviousState = JMVertexState.JMVertexState_Invalid;
            this.Outputs = new Dictionary<int, Channel>();
            Interlocked.Exchange(ref this.channelFileIdAllocator, 0);
            this.DuplicateVertexes = new List<JMVertex>();
            this.RestartedVertexes = new List<JMVertex>();
            this.VertexRetryCount = 0;
            this.IsAddedToGraphCycleCheckQueue = false;
        }

        internal void RemoveInputEdge(JMEdge edge)
        {
            lock (this)
            {
                inputEdges.Remove(edge.EdgeId);
            }
        }

        internal void AddInputEdge(JMEdge edge)
        {            
            lock (this)
            {
                inputEdges.Add(edge.EdgeId, edge);
            }
        }

        internal void RemoveOutputEdge(JMEdge edge)
        {
            lock (this)
            {
                outputEdges.Remove(edge.EdgeId);
            }
        }

        internal void AddOutputEdge(JMEdge edge)
        {
            lock (this)
            {
                outputEdges.Add(edge.EdgeId, edge);
            }
        }
       
        internal void ResetSelfForReschedule()
        {
            this.PreviousState = JMVertexState.JMVertexState_Invalid;
            this.CurrentState = JMVertexState.JMVertexState_Unscheduled;            
            this.numInputVertexesCompleted = 0;
            this.IsScheduledToExecute = 0;
        }

        internal void UnJoinOutputEdgeToVertexInternal(JMVertex to, JMEdge edge)
        {
            //lock (this) No need to lock, because this always enters via a call from the graph, which is locked.
            {
                if (this.ParentStageManager.Parent.GraphId != to.ParentStageManager.Parent.GraphId) throw new InvalidOperationException("Stages do not belong to same graph");
                //JMEdge edge = new JMEdge() { Channel = channel, EndVertex = to, StartVertex = this };
                this.RemoveOutputEdge(edge);
                to.RemoveInputEdge(edge);
                if (to.inputVertexIdToStatusMap.ContainsKey(to.StageVertexId))
                {
                    to.inputVertexIdToStatusMap.Remove(to.StageVertexId); //Initially mark this vertex as not completed.
                }                
                Interlocked.Decrement(ref to.numInputVertexes);
            }            
        }


        internal void JoinOutputChannelToVertexInternal(JMVertex to, Channel channel)
        {
            //lock (this) No need to lock, because this always enters via a call from the graph, which is locked.
            {
                if (this.ParentStageManager.Parent.GraphId != to.ParentStageManager.Parent.GraphId) throw new InvalidOperationException("Stages do not belong to same graph");
                JMEdge edge = new JMEdge() { Channel = channel, EndVertex = to, StartVertex = this};
                this.AddOutputEdge(edge);
                to.AddInputEdge(edge);
                if (!to.inputVertexIdToStatusMap.ContainsKey(to.StageVertexId))
                {
                    to.inputVertexIdToStatusMap.Add(to.StageVertexId, false); //Initially mark this vertex as not completed.
                }
                
                Interlocked.Increment(ref to.numInputVertexes);
            }            
        }        

        internal void SignalInputVertexCompleted(long vertexId)
        {
            Interlocked.Increment(ref this.numInputVertexesCompleted);
            //We do not need this lock. Because graph is not modified, once it starts running.
            //So we can safely read lockfree
            //lock (this)
            {
                //TODO: Probably just remove it from here. So that isEligible is as easy as count.
                //this.inputVertexIdToStatusMap[vertexId] = true;
            }
        }

        ///For each input vertex clone them. Cloning involves:
        ///     a. Adjust the inputs of clone to match, the vertex being cloned
        ///     b. Adjust the states? May be constructor will take care
        ///     
        internal JMVertex CloneSelfForRerun_PreviousRunDataLost(JMVertex failedNextStageVertex)
        {
            
            //JMVertex clonedVertex = new JMVertex(GlobalIndex.GetNextNumber(), this.ParentStageManager);
            JMVertex clonedVertex = this.ParentStageManager.AddVertex();
            lock (this)
            {
                //If the vertex is cloned already, just return that.
                //TODO: May be in later implementations, we can support multiple clones
                if (this.RestartedVertexes.Count > 0)
                {
                    return this.RestartedVertexes[0];
                }
                
                foreach (JMEdge edge in this.InputEdges.Values)
                {
                    edge.StartVertex.JoinOutputChannelToVertex(clonedVertex, edge.Channel);
                }

                //Get all the output channels ready
                foreach (Channel channel in this.Outputs.Values)
                {
                    Channel clonedChannel = channel.CloneSelf(clonedVertex);
                    clonedVertex.AddOutputChannel(clonedChannel);
                }

                this.numInputVertexesCompleted = failedNextStageVertex.numInputVertexesCompleted;
                //BUGBUG: Clone the Task data

                //add this to the cloned list.
                this.RestartedVertexes.Add(clonedVertex); 
                return clonedVertex;
            }
        }
        
        internal void AddOutputChannel(Channel newChannel)
        {
            lock (this)
            {
                this.Outputs.Add(newChannel.ChannelIndex, newChannel);
            }
        }


        internal void RemoveOutputEdgeToVertex(JMVertex to, JMEdge edge, bool allowModifyGraph)
        {
            this.parentStageManager.Parent.UnJoinVertexes(to, this, edge, allowModifyGraph);
        }

        internal void JoinOutputChannelToVertex(JMVertex to, Channel channel, bool allowModifyGraph)
        {
            this.parentStageManager.Parent.JoinVertexes(to, this, channel, allowModifyGraph);
        }

        
        //This is called in rare occassions when a vertex fails. So it is ok to create a new list
        //every time. But later we can decide to precreate this list as the graph is getting created.
        internal List<JMVertex> GetInputVertexList()
        {
            Dictionary<long, JMVertex> inputVertexes = new Dictionary<long,JMVertex>();
            lock (this)
            {
                foreach (JMEdge edge in this.InputEdges.Values)
                { 
                    if(!inputVertexes.ContainsKey(edge.StartVertex.GlobalVertexId))
                    {
                        inputVertexes.Add(edge.StartVertex.GlobalVertexId, edge.StartVertex);
                    }
                }
            }

            return inputVertexes.Values.ToList();
        }

        internal JMVertex CreateClone()
        {
            lock (this)
            {
                JMVertex clonedFailedVertex = this.ParentStageManager.AddVertex();
                //failedVertex.ResetSelfForReschedule();
                foreach (JMEdge edge in this.InputEdges.Values)
                {
                    edge.EndVertex = clonedFailedVertex;
                    clonedFailedVertex.InputEdges.Add(edge.EdgeId, edge);
                    
                }
                clonedFailedVertex.VertexRetryCount = this.VertexRetryCount;
                
                this.task.TaskSchedulingInfo.CopyTo(clonedFailedVertex.Task.TaskSchedulingInfo);
                return clonedFailedVertex;
            }

        }
        

        internal JMVertex CreateCloneWithClonedInputVertexes()
        {
            lock (this)
            {
                if (this.RestartedVertexes.Count() > 0)
                {
                    this.RestartedVertexes[0].VertexRetryCount = this.VertexRetryCount;
                    return this.RestartedVertexes[0];
                }


                JMVertex clonedFailedVertex = this.ParentStageManager.AddVertex();
                //failedVertex.ResetSelfForReschedule();
                foreach (JMEdge edge in this.InputEdges.Values)
                {
                    JMVertex vertexToReschedule = edge.StartVertex;
                    try
                    {
                        JMVertex clonedVertex = vertexToReschedule.CloneSelfForRerun_PreviousRunDataLost(this);
                        clonedVertex.JoinOutputChannelToVertex(clonedFailedVertex, edge.Channel, true);
                    }
                    catch (Exception ex)
                    {
                        //TODO: Throw particular exception. At present we are basically
                        // catching any exception and declaring the graph as failed.
                        throw ex;
                    }
                }
                clonedFailedVertex.VertexRetryCount = this.VertexRetryCount;
                this.RestartedVertexes.Add(clonedFailedVertex);
                this.task.TaskSchedulingInfo.CopyTo(clonedFailedVertex.Task.TaskSchedulingInfo);
                return clonedFailedVertex;
            }
            
        }


        internal void ReplaceSelfWithFailedVertexesOutputs(JMVertex failedVertex)
        {
            lock (failedVertex)
            {
                lock (this)
                {                    
                    List<JMEdge> ToRemoveEdges = new List<JMEdge>();
                    //First Add self to outputs
                    foreach (JMEdge edge in failedVertex.OutputEdges.Values)
                    {
                        this.JoinOutputChannelToVertex(edge.EndVertex, edge.Channel, true);
                        ToRemoveEdges.Add(edge);
                    }


                    //Now remove the failed vertexes outputs
                    foreach (JMEdge edge in ToRemoveEdges)
                    {
                        failedVertex.RemoveOutputEdgeToVertex(edge.EndVertex, edge, true);
                    }                    
                }
            }
        }

        private byte[] ReadBinaryFileData(string fullFilePath)
        {
            FileStream file = new System.IO.FileStream(fullFilePath, System.IO.FileMode.Open, System.IO.FileAccess.Read);

            byte[] binaryData = new byte[file.Length];
            int bytesRead = file.Read(binaryData, 0, (int)file.Length);
            file.Close();
            return binaryData;
        }

        internal void AddChannelInfoToVertex()
        {
            //Add the information to the vertex env variable
            //this.task.TaskSchedulingInfo.EnvironmentSettings.Add("VertexChannelFileName", this.channelFileName);      
            if (!string.IsNullOrEmpty(this.inputChannelFileName))
            {
                byte[] binaryData = ReadBinaryFileData(this.channelFilePath.TrimEnd(new char[] { '\\', '/' }) + "\\" + this.inputChannelFileName);
                string base64Str = Convert.ToBase64String(binaryData);

                this.task.TaskSchedulingInfo.EnvironmentSettings.Add("VertexInputChannels", base64Str);
            }


            if (!string.IsNullOrEmpty(this.outputChannelFileName))
            {
                byte[] binaryData = ReadBinaryFileData(this.channelFilePath.TrimEnd(new char[] { '\\', '/' }) + "\\" + this.outputChannelFileName);
                string base64Str = Convert.ToBase64String(binaryData);
                this.task.TaskSchedulingInfo.EnvironmentSettings.Add("VertexOutputChannels", base64Str);
            }            
        }       

        private void WriteChannelFile(bool isInput, ref string fileName)
        {            
            BinaryFormatter formatter = new BinaryFormatter();
            List<VertexData> vertexDataList = new List<VertexData>();
            string add = (isInput) ? "ip" : "op";
            foreach (var edge in (isInput) ? this.inputEdges.Values : this.outputEdges.Values)
            {
                if (edge.Channel.TypeOfChannel == ChannelType.NULL) continue;
                VertexData vertexData = new VertexData();
                vertexData.VertexChannel = edge.Channel;
                          
                if (edge.StartVertex != null)
                    vertexData.ChannelURL = edge.StartVertex.Task.Url;
                vertexDataList.Add(vertexData);
            }

            if (vertexDataList.Count == 0) return;

            this.channelFilePath = Directory.GetCurrentDirectory();
            //this.inputChannelFileName = this.GlobalVertexId + ".txt";
            fileName = add + this.GlobalVertexId + ".txt";
            //string channelFullFileName = this.channelFilePath.TrimEnd(new char[] { '\\', '/' }) + "\\" + add + this.inputChannelFileName;
            string channelFullFileName = this.channelFilePath.TrimEnd(new char[] { '\\', '/' }) + "\\" + fileName;
            this.fileStream = new FileStream(channelFullFileName, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);            
            formatter.Serialize(this.fileStream, vertexDataList);
            this.fileStream.Flush();
            this.fileStream.Close();            
        } 

        internal void SerializeChannel()
        {
            //TODO: instead of true/false, pass a type
            this.WriteChannelFile(true, ref this.inputChannelFileName);
            this.WriteChannelFile(false, ref this.outputChannelFileName);
        }        
        
        
        #endregion internal
        
        #region public 
        /// <summary>
        /// Sets the command line to be used to launch the code associated with the Vertex.
        /// Each vertex gets executed on a Compute Node. 
        /// </summary>
        /// <param name="cmdLine">The value of Command Line. Including the command line args</param>
        public void SetCommandLine(string cmdLine)
        {
            //TODO: Check if the task already finished execution
            this.task.TaskSchedulingInfo.CommandLine = cmdLine;            
        }

        /// <summary>
        /// The list of Resources (Various files - Exes, DLLs, Libs, INIs etc.)
        /// These files represent the required files that need to be in the workign Directory
        /// before the Command Line can be used to launch the code associated with the vertex.
        /// </summary>
        /// <param name="resourceFile">List of Files</param>
        public void SetResources(List<ComputeTaskResourceFile> resourceFile)
        {
            //TODO: Check if the task already finished execution
            if ((resourceFile != null) && resourceFile.Count() > 0)
                this.task.TaskSchedulingInfo.Files.AddRange(resourceFile);
        }

        /// <summary>
        /// Adds to the list of environment variables to be set before launching the code
        /// associated with a given vertex. 
        /// The Command Line is  used to launch the code associated witht the vertex
        /// </summary>
        /// <param name="name">Environment variable name</param>
        /// <param name="value">Environment variable value</param>
        public void SetEnvironmentVariable(string name, string value)
        {
            this.task.TaskSchedulingInfo.EnvironmentSettings.Add(name, value);
        }

        /// <summary>
        /// The set list of environment variables that need to be set before the 
        /// command line is  used to launch the code associated witht the vertex.
        /// Note: If other environment variables were already added, then this
        /// call will add to that list. If duplicates are found then an exception 
        /// will be raised
        /// </summary>
        /// <param name="vars"></param>
        public void SetEnvironmentVariable(NameValueCollection vars)
        {
            this.task.TaskSchedulingInfo.EnvironmentSettings.Add(vars);
        }

        /// <summary>
        /// Adds arary of channels as input channels of the vertex. 
        /// </summary>
        /// <param name="channels">Array of channels to add as input</param>
        public void AddInputs(Channel[] channels)
        {
            lock (this)
            {
                foreach (var channel in channels)
                {
                    JMEdge edge = new JMEdge() { Channel = channel, EndVertex = this, StartVertex = null};
                    this.AddInputEdge(edge);                    
                }
            }
        }

        /// <summary>
        /// Adds arary of channels as output channels of the vertex. 
        /// </summary>
        /// <param name="channels">Array of channels to add as output</param>
        public void AddOutputs(Channel[] channels)
        {
            lock (this)
            {                
                foreach(var channel in channels)
                {
                    this.Outputs.Add(channel.ChannelIndex, channel);
                    //channel.ChannelURL = this.task.Url;
                }
            }
        }

        //TODO: Add the ability for user to add more arguments.
        /// <summary>
        /// Instiantiates and adds Channels as Outputs for the vertex
        /// </summary>        
        /// /// <param name="channelType">This C# Type to instantiate to create instance of the Channel</param>
        /// <param name="isNull">Is this Channel of type ChannelType.NULL</param>
        /// <param name="howMany">The number of Instances of the Channel to create and add as output Channels of the vertex</param>
        /// <returns></returns>
        public Channel[] AddOutputs(Type channelType , bool isNull, UInt16 howMany)
        {
            if (howMany == 0) return null;
            Channel[] newChannels = new Channel[howMany];
            lock (this)
            {
                for (int i = 0; i < howMany; i++)
                {
                    //newChannels[i] = (Channel)Activator.CreateInstance(channelType, new object[] { (isNull)?ChannelType.NULL:ChannelType.Custom, i });                    
                    newChannels[i] = (Channel)Activator.CreateInstance(channelType, new object[] { (isNull) ? ChannelType.NULL : ChannelType.Custom});                    
                    this.Outputs.Add(newChannels[i].ChannelIndex, newChannels[i]);
                }
            }
            return newChannels;
        }       
        
        /// <summary>
        /// Joins the specified Output Channel ('channel' parameter) of this vertex, 
        /// as Input Channel of the specified vertex ('to' parameter)
        /// This method is generally used to define hand crafted dependencies between vertexes 
        /// </summary>
        /// <param name="to">The Vertex to which to join the given Channel. The Channel becomes an Input Channel of the 'to' vertex</param>
        /// <param name="channel">The Channel to Join</param>
        public void JoinOutputChannelToVertex(JMVertex to, Channel channel)
        {
            this.JoinOutputChannelToVertex(to, channel, false);                      
        }
       
        /// <summary>
        /// This method can be used to print a graph in text form
        /// </summary>
        /// <param name="indent">The paddig to be used</param>
        public void PrintVertex(string indent)
        {
            Trace.WriteLine(string.Format("{0}{1}", indent, StageVertexId));
            foreach (JMEdge edge in outputEdges.Values)
            {
                edge.EndVertex.PrintVertex(indent +"\t");
            }
        }
        

        #endregion public

        #region PublicVarsAndProps

        /// <summary>
        /// The system assings a unique ID to each vertex of the graph.
        /// This property exposes the unique ID.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Name Task Name of the vertex. This is the name that is given to the 
        /// Task that gets run as a result of instantiation of the vertex on the Compute Node.
        /// The system assigns a unique Task Name to each vertex.
        /// The name is based on the Stage name + per stage unique index
        /// </summary>
        public string TaskName
        {
            get
            {
                if (!string.IsNullOrEmpty(this.TaskUrl))
                {
                    string[] parts = this.TaskUrl.Split(new char[] { ':' });
                    System.Diagnostics.Debug.Assert(parts.Count() == 2);
                    return parts[1];
                }
                else
                    return null;
            }
        }
        /// <summary>
        /// URL of the task. This can be used while creating custom channels.
        /// The value of the string and its interpretation depends on the type of 
        /// cluster being used to instiantiate the vertex on to a compute node.
        /// </summary>
        public string TaskUrl
        {
            get { return this.Task.Url; }
        }
        /// <summary>
        /// The list of Output Channels of this Vertex. The uses uses the AddOutputs method 
        /// to add these channels.
        /// </summary>
        public Dictionary<int, Channel> Outputs { get; private set; }       
        /// <summary>
        /// The StageID of the vertex. 
        /// </summary>
        public long StageVertexId { get; private set; }
        /// <summary>
        /// The system assigs a graph wide unique ID to each vertex. 
        /// This property exposes that ID
        /// </summary>
        public long GlobalVertexId { get; private set; }


        #endregion PublicVarsAndProps

        #region PrivateVarsAndProps

        internal int IsScheduledToExecute
        {
            get { return this.isAddedToEligibleList; }
            set
            {
                //lock (this)
                {
                    Interlocked.Exchange(ref this.isAddedToEligibleList, value);
                }
            }
        }

        ComputeTask task;
        internal ComputeTask Task
        {
            get { return task; }
            set { task = value; }
        }



        Dictionary<long, bool> inputVertexIdToStatusMap;
        int numInputVertexes;
        internal int NumInputVertexes
        {
            get { return numInputVertexes; }
            set { numInputVertexes = value; }
        }

        int numInputVertexesCompleted; 

        Dictionary<long, JMEdge> inputEdges;
        internal Dictionary<long, JMEdge> InputEdges
        {
            get { return inputEdges; }
            set { inputEdges = value; }
        }

        Dictionary<long, JMEdge> outputEdges;
        internal Dictionary<long, JMEdge> OutputEdges
        {
            get { return outputEdges; }
            set { outputEdges = value; }
        }


        internal int NumEntriesFromOtherVertexes_GraphCycle { get; set; }

        JobStageManager parentStageManager;

        internal JobStageManager ParentStageManager
        {
            get { return parentStageManager; }
            set { parentStageManager = value; }
        }        

        /*
        bool isTraversed;

        internal bool IsTraversed
        {
            get { return isTraversed; }
            set { isTraversed = value; }
        }
         * */
        internal bool IsAddedToGraphCycleCheckQueue { get; set; }        

        private int isAddedToEligibleList;

        internal bool IsCompletedWithoutError
        {
            get
            {
                lock (this)
                {
                    if (this.Task == null)
                    {
                        return false;
                    }

                    //Trace.WriteLine(string.Format("IsCompletedWithoutError - Vertex ({0}:{1}:{2}) ExitCode={3}************", this.ParentStageManager.StageName, this.StageVertexId, ((MyFakeTask)this.Task).TaskId, this.Task.ExecutionInfo.ExitCode));
                    if ((this.Task.State == ComputeTaskState.Completed) && this.Task.ExecutionInfo.ExitCode == 0)
                    {
                        return true;
                    }
                    else
                        return false;
                }

            }
        }       

        internal bool NeedsAddingToEligibleList
        {
            get
            {
                //We do not need this lock. Because graph is not modified, once it starts running.
                //lock (this)
                {
                    //already added no need to check
                    if (this.IsScheduledToExecute == 1)
                        return false;
                    
                    if (this.numInputVertexesCompleted == this.numInputVertexes)
                    {
                        return true;
                    }
                    else
                        return false;
                }

            }
        }

        private JMVertexState currentState;
        internal JMVertexState CurrentState
        {
            get
            {
                return currentState;
            }
            set
            {
                lock (this)
                {
                    currentState = value;
                }
            }
        }

        private JMVertexState previousState;
        internal JMVertexState PreviousState
        {
            get
            { 
                return previousState;
            }
            set
            { 
                lock(this)
                {
                    previousState = value;
                }
            }
        }

        private  int channelFileIdAllocator;

        internal List<JMVertex> DuplicateVertexes { get; private set; }
        List<JMVertex> RestartedVertexes { get; set; }
        internal int VertexRetryCount { get; set; }

       

        FileStream fileStream;
        string channelFilePath = null;
        string inputChannelFileName = null;
        string outputChannelFileName = null;
        MemoryStream channelStream = new MemoryStream();

        #endregion PrivateVarsAndProps
    }
}
