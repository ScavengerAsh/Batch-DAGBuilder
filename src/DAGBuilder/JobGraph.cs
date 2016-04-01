using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using System.Threading;
using System.Diagnostics;
using DAGBuilderInterface;
using DAGBuilderCommunication;

namespace DAGBuilder
{
    /// <summary>
    /// This represents the DAG associated with the work that needs to be done.
    /// A general flow of how the system works is explained below.
    /// 1. The user starts with creating an instance of the JobGraph.
    /// 2. The  user then  creates Stages. Each stage acts as a template on how to create Vertexes in that Stage
    /// 3. The user then adds vertexes to each stage. Based on the stage template. 
    ///    e.g. (The user might have specified the list of resources for the Stage)
    /// 4. The user then adds Output Channels to vertexes of the stage
    /// 5. User then joins the Output Channels of each of the vertexes as input Channels for the next stage of vertexes
    /// 6. Thus defining a DAG of work to be done
    /// 7. Upon calling the JobGraph::Execute method, the graph begins to execute and vertexes are scheduled according to
    ///    the specified dependencies.
    /// </summary>
    public class JobGraph
    {
        #region Constructor
        /// <summary>
        /// Creates an instance of the JobGraph object
        /// </summary>
        /// <param name="jobName">The name of the DAG</param>
        /// <param name="graphComputeCluster">The default compute cluster to use to instanciate vertexes of the Graph.
        /// NOTE: The user can specify a seperate compute cluster per Stage. In case no cluster for a given stage is 
        /// specified, then this cluster will be  used for the vertexes of that stage.
        /// </param>
        public JobGraph(string jobName, ComputeCluster graphComputeCluster)
        {
            this.JobName = jobName;
            this.vertexIdToVertexMap = new Dictionary<long, JMVertex>();
            this.jobStages = new List<JobStageManager>();
            this.graphVertexes = new List<JMVertex>();
            this.primaryStages = new Dictionary<long,JobStageManager>();
            this.isGraphLocked = false;
            this.graphId = GlobalIndex.GetNextNumber();
            this.firstRunVertexes = new Dictionary<long, List<JMVertex>>();
            this.scheduleableVertexes = new List<JMVertex>();
            //Scheduling thread is initialzed when execute is called.
            this.FailedVertexesMissingInputdata = new Dictionary<long, JMVertex>();
            this.FailedVertexesNeedsRetry = new Dictionary<long, JMVertex>();
            this.DefaultComputeCluster = graphComputeCluster;
        }
        #endregion Constructor

        #region Public                
        /// <summary>
        /// Creates a new Stage in the graph.
        /// NOTE: 
        /// 1.  Stage is a grouping of a collection of nodes
        /// 2.  The creation order of stages does not have any bearing on when the vertexes of the stage are scheduled.
        ///     Stage just acts as a collection of vertexes who have similar metadata. 
        ///     e.g. All vertexes of the same stage might all share the same resource files, Or They might all run on a particular size of VMs.        
        /// </summary>
        /// <param name="stageName">Each stage can be named. The stage name is then used as part of the vertex names of that stage</param>
        /// <returns></returns>
        public JobStageManager AddStage(string stageName)
        {
            return AddStage(stageName, 0);
        }

        /// <summary>
        /// Create a stage  in the graph and also allocate vertexes of the stage
        /// </summary>
        /// <param name="stageName">Name of the Stage</param>
        /// <param name="numVertexes">Number of vertexes of the Stage to create</param>
        /// <returns></returns>
        public JobStageManager AddStage(string stageName, UInt32 numVertexes)
        {
            return AddStage(stageName, numVertexes, null, null);
        }

        /// <summary>
        /// Create a stage in the graph, allocated vertexes and assign common resources for the stage. 
        /// These common resoruces are then passed to each vertex of the Stage
        /// </summary>
        /// <param name="stageName">Name of the Stage</param>
        /// <param name="numVertexes">Number of vertexes</param>
        /// <param name="stageResources">List of common resources for each vertex of the Stage</param>
        /// <returns></returns>
        public JobStageManager AddStage(string stageName, UInt32 numVertexes, List<ComputeTaskResourceFile> stageResources)
        {
            return AddStage(stageName, numVertexes, stageResources, null);
        }

        /// <summary>
        /// Create a stage in the graph, allocated vertexes, assign common resources and specify a compute cluster for stage
        /// </summary>
        /// <param name="stageName">Name of the Stage</param>
        /// <param name="numVertexes">Number of vertexes of the Stage to create</param>
        /// <param name="stageResources">List of common resource for each vertex of the Stage</param>
        /// <param name="computeCluster">The compute cluster to use for this stage. Each vertex of this stage will be created on this compute cluster</param>
        /// <returns></returns>
        public JobStageManager AddStage(string stageName, UInt32 numVertexes, List<ComputeTaskResourceFile> stageResources, ComputeCluster computeCluster)
        {
            JobStageManager stageManager = null;
            if (this.isGraphLocked) throw new InvalidOperationException("Graph is locked for execution");
            //ComputeCluster toUseComputeCluster = DAGBuilder.Cluster;
            ComputeCluster toUseComputeCluster = this.DefaultComputeCluster;
            if (computeCluster != null)
            {
                toUseComputeCluster = computeCluster;
            }

            if (toUseComputeCluster == null)
            {
                throw new Exception("computeCluster param cannot be null as no graphComputeCluster was set during the creation of the Graph object ");
            }
            lock (this)
            {
                stageManager = new JobStageManager(stageName, numVertexes, stageResources, this, toUseComputeCluster);
                if (this.isGraphLocked) throw new InvalidOperationException("Graph is locked for execution");
                jobStages.Add(stageManager);
                primaryStages.Add(stageManager.StageId, stageManager);
                graphVertexes.AddRange(stageManager.StageVertexes.Values);
            }
            return stageManager;
        }

        /// <summary>
        /// Create dependeny on the vertexes of two stages.
        /// </summary>
        /// <param name="from">The Stage whose output Channels will be joined</param>
        /// <param name="to">The Stage to whose vertexes the output of the 'from' stage will be joined to </param>
        /// <param name="joinType">The type of Join to create. See <typeparamref name="DAGBuilder.StageVertexJoin"/>for details</param>
        /// <param name="channelType">The Type of Channel</param>
        public void JoinStages(JobStageManager from, JobStageManager to, StageVertexJoin joinType, ChannelType channelType)
        {
            if (from.Parent.graphId != to.Parent.GraphId) throw new InvalidOperationException("Stages do not belong to same graph");
            if (this.isGraphLocked) throw new InvalidOperationException("Graph is locked for execution");
            lock (this)
            {
                if (this.isGraphLocked) throw new InvalidOperationException("Graph is locked for execution");
                switch (joinType)
                {
                    case StageVertexJoin.Cross:
                        DoCrossJoin(from, to, channelType);
                        break;
                    case StageVertexJoin.Ratio:
                    case StageVertexJoin.OneToOne:
                        DoRatioJoin(from, to, 1, channelType);
                        break;
                }
                primaryStages.Remove(to.StageId);
            }
        }

        /// <summary>
        /// Create dependeny on the vertexes of two stages. Creates a N:1 join. See <typeparamref name="StageVertexJoin"/>for details
        /// </summary>
        /// <param name="from">The Stage whose output Channels will be joined</param>
        /// <param name="to">The Stage to whose vertexes the output of the 'from' stage will be joined to </param>
        /// <param name="attachNInputstoOne">How many outputs to join to a single vertex</param>
        /// <param name="channelType">Type of Channel</param>
        public void RatioJoinStages(JobStageManager from, JobStageManager to, int attachNInputstoOne, ChannelType channelType)
        {
            if (from.Parent.graphId != to.Parent.GraphId) throw new InvalidOperationException("Stages do not belong to same graph");
            if (this.isGraphLocked) throw new InvalidOperationException("Graph is locked for execution");
            lock (this)
            {
                if (this.isGraphLocked) throw new InvalidOperationException("Graph is locked for execution");            
                DoRatioJoin(from, to, attachNInputstoOne, channelType);
                primaryStages.Remove(to.StageId);
            }
        }

        internal void AddFailedVertexNeedsRetry(JMVertex failedVertex)
        {
            lock (this)
            {
                this.FailedVertexesNeedsRetry.Add(failedVertex.StageVertexId, failedVertex);
            }
        }

        internal void AddFailedVertexMissingInputData(JMVertex failedVertex)
        {
            lock (this)
            {
                this.FailedVertexesMissingInputdata.Add(failedVertex.StageVertexId, failedVertex);
            }
        }

        /// <summary>
        /// Helper API to print graph in a string format
        /// </summary>
        /// <param name="viaStages">Whether all Stages also should be printed</param>
        public void PrintGraph(bool viaStages)
        {
            lock (this)
            {

                if (viaStages)
                {
                    foreach (JobStageManager stageMgr in jobStages)
                    {
                        Console.WriteLine();
                        Console.WriteLine(string.Format("Stage {0}", stageMgr.StageId));
                        stageMgr.PrintVertexes();
                        Console.WriteLine();
                    }
                }
                else
                {
                    foreach (JobStageManager jobStage in primaryStages.Values)
                    {
                        foreach (JMVertex vertex in jobStage.StageVertexes.Values)
                        {
                            vertex.PrintVertex("");
                        }
                    }

                }
            }
        }               

        //TODO: Supply a BeginExecute/EndExecute functionality
        /// <summary>
        /// Start the execution of the Directed Acyclic Graph.
        /// Once the method is called, then the Graph cannot be modified by  the user.
        /// </summary>
        public void Execute()
        {
            if (this.isGraphLocked) throw new InvalidOperationException("Graph is already locked for execution");
            lock (this)
            {
                if (this.DoesGraphHaveCycles()) throw new InvalidOperationException("Graph is not a DAG.");
                if (this.isGraphLocked) throw new InvalidOperationException("Graph is already locked for execution");
                this.isGraphLocked = true;
            }
            this.scheduler = new Scheduler(this);

            //initialize the scheduling thread
            this.schedulingThread = new Thread(new ThreadStart(scheduler.Schedule));
            this.schedulingThread.Name = "SchedulingThreadFunc";

            this.schedulingThread.Start();
            //TODO: The execute could just return and then fire an event when things are done
            this.schedulingThread.Join();

        }

        #endregion public
        
        #region Internal        
        

        internal void UnJoinVertexes(JMVertex to, JMVertex from, JMEdge edge, bool allowModifyGraph)
        {
            if (!allowModifyGraph && this.isGraphLocked) throw new InvalidOperationException("Graph is locked for execution");
            lock (this)
            {
                if (!allowModifyGraph && this.isGraphLocked) throw new InvalidOperationException("Graph is locked for execution");
                from.UnJoinOutputEdgeToVertexInternal(to, edge);
            }
        }

        internal void JoinVertexes(JMVertex to, JMVertex from, Channel channel, bool allowModifyGraph)
        {
            if (!allowModifyGraph && this.isGraphLocked) throw new InvalidOperationException("Graph is locked for execution");
            lock (this)
            {
                if (!allowModifyGraph && this.isGraphLocked) throw new InvalidOperationException("Graph is locked for execution");
                from.JoinOutputChannelToVertexInternal(to, channel);
            }
        }

        internal void DeInitialize(string reason)
        {
            foreach (var stage in this.jobStages)
            {
                stage.StageComputeClusterToUse.ClusterDeInitialize(reason);
            }
        }

        #endregion Internal

        #region Private


        private void SchedulingThreadFunc()
        { 
            // Schedules all eligible vertexes
            // Wake up after every N secs or when N number of task are eligible
            // TODO: Later wake up on cluster size change event            
        }

        /// <summary>
        /// Raises the JobExecutionDone event and invokes handlers
        /// </summary>
        protected virtual void OnRaiseJobExecutionDone()
        {
            // Make a temporary copy of the event to avoid possibility of
            // a race condition if the last subscriber unsubscribes
            // immediately after the null check and before the event is raised.
            EventHandler handler = this.JobExecuteDone;

            // Event will be null if there are no subscribers
            if (handler != null)
            {
                //TODO: Do it in a seperate thread?
                // Use the () operator to raise the event.
                handler(this, null);
            }
        }


        private void AddScheduleableVertex(JMVertex vertex)
        {
            lock (this.scheduleableVertexes)
            {                
                this.scheduleableVertexes.Add(vertex);
            }
        }



        /// <summary>
        /// Cross join is done to do partitioning. Stage1 produces data as a set of channels.
        /// Each Channel corrosponds to a partition. When a cross join is done, then the next 
        /// stage is joined with the previous stage in such a way that each of the 2nd stage vertex
        /// corrosponds to a partiton. Thus each vertex reads its partition from each of the previous
        /// stages channels.
        /// Stage 1-->Vertex 1--> [3 channels]
        /// Stage 2 --> Vertex 1 --> Reads Stage 1's Channel[0]
        /// Stage 2 --> Vertex 2 --> Reads Stage 1's Channel[1]
        /// Stage 2 --> Vertex 3 --> Reads Stage 1's Channel[2]        
        /// </summary>
        /// <param name="prevStage">The previous stage JobStageManager</param>
        /// <param name="nextStage">The next stage JobStageManager</param>
        /// <param name="channelType">Type of Channel</param>
        private void DoCrossJoin(JobStageManager prevStage, JobStageManager nextStage, ChannelType channelType)
        {
            lock (this)
            {
                lock (prevStage)
                {
                    lock (nextStage)
                    {                       
                        int numNextStageVertexes = nextStage.StageVertexes.Count;
                        if (numNextStageVertexes > 0)
                        {
                            foreach (JMVertex vertex in prevStage.StageVertexes.Values)
                            {
                                foreach (Channel channel in vertex.Outputs.Values)
                                {
                                    int jointoVertexAtIndex = channel.ChannelIndex % numNextStageVertexes;
                                    JMVertex to = nextStage.StageVertexes.ElementAt(jointoVertexAtIndex).Value;
                                    vertex.JoinOutputChannelToVertex(to, channel);
                                }
                            }
                        }
                        else
                        {
                            throw new Exception("No vertexes in the stage to join to");
                        }
                    }
                }
            }
        }


        private void DoRatioJoin(JobStageManager from, JobStageManager to, int attachNInputstoOne, ChannelType channelType)
        {
            List<JMVertex> input = new List<JMVertex>();
            bool done = false;
            lock (this)
            {
                lock (from)
                {
                    var enumerator = from.StageVertexes.Values.GetEnumerator();
                    lock (to)
                    {
                        foreach (JMVertex toVertex in to.StageVertexes.Values)
                        {
                            if (done) break;
                            for (int iter = 0; iter < attachNInputstoOne; iter++)
                            {
                                if (enumerator.MoveNext())
                                {
                                    //NOTE: in ration join we expect only 1 output channel for each vertex to be joined.
                                    enumerator.Current.JoinOutputChannelToVertex(toVertex, enumerator.Current.Outputs.First().Value);
                                }
                                else done = true;
                            }
                        }
                    }
                }
            }
        }

        internal Dictionary<long, List<JMVertex> > FilterInitialRunVertexes(bool clearCheckCycleFlagsAndCount = true)
        {
            lock (this)
            {
                this.firstRunVertexes.Clear();
                //TODO: make sure the graph is locked by now
                foreach (JMVertex graphvertex in graphVertexes)
                {
                    if (clearCheckCycleFlagsAndCount)
                    {
                        //graphvertex.IsTraversed = false;
                        graphvertex.IsAddedToGraphCycleCheckQueue = false;
                        graphvertex.NumEntriesFromOtherVertexes_GraphCycle = 0;
                    }
                    if (graphvertex.InputEdges.Count > 0)
                    {
                        bool isInitialRunVertex = false;
                        foreach (var edge in graphvertex.InputEdges.Values)
                        {
                            //BUGBUG: If there are vertexes, for which someinputs are from blobs and others from a prior vertex, then this will fail?
                            if (edge.StartVertex != null)
                            {
                                isInitialRunVertex = false;
                                break;
                            }
                            else
                            {
                                isInitialRunVertex = true;
                            }
                        }
                        if (false == isInitialRunVertex)
                            continue;
                    }
                    if (this.firstRunVertexes.ContainsKey(graphvertex.ParentStageManager.StageId))
                    {
                        this.firstRunVertexes[graphvertex.ParentStageManager.StageId].Add(graphvertex);
                    }
                    else
                    {
                        this.firstRunVertexes[graphvertex.ParentStageManager.StageId] = new List<JMVertex>();
                        this.firstRunVertexes[graphvertex.ParentStageManager.StageId].Add(graphvertex);
                    }
                }

                return this.firstRunVertexes;
            }
        }

        /// <summary>
        /// Checks whether the Graph is Directed Acyclic or not.
        /// </summary>
        /// <returns>true if graph is cyclic, otherwise false</returns>
        public bool DoesGraphHaveCycles()
        {
            //TODO: enable this assert
            //System.Diagnostics.Debug.Assert(isGraphLocked);
            Queue<JMVertex> queue = new Queue<JMVertex>();
            lock (this)
            {
                this.FilterInitialRunVertexes(true);
                foreach (List<JMVertex> vertexes in this.firstRunVertexes.Values)
                {
                    foreach (JMVertex vertex in vertexes)
                    {
                        queue.Enqueue(vertex);
                        vertex.IsAddedToGraphCycleCheckQueue = true;
                    }
                }

                while (queue.Count > 0)
                {
                    var vertex = queue.Dequeue();                    
                    if (vertex.OutputEdges == null) continue;

                    foreach (JMEdge outputEdge in vertex.OutputEdges.Values)
                    {
                        if (outputEdge.EndVertex != null)
                        {
                            outputEdge.EndVertex.NumEntriesFromOtherVertexes_GraphCycle++;
                            if (outputEdge.EndVertex.NumEntriesFromOtherVertexes_GraphCycle > outputEdge.EndVertex.NumInputVertexes)
                            {
                                Trace.WriteLine(string.Format("Cycle found in graph at vertex - {0}, comming from vertex {1}", outputEdge.EndVertex.Name, vertex.Name));
                                return true;
                            }

                            if (!outputEdge.EndVertex.IsAddedToGraphCycleCheckQueue)
                            {
                                queue.Enqueue(outputEdge.EndVertex);
                                outputEdge.EndVertex.IsAddedToGraphCycleCheckQueue = true;
                            }
                        }
                    }
                }
            }
            return false;
        }        

        #endregion Private

        #region VarsAndProps
        
        private string JobName { get; set; }        
        private Dictionary<long, JMVertex> vertexIdToVertexMap;
        private List<JobStageManager> jobStages;
        private List<JMVertex> graphVertexes;

        internal List<JMVertex> GraphVertexes
        {
            get { return graphVertexes; }            
        }


        private Dictionary<long, List<JMVertex>> firstRunVertexes;

        internal Dictionary<long, List<JMVertex>> FirstRunVertexes
        {
            get { return firstRunVertexes; }            
        }

        private Dictionary<long, JobStageManager> primaryStages;

        private bool isGraphLocked;
        
        private long graphId;

        internal long GraphId
        {
            get { return graphId; }
            set { graphId = value; }
        }

        private ComputeCluster DefaultComputeCluster { get; set; }
        private List<JMVertex> scheduleableVertexes;
        private Scheduler scheduler;
        private Thread schedulingThread;

        /// <summary>
        /// The list of vertexes that failed because input data to those vertexes was missing
        /// </summary>
        public Dictionary<long, JMVertex> FailedVertexesMissingInputdata { get; set; }

        /// <summary>
        /// The list of vertexes that have marked themselves as failed and asked for a retry
        /// </summary>
        public Dictionary<long, JMVertex> FailedVertexesNeedsRetry { get; set; }

        /// <summary>
        /// JobExecuteDone event is rasied with the Job is completed.
        /// </summary>
        public event EventHandler JobExecuteDone;
        
        #endregion VarsAndProps
    }    
}
