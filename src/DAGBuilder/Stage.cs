using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using DAGBuilderInterface;

namespace DAGBuilder
{
    /// <summary>
    /// Defines the type of joins that are supported by the system
    /// Users can use these joins to acheive merging, partitioning. 
    /// </summary>
    public enum StageVertexJoin
    { 
        /// <summary>
        /// This type of Join is generally used for Partitioning of data.
        /// Where vertexes in a previous stage produce data in multiple partitions (expressed as Output Channels). 
        /// Vertexes of the next stage then consume the produced data, 
        /// where generally there is an independent vertex per partition (OutPut Channel) of data.
        /// Each independent vertex picks up its assigned partition from each of the vertexes in the previous stage        
        /// </summary>
        Cross,
        /// <summary>
        /// This join is used to create a 1:1 dependency between vertexes of multiple stages. 
        /// In some sense it is a specialized case of Ratio Join
        /// </summary>
        OneToOne,
        /// <summary>
        /// This join is generally used for data aggregation, where a set of vertexes in a previous stage produce data (expressed as Output Channels)
        /// And then the merge vertex in the next stage aggregates the previously produced data. To do so, stages use the Ratio Join.
        /// </summary>
        Ratio        
    }
   
    /// <summary>
    /// When a Graph::AddStage is called, an instance of this type is returned.
    /// This acts as a bag of properties that get applied to every vertex of that Stage.  
    /// Individual vertexes, still have the ability to override all or some of those properties.
    /// 
    /// Stages also give the ability to express dependencies and serve as a short hand instead of having 
    /// to specify dependencies per vertex of the Graph
    /// For e.g. Conside a 2 stage job that does error counting from log files that are stored in blobs
    /// Stage1 reads data from blobs and reduces it to count of errors by category. The 2nd stage then aggregates all the errors
    /// that were produced as part of reduction at each of the vertexes of Stage 1.
    /// To express this dependency between stages, the user can simply do a Ration Join between the two stages. 
    /// Thus avoiding the need to hand craft the dependency between each vertex of the 2 stages. 
    /// </summary>
    public class JobStageManager
    {
        #region Constructor                
        
        internal JobStageManager(string stageName, UInt32 numVertexes, List<ComputeTaskResourceFile> stageResources, JobGraph parent, ComputeCluster stageComputeCluster)
        {
            Initialize(stageName, numVertexes, stageResources, parent, null, null, stageComputeCluster);
        }

       
        #endregion Constructor

        #region Public           
        /// <summary>
        /// The vertexes that belong to this stage.
        /// The key (long) is the uniquely assigned vertex Id (assigned by the system). The value represents the Vertex
        /// </summary>
        public Dictionary<long, JMVertex> StageVertexes 
        {
            get { return stageVertexes; }
        }

        /// <summary>
        /// Helper method to print the info about each vertex in text form. Prints as a single line
        /// </summary>
        public void PrintVertexes()
        {
            foreach (JMVertex vertex in stageVertexes.Values)
            {
                Console.Write("{0}\t", vertex.StageVertexId);
            }
        }

        /// <summary>
        /// Adds a new vertex to the stage. 
        /// </summary>
        /// <returns>the newly added vertex</returns>
        
        public JMVertex AddVertex()
        {
            JMVertex vertex = new JMVertex(Interlocked.Increment(ref this.VertexIdGenerator), this);
            vertex.SetResources (this.stageResources);
            lock (this)
            {
                stageVertexes.Add(vertex.StageVertexId, vertex);
            }
            return vertex;
        }

        /// <summary>
        /// Name of the stage. This name is used as a prefix for naming vertexes in that stage.
        /// </summary>
        public string StageName { get; private set; }        

        #endregion Public  
     
        #region internal

        internal JMVertex AddVertex_hasLock()
        {
            JMVertex vertex = new JMVertex(Interlocked.Increment(ref this.VertexIdGenerator), this);
            vertex.SetResources(this.stageResources);
            stageVertexes.Add(vertex.StageVertexId, vertex);
            return vertex;
        }

        internal void GenerateNewStageId()
        {
            StageId = GlobalIndex.GetNextNumber();
        }

        internal JobGraph Parent { get; set; }

        internal JobStageManager PreviousStage { get; set; }
        internal JobStageManager NextStage { get; set; }
        internal ComputeCluster StageComputeClusterToUse { get; set; }
        internal long StageId { get; private set; }

        #endregion internal

        #region Private
        private void Initialize(string stageName, UInt32 numVertexes, List<ComputeTaskResourceFile> stageResources, JobGraph parent, JobStageManager previousStage, JobStageManager nextStage, ComputeCluster stageComputeClusterToUse)
        {
            Interlocked.Exchange(ref this.VertexIdGenerator, -1); //so that the 1st vertex get 0
            GenerateNewStageId();
            StageName = stageName;
            this.stageResources = stageResources;
            this.StageComputeClusterToUse = stageComputeClusterToUse;
            Parent = parent;
            PreviousStage = previousStage;
            NextStage = nextStage;
            lock (this)
            {
                for (UInt32 i = 0; i < numVertexes; i++)
                {
                    AddVertex_hasLock();
                }
            }
        }

        private long VertexIdGenerator;        
        
        private Dictionary<long, JMVertex> stageVertexes = new Dictionary<long, JMVertex>();
        List<ComputeTaskResourceFile> stageResources;
        
        #endregion Private
    } 
    
}
