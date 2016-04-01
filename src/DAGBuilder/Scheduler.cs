using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;
using DAGBuilderInterface;

namespace DAGBuilder
{
    class Scheduler
    {
        #region Constructor
        static Scheduler()
        {
            Scheduler.VertexFileRetentionPeriod = TimeSpan.FromHours(1);
        }
        public Scheduler(JobGraph graph)
        {
            this.graph = graph;
            this.numVertexesInGraph = graph.GraphVertexes.Count;
            this.numVertexesEnded = 0;
            this.readyVertexes = new Dictionary<long, JMVertex>();
            this.runningVertexes = new Dictionary<long, JMVertex>();
            this.completedVertexes = new Dictionary<long, JMVertex>();
            this.completed_RetainDataVertexes = new Dictionary<long, JMVertex>();
            this.scheduleWaitEvent = new ManualResetEvent(false);
            this.vertexStateManager = new JMVertexStateManager(this, null);
            Interlocked.Exchange(ref this.shouldEndScheduling, 0);
        }
        #endregion Constructor

        #region Internal

        internal IAsyncResult BeginSchedule()
        {
            throw new NotImplementedException("Async schedule is not yet implemented");
        }

        internal bool AddEligibleVertex(JMVertex vertex)
        {
            lock (this)
            {
                if (!this.readyVertexes.ContainsKey(vertex.GlobalVertexId))
                {
                    this.readyVertexes.Add(vertex.GlobalVertexId, vertex);
                    vertex.IsScheduledToExecute = 1; //this will do an interlocked change
                    this.scheduleWaitEvent.Set();
                    return true;
                }
                else 
                    return false;
            }
        }

        internal void Schedule()
        {

            try
            {
                //Start the vertexFileRetentionTimer
                this.increaseVertexFileRetentionTimer = new Timer((target) =>
                    {
                        lock (this)
                        {
                            foreach (JMVertex vertex in this.completed_RetainDataVertexes.Values)
                            {
                                //TODO: Increase the vertex retention for each vertex                                
                                // if failed to increase then what?
                            }
                        }
                    },
                    null,
                    TimeSpan.FromMinutes((Scheduler.VertexFileRetentionPeriod.Minutes / 2)),
                    TimeSpan.FromMinutes((Scheduler.VertexFileRetentionPeriod.Minutes / 2))
                    );

                //Start the scheduling now.
                this.GetInitialReadyList();
                UInt64 count = 0;
                DateTime startTime = DateTime.UtcNow;

                while (true)
                {
                    Trace.WriteLine(string.Format("Ready to schedule round number {0}. NumCompleted={1}, TotalTasks={2}", count++, this.numVertexesEnded, this.numVertexesInGraph));

                    lock (this)
                    {
                        if ((this.numVertexesEnded == this.numVertexesInGraph) && this.graph.FailedVertexesMissingInputdata.Count() == 0)
                        {
                            //TODO: Cancel currently executing vertexes.
                            Trace.WriteLine(string.Format("Graph Execution completed. Total vertexes scheduled = {0}", this.numVertexesEnded));
                            System.Diagnostics.Debug.Assert(this.readyVertexes.Count == 0);
                            break;
                        }

                        if (this.shouldEndScheduling == 1)
                        {
                            //TODO: Cancel currently executing vertexes.
                            Trace.WriteLine(string.Format("Graph Execution completed. Because vertexes failed. Num completed vertexes = {0}", this.numVertexesEnded));
                            //Console.WriteLine(string.Format("Graph Execution completed. Because vertexes failed. Num completed vertexes = {0}", this.numVertexesEnded));                        
                            break;
                        }

                        //DumpCompletedVertexIds(ref startTime);
                        //Before we start the next round of scheduling, lets take care of retries and reruns of subgraphs, due to failures
                        this.RescheduleGraphPortion();

                        this.RescheduleFailedVertexes();

                        Trace.WriteLine(string.Format("Number of tasks to schedule = {0}", this.readyVertexes.Values.Count));

                        foreach (JMVertex vertex in this.readyVertexes.Values)
                        {
                            this.vertexStateManager.HandleVertexStateTransitions(vertex);
                        }
                        this.readyVertexes.Clear();
                        this.scheduleWaitEvent.Reset();
                    }
                    this.scheduleWaitEvent.WaitOne(TimeSpan.FromSeconds(1));
                }
                //BUGBUG: Call each stages ClusterDeInitialize here.
                //DAGBuilder.Cluster.ClusterDeInitialize("All Tasks Done");    
                this.graph.DeInitialize("Job done"); 
            }
            catch (Exception e)
            {
                Trace.WriteLine(e.ToString());
            }
        }


        internal void OnVertexStart(JMVertex vertex)
        {
            //This is the place where we know that the vertex has
            //started its state transition process

        }

        internal void OnVertexScheduling(JMVertex vertex)
        { 
            //NOP
        }
        internal void OnVertexScheduled(JMVertex vertex)
        {            
            lock (this)
            {
                this.runningVertexes.Add(vertex.GlobalVertexId, vertex);                
            }             
        }

        internal void OnVertexCompleted(JMVertex vertex, int error, bool hasRetainData)
        {
            lock (this)
            {                
                this.runningVertexes.Remove(vertex.GlobalVertexId);

                switch (error)
                { 
                    case DagBuilderWellKnownErrors.Passed:
                        if (hasRetainData)
                        {
                            this.completed_RetainDataVertexes.Add(vertex.GlobalVertexId, vertex);
                        }
                        else
                        {
                            this.completedVertexes.Add(vertex.GlobalVertexId, vertex);
                        }
                        break;

                    case DagBuilderWellKnownErrors.PreviousVertexDataUnavailable:           
                        //No need to add it in any other lists/maps.
                        break;
                    case DagBuilderWellKnownErrors.NeedsRetry:
                        //No need to add it in any other lists/maps.
                        break;
                    case DagBuilderWellKnownErrors.Failed:
                        Interlocked.Exchange(ref this.shouldEndScheduling, 1);
                        break;                    
                }                
            }
             
        }
        
        internal void OnVertexEnded(JMVertex vertex, bool hadRetainData)
        {             
            Trace.WriteLine(string.Format("++++vertex ({0}) ended", vertex.StageVertexId));
            Interlocked.Increment(ref this.numVertexesEnded);
            lock (this)
            {
                try
                {
                    if (hadRetainData)
                    {
                        this.completed_RetainDataVertexes.Remove(vertex.GlobalVertexId);
                    }
                    else
                    {
                        this.completedVertexes.Remove(vertex.GlobalVertexId);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }
            }
        }       

        
        internal void CheckAddVertexToEligibleList(JMVertex vertex)
        {
            lock (this)
            {
                lock (vertex) 
                {
                    if (vertex.NeedsAddingToEligibleList)
                    {                        
                        //Serialize all the channel information for the vertex. 
                        //This can then be passed as filename in env variable.
                        //TODO: What if exception is thrown?
                        vertex.SerializeChannel();
                        vertex.AddChannelInfoToVertex();
                        this.AddEligibleVertex(vertex);
                    }
                }
            }
        }

        internal void CallProtectedHandleVertexCompleted(JMVertex vertex)
        {
            lock (this)
            {
                this.vertexStateManager.HandleVertexCompletedProtected(vertex);
            }
        }
        #endregion Internal
      
        #region Private

        class EdgeVertexPair
        {
            public JMEdge Edge { get; set; }
            public JMVertex Vertex { get; set; }
        }


        private JMVertex CloneFailedVertexForRerun(JMVertex failedVertex)
        {
            JMVertex clonedFailedVertex = null;
            try
            {
                clonedFailedVertex = failedVertex.CreateClone();
                //NOTE: Some cloned input vertexes might have already completed their 
                // run. We will take care of informing parent, when we try to mark them
                // as eligible to run.
            }
            catch (Exception e)
            {
                Trace.WriteLine("caught exception. Returning false from RescheduleGraphPortion()");
                Trace.WriteLine(e.ToString());
            }

            return clonedFailedVertex;
        }


        private JMVertex CreateVertexCloneWithProperInputs(JMVertex failedVertex)
        {
            JMVertex clonedFailedVertex = null;
            try
            {
                clonedFailedVertex = failedVertex.CreateCloneWithClonedInputVertexes();
                //NOTE: Some cloned input vertexes might have already completed their 
                // run. We will take care of informing parent, when we try to mark them
                // as eligible to run.
            }
            catch (Exception e)
            {
                Trace.WriteLine("caught exception. Returning false from RescheduleGraphPortion()");
                Trace.WriteLine(e.ToString());                
            }

            return clonedFailedVertex;
        }

        private bool RescheduleFailedVertexes()
        {
            bool result = true;
            lock (this.graph) //BUGBUG: This can be dangerous. We already have a scheduler lock before this. please revisit
            {
                foreach (JMVertex failedVertex in this.graph.FailedVertexesNeedsRetry.Values)
                {
                    Trace.WriteLine("Entered the failedVertexes in RescheduleFailedVertexes");
                    JMVertex clonedFailedVertex = this.CloneFailedVertexForRerun(failedVertex);
                    if (clonedFailedVertex == null) return false;

                    clonedFailedVertex.ReplaceSelfWithFailedVertexesOutputs(failedVertex);
                    //BUGBUG: We need to do the equivalent of below. At present only the channels are taken care of and nothing else.
                    //clonedFailedVertex.Task = failedVertex.Task;

                    //Add one count for clonedFailedVertex
                    Interlocked.Increment(ref this.numVertexesInGraph);

                    foreach (JMEdge edge in clonedFailedVertex.InputEdges.Values)
                    {
                        //This will take care of scheduling. The scheduling tread will pick this up.                            
                        lock (edge.StartVertex)
                        {
                            if (edge.StartVertex.CurrentState > JMVertexState.JMVertexState_Completed)
                            {
                                //no need to add this to eligible list. It is already finished executing
                                //Just signal the parent that this vertex is done.
                                this.vertexStateManager.NotifyParentVertexCompleted(edge.StartVertex, clonedFailedVertex);
                            }                            
                        }
                    }

                    
                    if (clonedFailedVertex.InputEdges.Count() == 0)
                    {
                        CheckAddVertexToEligibleList(clonedFailedVertex);
                        
                    }

                    //Make sure the vertex got added to eligible list.
                    System.Diagnostics.Debug.Assert(clonedFailedVertex.IsScheduledToExecute == 1);
                }

                this.graph.FailedVertexesNeedsRetry.Clear();
            }

            return result;
        }

        //TODO: We need to refact this part of the code.
        private bool RescheduleGraphPortion()
        {
            //we have a scheduler lock here.
            lock (this.graph) //BUGBUG: This can be dangerous. We already have a scheduler lock before this. please revisit
            {
                //TODO: As an additional feature, may be we just only allow N % of the graph to be rerun. Once we are
                // past that N %, probably the graph is having trouble completing. Its better to give up, than spend
                // time re-running a large portion of the graph.

                foreach (JMVertex failedVertex in this.graph.FailedVertexesMissingInputdata.Values)
                {
                    Trace.WriteLine("Entered the failedVertexes in RescheduleGraphPortion");
                    //this is the vertex that failed execution due to missing data. 
                    //We will reschedule all the inputs and this vertex again.

                    //because it is a DAG, we get a naturaly lock sequence. We will not deadlock with anybody                   
                    List<EdgeVertexPair> listOfWork = new List<EdgeVertexPair>();
                    lock (failedVertex)
                    {
                        JMVertex clonedFailedVertex = this.CreateVertexCloneWithProperInputs(failedVertex);
                        if (clonedFailedVertex == null) return false;
                     
                        clonedFailedVertex.ReplaceSelfWithFailedVertexesOutputs(failedVertex);
                        //Add one count for clonedFailedVertex
                        Interlocked.Increment(ref this.numVertexesInGraph);

                        //This vertex might have no input vertexes (Stage 0 vertex)
                        if (clonedFailedVertex.InputEdges.Count() == 0)
                        {
                            this.AddEligibleVertex(clonedFailedVertex);
                        }
                        
                        //now lets add the cloned vertexes to the eligible list
                        //But we have to be careful. Someother vertex might also come and do the same thing
                        //We need to make sure the current vertex is not already in the eligible list, and that
                        //it has not already finished executing.
                        foreach (JMEdge edge in clonedFailedVertex.InputEdges.Values)
                        {
                            //This will take care of scheduling. The scheduling tread will pick this up.                            
                            lock (edge.StartVertex)
                            {
                                if (edge.StartVertex.CurrentState > JMVertexState.JMVertexState_Completed)
                                {
                                    //no need to add this to eligible list. It is already finished executing
                                    //Just signal the parent that this vertex is done.
                                    this.vertexStateManager.NotifyParentVertexCompleted(edge.StartVertex, clonedFailedVertex);
                                }
                                //Now we need to figure if this vertex is getting scheduled at this moment.
                                if (edge.StartVertex.IsScheduledToExecute != 1)
                                {
                                    this.AddEligibleVertex(edge.StartVertex);
                                    Interlocked.Increment(ref this.numVertexesInGraph);
                                }
                            }
                        }
                    }

                }

                this.graph.FailedVertexesMissingInputdata.Clear();

            }
            return true;
        }       
        

        private void DumpCompletedVertexIds(ref DateTime lastDumpTime)
        {
            string completedVertexIds = "";
            foreach (long vertexId in this.completedVertexes.Keys)
            {
                completedVertexIds += vertexId.ToString();
                completedVertexIds += ",";
            }

            DateTime current = DateTime.UtcNow;
            //TODO: Make it configurable
            if (current.Subtract(lastDumpTime).TotalSeconds >= 30)
            {
                Trace.WriteLine(string.Format("Still not completed tasks = {0}", this.GetAllNonCompletedTasks()));
                Trace.WriteLine(string.Format("completed Vertexes = {0}", completedVertexIds));
                lastDumpTime = current;
            }
        }

        private string GetAllNonCompletedTasks()
        {
            string taskIds = "";
            foreach (JMVertex vertex in this.graph.GraphVertexes)
            {
                if (vertex.CurrentState != JMVertexState.JMVertexState_Invalid)
                {
                    taskIds += vertex.StageVertexId;
                    taskIds += "'";
                }
            }
            return taskIds;
        }
        /*
        private void GetInitialReadyList()
        {
            Dictionary<long, List<JMVertex>> eligibleVertexes = this.graph.FilterInitialRunVertexes();
            int iter = 0;
            lock (this)
            {
                while (true)
                {
                    bool shouldContinue = false;
                    for (int i = 0; i < eligibleVertexes.Count; i++)
                    {
                        if (eligibleVertexes.ElementAt(i).Value.Count > iter)
                        {
                            JMVertex vertex = eligibleVertexes.ElementAt(i).Value.ElementAt(iter);
                            //this.readyVertexes.Add(vertex.GlobalVertexId, vertex);
                            this.CheckAddVertexToEligibleList(vertex);
                            shouldContinue = true;
                        }
                    }
                    if (shouldContinue == false) break;
                    iter++;
                }
            }
        }
         * */

        private void GetInitialReadyList()
        {
            Dictionary<long, List<JMVertex>> eligibleVertexes = this.graph.FilterInitialRunVertexes();            
            lock (this)
            {
                foreach (List<JMVertex> vertexList in eligibleVertexes.Values)
                {
                    foreach (JMVertex vertex in vertexList)
                    {
                        this.CheckAddVertexToEligibleList(vertex);
                    }
                }               
            }
        }
        #endregion Private

        #region VarsAndProps
        JobGraph graph;
        
        Dictionary<long, JMVertex> readyVertexes;
        Dictionary<long, JMVertex> runningVertexes;
        Dictionary<long, JMVertex> completedVertexes;
        Dictionary<long, JMVertex> completed_RetainDataVertexes;
        ManualResetEvent scheduleWaitEvent;
        JMVertexStateManager vertexStateManager;
        int numVertexesInGraph;
        int numVertexesEnded;
        internal static TimeSpan VertexFileRetentionPeriod { get; private set; }
        Timer increaseVertexFileRetentionTimer;

        int shouldEndScheduling;
        #endregion VarsAndProps
    }
}
