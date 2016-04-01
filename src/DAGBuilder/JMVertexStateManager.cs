using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading;
using System.IO;
using DAGBuilderInterface;

namespace DAGBuilder
{
    internal enum JMVertexState
    {
        JMVertexState_Invalid,
        JMVertexState_Unscheduled,
        JMVertexState_ReadyToSchedule,
        JMVertexState_Scheduling,
        JMVertexState_Scheduled,
        JMVertexState_Completed,
        JMVertexState_RetainData,
        JMVertexState_Ended
    }

    internal class JMVertexStateManager
    {
        #region Constructor
        internal JMVertexStateManager(Scheduler scheduler, ComputeCluster cluster)
        {
            this.computeCluster = cluster;            
            //this.computeCluster = Cluster.GetInstance().XcCluster;            
            this.scheduler = scheduler;            
        }

        #endregion Constructor

        #region public
        public void HandleVertexStateTransitions(JMVertex vertex)
        {
            try
            {
                lock (this)
                {
                    if (vertex.CurrentState != JMVertexState.JMVertexState_Unscheduled)
                    {
                        Trace.WriteLine(string.Format("Vertex ({0}) not is correct state ({1})", vertex.StageVertexId, vertex.CurrentState));
                    }
                    System.Diagnostics.Debug.Assert(vertex.CurrentState == JMVertexState.JMVertexState_Unscheduled);
                }
                //Task.Factory.StartNew(CallMoveNextState, vertex);
                CallMoveNextState(vertex);
            }
            catch (Exception e)
            {
                Trace.WriteLine(e.ToString());
            }
            
        }

        #endregion public

        #region private       

        private void CallMoveNextState(Object state)
        {
            MoveNextState((JMVertex)state);
        }        

        internal void NotifyParentVertexCompleted(JMVertex completedChild, JMVertex parent)
        {
            foreach (JMEdge output in completedChild.OutputEdges.Values)
            {
                if (output.EndVertex.GlobalVertexId == parent.GlobalVertexId)
                {
                    output.EndVertex.SignalInputVertexCompleted(completedChild.GlobalVertexId);
                    this.scheduler.CheckAddVertexToEligibleList(output.EndVertex);
                }
            }
        }

        internal void HandleVertexCompletedProtected(JMVertex vertex)
        {
            //When we enter here, we are sure the scheduler is locked.
            try
            {                
                //Trace.WriteLine(string.Format("@@@@@@@@@@@@@@Received Completed Vertex ({0}:{1}:{2})************", vertex.ParentStageManager.StageName, vertex.StageVertexId, ((MyFakeTask)vertex.Task).TaskId));
                //NOTE: The vertex does not move to next state if there are failures.
                //TODO: Probably would be good to still move the vertex to ended, so that cleanup can be performed.
                int errorCode = DagBuilderWellKnownErrors.Passed;
                vertex.CurrentState = JMVertexState.JMVertexState_Completed;
                
                if (!vertex.IsCompletedWithoutError)
                {
                    //Console.WriteLine("Vertex actually failed!!");
                    //Trace.WriteLine(string.Format("@@@@@@@@@@@@@@Completed(FAILED) Vertex ({0}:{1}:{2})************", vertex.ParentStageManager.StageName, vertex.StageVertexId, ((MyFakeTask)vertex.Task).TaskId));                    
                    //readjust this vertexes state to unscheduled.
                    //vertex.PreviousState = JMVertexState.JMVertexState_Invalid;
                    //vertex.CurrentState = JMVertexState.JMVertexState_Unscheduled;

                    if ((vertex.Task.ExecutionInfo.ExitCode != null) &&
                        (vertex.Task.ExecutionInfo.ExitCode == DagBuilderWellKnownErrors.PreviousVertexDataUnavailable))
                    {
                        //We need to re-run prevoious stage vertexes. So add it to the list.The scheduler will take care of it
                        vertex.ParentStageManager.Parent.AddFailedVertexMissingInputData(vertex);

                        errorCode = DagBuilderWellKnownErrors.PreviousVertexDataUnavailable;
                    }
                    else
                    {
                        if (vertex.VertexRetryCount < GraphBuilderSettings.MaxVertexRetryCount)
                        {
                            vertex.VertexRetryCount += 1;
                            vertex.ParentStageManager.Parent.AddFailedVertexNeedsRetry(vertex);
                            errorCode = DagBuilderWellKnownErrors.NeedsRetry;
                        }
                        else
                        {
                            errorCode = DagBuilderWellKnownErrors.Failed;
                        }
                    }
                }
                else
                {
                    //Inform the dependents that this vertex has finished execution. 
                    //So that the scheduler can get scheduled.
                    //Trace.WriteLine(string.Format("@@@@@@@@@@@@@@Completed Vertex ({0}:{1}:{2})************", vertex.ParentStageManager.StageName, vertex.StageVertexId, ((MyFakeTask)vertex.Task).TaskId));                    
                    try
                    {
                        foreach (JMEdge output in vertex.OutputEdges.Values)
                        {
                            output.EndVertex.SignalInputVertexCompleted(vertex.GlobalVertexId);
                            this.scheduler.CheckAddVertexToEligibleList(output.EndVertex);
                        }
                        Trace.WriteLine(string.Format("Vertex ({0}) calling EndVertex.", vertex.StageVertexId));
                        //TODO: Take care of retain data                       
                        //vertex.PreviousState = JMVertexState.JMVertexState_Completed;
                        //Task.Factory.StartNew(CallMoveNextState, vertex);
                    }
                    catch (Exception e)
                    {
                        //TODO: What to do? Fail the Job?
                        Trace.WriteLine(e.ToString());
                    }
                }                
                //Inform scheduler of vertex completion. So that it can do its book keeping.
                this.scheduler.OnVertexCompleted(vertex, errorCode, (vertex.OutputEdges.Count > 0));

                vertex.PreviousState = JMVertexState.JMVertexState_Completed;
                Task.Factory.StartNew(CallMoveNextState, vertex);
   
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex.ToString());
                Console.WriteLine(ex.ToString());
            }

            
        }
       

        private void MoveNextState(JMVertex vertex)
        {
            //TODO: Do we need to do the validation here for whether all the properties 
            // are propertly filled or not.            
            try
            {
                switch (vertex.PreviousState)
                {
                    case JMVertexState.JMVertexState_Invalid:
                        vertex.CurrentState = JMVertexState.JMVertexState_Unscheduled;                        
                        //TODO: Do something related to vertex start
                        this.scheduler.OnVertexStart(vertex);
                        vertex.PreviousState = JMVertexState.JMVertexState_Unscheduled;
                        MoveNextState(vertex);
                        break;
                        //Task.Factory.StartNew(CallMoveNextState, vertex);
                        //break;

                    case JMVertexState.JMVertexState_Unscheduled:
                        vertex.CurrentState = JMVertexState.JMVertexState_Scheduling;
                        //this.computeCluster.ScheduleTask(vertex.Task);
                        vertex.ParentStageManager.StageComputeClusterToUse.ScheduleTask(vertex.Task);
                        this.scheduler.OnVertexScheduling(vertex);
                        vertex.PreviousState = JMVertexState.JMVertexState_Scheduling;
                        //Task.Factory.StartNew(CallMoveNextState, vertex);
                        //break;
                        MoveNextState(vertex);
                        break;

                    case JMVertexState.JMVertexState_Scheduling:
                        vertex.CurrentState = JMVertexState.JMVertexState_Scheduled;
                        //Trace.WriteLine(string.Format("Scheduling Task. Vertex ({0}:{1}:{2})************", vertex.ParentStageManager.StageName, vertex.StageVertexId, ((MyFakeTask)vertex.Task).TaskId));
                        this.scheduler.OnVertexScheduled(vertex);                        
                        //this.computeCluster.BeginWaitForStateChange(vertex.Task, ComputeTaskState.Completed, TimeSpan.FromSeconds(5),
                        vertex.ParentStageManager.StageComputeClusterToUse.BeginWaitForStateChange(vertex.Task, ComputeTaskState.Completed, TimeSpan.FromSeconds(5),
                            (antecedent) =>
                            {
                                try
                                {
                                    Trace.WriteLine(String.Format("(Thread ID - ({0}) - {1})Start BeginWaitForStateChange Handler. Task - {2}", Thread.CurrentThread.IsThreadPoolThread, Thread.CurrentThread.ManagedThreadId, vertex.Task.Name));
                                    System.Diagnostics.Debug.Assert(vertex.Task.State == ComputeTaskState.Completed);
                                    vertex.PreviousState = JMVertexState.JMVertexState_Scheduled;
                                    Task.Factory.StartNew(CallMoveNextState, vertex);
                                    Trace.WriteLine(String.Format("Done BeginWaitForStateChange Handler. Task - {0}", vertex.Task.Name));
                                }
                                catch (Exception e)
                                {
                                    Trace.WriteLine(e.ToString());
                                }
                            }, null);
                        break;

                    case JMVertexState.JMVertexState_Scheduled:
                        this.scheduler.CallProtectedHandleVertexCompleted(vertex);
                        //this.HandleVertexCompleted(vertex);
                        break;

                    case JMVertexState.JMVertexState_Completed:
                        vertex.CurrentState = JMVertexState.JMVertexState_Ended;
                        //Mark the numvertex done count.
                        Trace.WriteLine(string.Format("Vertex ({0}) Ended.", vertex.StageVertexId));
                        this.scheduler.OnVertexEnded(vertex, (vertex.OutputEdges.Count > 0));
                        break;
                }
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex.ToString());
                Console.WriteLine(ex.ToString());
            }

        }

        #endregion private

        #region VarsAndProps

        private ComputeCluster computeCluster;
        private Scheduler scheduler;        
        #endregion VarsAndProps
    }
}
