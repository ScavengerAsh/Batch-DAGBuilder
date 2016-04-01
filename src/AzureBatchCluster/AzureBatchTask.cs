using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
//using Microsoft.WindowsAzure.StorageClient;
using Microsoft.Azure.Batch;

using Microsoft.Azure;
using DAGBuilderInterface;
using System.Threading;
using System.Diagnostics;
using Microsoft.Azure.Batch.Auth;


namespace AzureBatchCluster
{
    internal enum AzureTaskSchedulingState
    { 
        Active,
        QueuedForScheduling,
        Scheduled,
        ScheduleFailed
    }

    /// <summary>
    /// The results for the wait for state change of a task
    /// </summary>
    internal class AzureTaskStateResult: IAsyncResult
    {
        internal AzureTaskStateResult(object userState)
        {
            this.userState = userState;
        }

        public object AsyncState
        {
            get { throw new NotImplementedException(); }
        }

        public System.Threading.WaitHandle AsyncWaitHandle
        {
            get
            {
                lock (this)
                {
                    if (waitHandle == null)
                        waitHandle = new ManualResetEvent(false);
                    
                    //Just in case things have already completed
                    if (this.IsCompleted)
                    {
                        waitHandle.Set();
                    }
                }
                return waitHandle;
            }
        }

        public bool CompletedSynchronously { get; internal set; }

        public bool IsCompleted { get; internal set; }

        internal void SetCompleted(bool isSync)
        {
            lock (this)
            {
                this.IsCompleted = true;
                this.CompletedSynchronously = isSync;
                if (waitHandle != null)
                {
                    waitHandle.Set();
                }
            }
        }

        private object userState;
        ManualResetEvent waitHandle;
    }

    class AzureTaskWaiter
    {
        public AzureTaskWaiter(AsyncCallback callback) 
        {
            this.Callback = callback;
        }
        public AsyncCallback Callback { get; private set; }
    }

    class TaskStateWaiter : AzureTaskWaiter
    {
        public TaskStateWaiter(AsyncCallback callback, AzureTaskStateResult result):base(callback)
        {            
            this.Result = result;
        }

        public void SetResultAndInvokeCallback(bool isSync)
        {
            this.Result.SetCompleted(isSync);
            ThreadPool.QueueUserWorkItem(this.CallTheCallBack);
            //this.Callback(this.Result);            
        }

        internal void CallTheCallBack(object state)
        {
            this.Callback(this.Result);            
        }
        public AzureTaskStateResult Result { get; set; }
    }

    /// <summary>
    /// Represents the Azure Batch cluster implementation of the ComputeTask Interface
    /// </summary>
    public class AzureBatchTask : ComputeTask
    {
        #region constructor
        internal AzureBatchTask(TaskClusterConfiguration config, string name)            
        {
            //base.Task.TVMType = TVMType.Dedicated;                   
            tenantUri = new Uri(config.TenantUri);
            //taskCredentials = new BatchSharedKeyCredential(config.AccountName, config.Key);
            //wiName = config.WorkItemName;
            jobName = config.JobName;
            taskName = name;
        }

        #endregion constructor

        #region publicMethods
        
        #endregion publicMethods

        #region publicproperties
        /// <summary>
        /// The command line used to launch the task
        /// </summary>
        public string CommandLine { get; set; }
        #endregion publicproperties
        #region ComputeTask InterfaceMethods
        /// <summary>
        /// Should the task be run on the compute node in Elevated mode
        /// </summary>
        public bool RunElevated
        {
            get;
            set;
        }

        private ComputeTaskSchedulingInfo taskSchedulingInfo = new AzureBatchSchedulingInfo();
        /// <summary>
        /// Gets the tasks scheduling info.
        /// </summary>
        public ComputeTaskSchedulingInfo TaskSchedulingInfo
        {
            get
            {
                return this.taskSchedulingInfo;

            }
            set
            {
                this.taskSchedulingInfo = value;                
            }
        }

        /// <summary>
        /// The task URL
        /// </summary>
        public string Url
        {
            get 
            {
                //BUGBUG: May be we should return the full task URL. 
                // For now its ok as this is what is used to get to files.
                return this.jobName + ":" + this.taskName;
                //return this.taskName;
            }
        }

        /// <summary>
        /// Task creation time
        /// </summary>
        public DateTime? CreationTime
        {
            get { throw new NotImplementedException(); }
        }

        public string ETag
        {
            get { throw new NotImplementedException(); }
        }

        public DateTime? LastModified
        {
            get { throw new NotImplementedException(); }
        }

        private ComputeTaskState state = ComputeTaskState.None;

        /// <summary>
        /// Task state
        /// </summary>
        public ComputeTaskState State
        {
            get { return state; }
            internal set
            {
                lock (this)
                {
                    this.state = value;
                    NotifyWaiters();
                }
            }
        }

        public DateTime? StateTransitionTime
        {
            get { throw new NotImplementedException(); }
        }

        public ComputeTaskState? PreviousState
        {
            get
            {
                return this.state;
            }
        }

        public DateTime? PreviousStateTransitionTime
        {
            get { throw new NotImplementedException(); }
        }

        public ComputeTaskExecutionInfo ExecutionInfo
        {
            get{return this.taskResponseExecutionInfo;}
            set {throw new NotImplementedException();}
        }

        public ComputeTaskStatistics Statistics
        {
            get { throw new NotImplementedException(); }
        }

        public string Name
        {
            get
            {
                return taskName;
            }            
        }

        #endregion ComputeTask InterfaceMethods

        #region Waiters

        void DoCallbackNow(TaskStateWaiter waiter, bool isSync)
        {
            waiter.Result.SetCompleted(isSync);
            waiter.Callback(waiter.Result);
        }

        //this gets called in a lock.
        internal void NotifyWaiters()
        {            
            if (this.taskStateWaiters == null)
            {
                Trace.WriteLine(String.Format("Task State changed. Task - {0}), but no waiters to wait on", this.Name));
                return;
            }

            foreach (ComputeTaskState waitState in this.taskStateWaiters.Keys)
            {
                //When we come here we already set the state to new state
                if (this.state < waitState)
                {
                    Trace.WriteLine(String.Format("Not reached desired state(Task {0}. Current State = {1}, Waiting For = {2}", this.Name, this.state, waitState));
                    continue; //not reached the state
                }

                List<TaskStateWaiter> taskStateWaiters = this.taskStateWaiters[waitState];
                if (taskStateWaiters == null) continue; //no waiter for this state

                foreach (TaskStateWaiter taskStateWaiter in taskStateWaiters)
                {
                    Trace.WriteLine(String.Format("*****(ThreadID - {0}) - Reached desired state(Task {1}). Current State = {2}, Waiting For = {3}", Thread.CurrentThread.ManagedThreadId, this.Name, this.state, waitState));
                    taskStateWaiter.SetResultAndInvokeCallback(false);
                }
                //now that waiters are notified, remove them from further notification
                this.taskStateWaiters[waitState].Clear();
            }

        }


        internal IAsyncResult BeginWaitForTaskState(ComputeTaskState waitFor, TimeSpan timeout, AsyncCallback callback, object userState)
        {
            AzureTaskStateResult result = null;
            bool isInplaceCallback = false;
            result = new AzureTaskStateResult(userState);
            var waiter = new TaskStateWaiter(callback, result);
            lock (this)
            {
                if (taskStateWaiters == null)
                {
                    this.taskStateWaiters = new Dictionary<ComputeTaskState, List<TaskStateWaiter>>();
                }
                
                List<TaskStateWaiter> waitersList = null;
                if (this.State >= waitFor)
                {
                    //DoCallbackNow(waiter, true); //BUGBUG: This needs to be outside of lock.
                    isInplaceCallback = true;
                }
                else
                {
                    if (this.taskStateWaiters.ContainsKey(waitFor))
                    {
                        waitersList = this.taskStateWaiters[waitFor];
                    }
                    else
                    {
                        waitersList = new List<TaskStateWaiter>();
                        taskStateWaiters[waitFor] = waitersList;
                    }
                    waitersList.Add(waiter);
                }
            }
            if (isInplaceCallback)
            {
                DoCallbackNow(waiter, true); //BUGBUG: This needs to be outside of lock.
            }
            return result;
        }

        internal bool EndWaitForStateChange(IAsyncResult result)
        {
            return (result as AzureTaskStateResult).IsCompleted;
            //throw new NotImplementedException();
        }

        
        #endregion Waiters

        #region InternalVariables        
        private AzureBatchExecutionInfo taskResponseExecutionInfo;
        internal TaskExecutionInformation TaskResponseExecutionInfo 
        {
            set
            {
                lock (this)
                {
                    if (this.taskResponseExecutionInfo == null)
                    {
                        this.taskResponseExecutionInfo = new AzureBatchExecutionInfo();
                    }
                    this.taskResponseExecutionInfo.Copy(value);
                }
            }
        }
        #endregion InternalVariables
        #region PrivateVariables
        Uri tenantUri;
        //BatchSharedKeyCredentials taskCredentials;
        //string wiName;
        string jobName;
        string taskName;
        internal AzureTaskSchedulingState InternalSchedulingState { get; set; }
        Dictionary<ComputeTaskState, List<TaskStateWaiter> > taskStateWaiters = null;
        //AddTaskRequest addTaskRequest;
        //AzureTaskStateResult asyncResult;
        #endregion PrivateVariables
    }
}
