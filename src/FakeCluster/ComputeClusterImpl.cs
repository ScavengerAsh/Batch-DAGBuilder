using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DAGBuilderInterface;

namespace FakeCluster
{
    internal class CommandLineParser
    {
        public CommandLineParser(string cmdLine)
        {
            this.CommandLine = cmdLine;
            this.Parse();
        }

        private void Parse()
        {
            if(string.IsNullOrEmpty(this.CommandLine)) return;

            string[] splitStrings = CommandLine.Split(new char[] { ' ' });
            foreach (string str in splitStrings)
            {
                int idx = str.IndexOf('=');
                if (idx == -1)
                { 
                    int optionIdx = str.IndexOf('-');
                    if(optionIdx != -1)
                    {
                        string option = str.Substring(optionIdx + 1);
                        flags.Add(option, option);
                    }
                }
                else
                {
                    string[] optionValuePair = str.Split(new char[] { '=' });
                    if(optionValuePair.Count() == 2)
                    {
                        int optionIdx = optionValuePair[0].IndexOf('-');
                        nameValues.Add(optionValuePair[0].Substring(optionIdx + 1), optionValuePair[1]);
                    }
                }
            }
        }
        Dictionary<string, string> flags = new Dictionary<string, string>();
        public Dictionary<string, string> Flags { get { return flags; } }
        Dictionary<string, string> nameValues = new Dictionary<string, string>();
        public Dictionary<string, string> NameValues { get { return nameValues; } }
        private string CommandLine { get; set; }
    }

    /// <summary>
    /// Affinity Info
    /// </summary>
    public class MyFakeAffinityInfo : ComputeTaskAffinityInfo
    {
        System.Collections.Specialized.NameValueCollection nameValCol = new System.Collections.Specialized.NameValueCollection();

        /// <summary>
        /// Get/Set the affinity Ids
        /// </summary>
        public System.Collections.Specialized.NameValueCollection AffinityIds
        {
            get
            {
                return nameValCol;
            }
            set
            {
                nameValCol = value;
            }
        }
    }

    /// <summary>
    /// Fake ComputeTask Constraints.
    /// </summary>

    public class MyFakeComputeTaskConstraints : ComputeTaskConstraints
    {

        public int MaxTaskRetryCount
        {
            get;
            set;
        }

        public TimeSpan? MaxWallClockTime
        {
            get;
            set;
        }

        public TimeSpan? RetentionTime
        {
            get;
            set;
        }
    }
    /*
    public class MyFakeSession : ComputeSession
    {
        public MyFakeSession() { }
    }*/
    public class MyFakeTaskExecutionInfo : ComputeTaskExecutionInfo
    {
        public DateTime? StartTime
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public string AffinityId
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public DateTime? EndTime
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        private int? exitCode = 0;
        public int? ExitCode
        {
            get
            {
                return exitCode;
            }
            set
            {
                exitCode = value;
            }
        }

        public ComputeTaskSchedulingError SchedulingError
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public string TVMUrl
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public int RetryCount
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public DateTime? LastRetryTime
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public int RequeueCount
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public DateTime? LastRequeueTime
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public long PreemptionCount
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public DateTime? LastPreemptionTime
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }
    }
    public class MyFakeTaskSchedulingInfo : ComputeTaskSchedulingInfo
    {

        public void CopyTo(ComputeTaskSchedulingInfo to)
        {
            to.AffinityInfo = this.AffinityInfo;
            to.CommandLine = this.CommandLine;
            to.Constraints = this.Constraints;
            to.EnvironmentSettings = this.EnvironmentSettings;
            if (this.Files != null)
                to.Files.AddRange(this.Files);
            to.Name = this.Name;
            to.TVMType = this.TVMType;            
        }
        public string CommandLine
        {
            get;
            set;
        }

        private List<ComputeTaskResourceFile> files = new List<ComputeTaskResourceFile>();
        public List<ComputeTaskResourceFile> Files
        {
            get;
            private set;
        }


        System.Collections.Specialized.NameValueCollection envSettings = new System.Collections.Specialized.NameValueCollection();
        public System.Collections.Specialized.NameValueCollection EnvironmentSettings
        {
            get
            {
                return envSettings;
            }
            set
            {
                envSettings = value;
            }
        }
        /*
        public System.Collections.Specialized.NameValueCollection EnvironmentSettings
        {
            get;
            set;            
        }*/

        MyFakeAffinityInfo ai = new MyFakeAffinityInfo();
        public ComputeTaskAffinityInfo AffinityInfo
        {
            get{return ai;}
            set {
                if (value is MyFakeAffinityInfo)
                    ai = (MyFakeAffinityInfo)value;
            }            
        }

        public string TVMType
        {
            get;
            set;
        }

        MyFakeComputeTaskConstraints constraints = new MyFakeComputeTaskConstraints();
        public ComputeTaskConstraints Constraints
        {
            get{return constraints;}
            set
            {
                if (value is MyFakeComputeTaskConstraints)
                    constraints = (MyFakeComputeTaskConstraints)value;
            }
        }

        public string Name
        {
            get;
            set;
        }
    }
    public class MyFakeTask : ComputeTask
    {
        #region Constructor
        public MyFakeTask(long id, string taskName)
        {
            this.TaskId = id;
            this.ExecutionInfo = new MyFakeTaskExecutionInfo();
            this.TaskSchedulingInfo = new MyFakeTaskSchedulingInfo();
            this.name = taskName;
        }

        #endregion Constructor

        #region PropsAndVars

        public void RegisterCallback(ComputeTaskState waitForState, AsyncCallback callback)
        {
            lock (this)
            {                
                this.WaitForState = waitForState;
                System.Diagnostics.Debug.Assert(this.TaskStateReachedCallback == null);
                //TODO: Make it such that multiple callbacks can be registered.
                this.TaskStateReachedCallback = callback;
                if (this.TaskState >= waitForState)
                    InvokeTaskWaitCallback();
            }

        }
        private void InvokeTaskWaitCallback()
        {
            lock (this)
            {
                //Task.Factory.StartNew(() => { this.TaskStateReachedCallback(this.AsyncResult); }).Wait();
                this.TaskStateReachedCallback(this.AsyncResult);
                this.TaskStateReachedCallback = null;
            }
        }
        public Timer MyTimer { get; set; }
        public long TaskId { get; private set; }        
        public ComputeTaskState TaskState
        {
            get
            {
                lock (this)
                {
                    return state;
                }
            }
            set
            {
                lock (this)
                {
                    state = value;
                    if (this.TaskStateReachedCallback != null)
                    {
                        if (this.TaskState >= this.WaitForState)
                            InvokeTaskWaitCallback();
                    }
                }
            }
        }

        string Url
        {
            set
            {
                this.url = value;
            }
        }

        
        DateTime? CreationTime
        {
            
            set
            {
                value = this.creationTime;
            }
             
        }

        public MyFakeAsyncResult AsyncResult { get; set; }
        private ComputeTaskState state;
        private AsyncCallback TaskStateReachedCallback { get; set; }
        private ComputeTaskState WaitForState { get; set; }


        #endregion PropsAndVars

        #region ComputeTaskInterface

        public bool RunElevated
        {
            get;
            set;
        }

        public ComputeTaskSchedulingInfo TaskSchedulingInfo { get; private set; }        
        
        ComputeTaskExecutionInfo ExecutionInfo
        {
            get;            
            set;            
        }

        DateTime? creationTime = DateTime.UtcNow;
        DateTime? ComputeTask.CreationTime
        {
            get
            {
                return creationTime;
            }
            /*
            set
            {
                throw new NotImplementedException();
            }
            */
        }

        string eTag = "";
        string ComputeTask.ETag
        {
            get
            {
                return eTag;
            }
            /*
            set
            {
                throw new NotImplementedException();
            }
             * */
        }

        DateTime? ComputeTask.LastModified
        {
            get
            {
                throw new NotImplementedException();
            }
            /*
            set
            {
                throw new NotImplementedException();
            }
             * */
        }

        /// <summary>
        /// Gets the current state of the task. 
        /// </summary>
        ComputeTaskState ComputeTask.State
        {
            get
            {
                return this.TaskState;
            }
        }

        DateTime? ComputeTask.StateTransitionTime
        {
            get
            {
                throw new NotImplementedException();
            }

            /*
            set
            {
                throw new NotImplementedException();
            }
             * */
        }

        ComputeTaskState? ComputeTask.PreviousState
        {
            get
            {
                throw new NotImplementedException();
            }

            /*
            set
            {
                throw new NotImplementedException();
            }
             * */
        }

        DateTime? ComputeTask.PreviousStateTransitionTime
        {
            get
            {
                throw new NotImplementedException();
            }

            /*
            set
            {
                throw new NotImplementedException();
            }
             * */
        }

        ComputeTaskExecutionInfo ComputeTask.ExecutionInfo
        {
            get
            {
                return this.ExecutionInfo;
            }

            /*
            set
            {
                throw new NotImplementedException();
            }
             * */
        }

        ComputeTaskStatistics ComputeTask.Statistics
        {
            get
            {
                throw new NotImplementedException();
            }
            /*
            set
            {
                throw new NotImplementedException();
            }
             * */
        }

        string url = "";
        string ComputeTask.Url
        {
            get
            {
                return url;
            }
            /*
            set
            {
                throw new NotImplementedException();
            }
             * */
        }

        string name = "";
        string ComputeTask.Name
        {
            get
            {
                return name;
            }
            /*
            set
            {
                throw new NotImplementedException();
            }
             * */
        }
        #endregion ComputeTaskInterface
    }

    public class MyFakeAsyncResult : IAsyncResult
    {
        public object AsyncState
        {
            get { throw new NotImplementedException(); }
        }

        public WaitHandle AsyncWaitHandle
        {
            get { throw new NotImplementedException(); }
        }

        public bool CompletedSynchronously
        {
            get;
            set;
        }

        public bool IsCompleted
        {
            get;
            set;
        }
    }
    //TODO: Figure out a way to create a compute cluster on the fly. Use reflection?
    public class MyFakeComputeCluster : ComputeCluster
    {
        public MyFakeComputeCluster()
        {
            this.Tasks = new Dictionary<long, MyFakeTask>();
        }        
        private string componentName;
        private string configurationFile;
        //MyFakeSession session;
        long id = 0;
        public Dictionary<long, MyFakeTask> Tasks { get; set; }

        public void ClusterInitialize(string componentName, string configurationFile)
        {
            this.componentName = componentName;
            this.configurationFile = configurationFile;
            //this.session = new MyFakeSession();
            this.Tasks = new Dictionary<long, MyFakeTask>();
        }

        public void ClusterDeInitialize(string reason)
        {
            System.Diagnostics.Trace.WriteLine("Cluster deinit");
        }

        /*
        public ComputeSession OpenSession(ComputeSessionParams sessionParams)
        {
            return this.session;
        }

        public void CloseSession(ComputeSession session)
        {
            throw new NotImplementedException();
        } */       
        
        public ComputeTask CreateNewTask(/*ComputeSession session,*/ string taskName)
        {
            lock (this)
            {
                Interlocked.Increment(ref id);
                MyFakeTask task = new MyFakeTask(id, id.ToString());
                this.Tasks.Add(task.TaskId, task);
                return task;
            }
        }

        public ComputeTask GetCurrentTask(/*ComputeSession session*/)
        {
            throw new NotImplementedException();
        }

        public void CancelTask(ComputeTask task)
        {
            throw new NotImplementedException();
        }
       
        /// <summary>
        /// The Fake cluster can be used to simulate various task failures, to determine
        /// how the system handles failures in terms of retries etc.
        /// </summary>
        /// <param name="target"></param>
        public void MyTimerCallback(Object target)
        {
            ComputeTask task = (ComputeTask)target;
            MyFakeTask mft = task as MyFakeTask;
            CommandLineParser parser = new CommandLineParser(task.TaskSchedulingInfo.CommandLine);
            if (parser.NameValues.Keys.Contains("ReadFileReturn", StringComparer.InvariantCultureIgnoreCase))
            {
                this.readFileReturnParam = parser.NameValues["ReadFileReturn"];
            }
            bool shouldFail = parser.Flags.Keys.Contains("Fail");
            int numTimesToFailBeforePass = 0;
            if (parser.NameValues.Keys.Contains("FailCount"))
            {
                numTimesToFailBeforePass = Convert.ToInt16(parser.NameValues["FailCount"]);
            }
            int ExitCode = 0;
            if (parser.NameValues.Keys.Contains("ExitCode"))
            {
                ExitCode = Convert.ToInt16(parser.NameValues["ExitCode"]);
            }
            if (task.TaskSchedulingInfo.CommandLine != null && shouldFail && numTimesFailed < numTimesToFailBeforePass)
            {
                System.Diagnostics.Trace.WriteLine("Failing the Task. CommandLine = "+ task.TaskSchedulingInfo.CommandLine);
                //task.ExecutionInfo.ExitCode = DagBuilderWellKnownErrors.PreviousVertexDataUnavailable;
                task.ExecutionInfo.ExitCode = ExitCode;
                numTimesFailed++;
            }
            else
            {
                task.ExecutionInfo.ExitCode = DagBuilderWellKnownErrors.Passed;
            }
            mft.TaskState = ComputeTaskState.Completed;
        }

        public void ScheduleTask(ComputeTask task)
        {
            long sleepSec = DateTime.Now.ToFileTime() % 7;
            if (sleepSec == 0) sleepSec = 1;
            System.Diagnostics.Trace.WriteLine(String.Format("Sleeping for {0} secs", sleepSec));            
            MyFakeTask mft = task as MyFakeTask;
            //lets just reset the exit code and state
            task.ExecutionInfo.ExitCode = DagBuilderWellKnownErrors.Passed;
            mft.TaskState = ComputeTaskState.Queued;            
            mft.MyTimer = new Timer(
                MyTimerCallback,
                task,
                TimeSpan.FromSeconds(sleepSec),
                //TimeSpan.FromSeconds(2),
                new TimeSpan(Timeout.Infinite));            
        }

        public IAsyncResult BeginWaitForStateChange(ComputeTask task, ComputeTaskState waitFor, TimeSpan timeout, AsyncCallback callback, object state)
        {
            MyFakeTask faketask = task as MyFakeTask;
            faketask.AsyncResult = new MyFakeAsyncResult();
            faketask.RegisterCallback(waitFor, callback);
            return faketask.AsyncResult;
        }

        public bool EndWaitForStateChange(IAsyncResult result)
        {
            return true;
        }
        
        int numTimesFailed = 0;
        string readFileReturnParam = "";
    }
    
}
