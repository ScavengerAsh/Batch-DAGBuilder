using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

using Microsoft.Azure;

using DAGBuilderInterface;
using System.Threading;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.FileStaging;

using System.Net;
using System.IO;
using Microsoft.WindowsAzure.Storage.Blob;


namespace AzureBatchCluster
{
    class AzureTaskBootupHelper
    {
        

        private BatchSharedKeyCredentials batchCredentials;
        private BatchClient batchClient;

        public string BootUpWorkItemAndJob(string batchTenantUrl, string accountName, string accountKey, string wiName, string poolName, JobInitializationChoices jobInitParams= JobInitializationChoices.CreateNewDeleteOld)
        {
            TaskClusterConfiguration config = new TaskClusterConfiguration();
            config.TenantUri = batchTenantUrl;
            config.AccountName = accountName;
            config.Key = accountKey;
            //config.WorkItemName = wiName;
            config.PoolName = poolName;
            config.JobInitializationParams = jobInitParams;            
            return BootUpWorkItemAndJob(config);

        }

        private bool DoesWorkItemExist(string wiName)
        {
            try
            {
                var job = this.batchClient.JobOperations.GetJob(wiName);
                if (job.Id.Equals(wiName, StringComparison.InvariantCultureIgnoreCase)) return true;
                else return false;
            }
            catch (Exception ae)
            {
                BatchException te = ae.InnerException as BatchException;
                if (te == null) throw;

                if (te.RequestInformation.HttpStatusCode.HasValue && te.RequestInformation.HttpStatusCode.Value == HttpStatusCode.NotFound)
                {
                    Console.WriteLine("WorkItem {0} does not exist", wiName);
                    return false;
                }
                else
                {
                    Console.WriteLine("Got task exception. But not with status code 404. Please Debug \r\n - {0}", te.Message);
                    throw te;
                }
            }            
        }
        
        public string BootUpWorkItemAndJob(TaskClusterConfiguration config)
        {
            //this.UploadFiles(config);
            
            lock (this)
            {
                if(this.batchCredentials == null) this.batchCredentials = new BatchSharedKeyCredentials(config.TenantUri, config.AccountName, config.Key);
                if (this.batchClient == null)
                {
                    this.batchClient = BatchClient.Open(this.batchCredentials);                    
                }
            }

            
            //BatchCredentials creds = new BatchSharedKeyCredential(config.AccountName, config.Key);
            string jobName = config.JobName;

            if ((config.JobInitializationParams == JobInitializationChoices.CreateNewIfNotExist))
            {
                if (DoesWorkItemExist(jobName))
                {
                    return jobName;
                }
            }

            if (config.JobInitializationParams == JobInitializationChoices.CreateNewWithUniqueName)
            {
                jobName = Guid.NewGuid().ToString();
            }


            if (config.JobInitializationParams == JobInitializationChoices.CreateNewDeleteOld)
            {
                Console.WriteLine("Deleting Job {0}", config.JobName);
                //DeleteWorkItemRequest delWIReq = new DeleteWorkItemRequest(config.TaskTenantUri, taskCredentials, config.WorkItemName);                

                try
                {                    
                    var job = this.batchClient.JobOperations.GetJob(config.JobName);
                    job.Delete();                    
                }
                catch (Exception e)
                {
                    BatchException te = e.InnerException as BatchException;
                    if (te == null) throw;

                    if (te.RequestInformation.HttpStatusCode.HasValue && te.RequestInformation.HttpStatusCode.Value != HttpStatusCode.NotFound)                
                    {
                        throw te;
                    }
                    else
                    {
                        Console.WriteLine("Job - {0} not found. Progressing", config.JobName);
                    }                    
                }


                Console.WriteLine("Delete Job requ accepted. Waiting for system to delete the Job completely - {0}", config.JobName);

                Stopwatch sw = Stopwatch.StartNew();

                int count = 0;
                do
                {
                    System.Threading.Thread.Sleep(1000);
                    try
                    {                        
                        var job = this.batchClient.JobOperations.GetJob(config.JobName);
                        Console.WriteLine("Waiting for Job to be deleted from the system. Iter no {0}. Elapsed time = {1} sec", count, sw.Elapsed.TotalSeconds);
                    }
                    catch (Exception e)
                    {
                        BatchException te = e.InnerException as BatchException;
                        if (te == null) throw;

                        if (te.RequestInformation.HttpStatusCode.HasValue && te.RequestInformation.HttpStatusCode.Value == HttpStatusCode.NotFound)
                        {
                            Console.WriteLine("Job - {0} not found. Delete finished. Progressing", config.JobName);
                        }
                        else
                        {
                            throw te;                            
                        }                        
                    }
                    
                    count++;
                } while (sw.Elapsed.TotalMinutes < 5);
                Console.WriteLine("Job - {0} deleted. ", config.JobName);
            }

            //if ((config.JobInitializationParams == JobInitializationChoices.CreateNewDeleteOld) || (config.JobInitializationParams == JobInitializationChoices.CreateNewWithUniqueName))
            {
                try
                {
                    Console.WriteLine("Adding new Job ({0})", jobName);
                    var job = this.batchClient.JobOperations.CreateJob(jobName, new Microsoft.Azure.Batch.PoolInformation() { PoolId = config.PoolName });
                    job.Commit();
                    return jobName;
                }
                catch (Exception e)
                {
                    throw e;
                }              
                
            }
            //return wiName;
        }        
    }

    class AzureBatchHelper
    {
        #region Constructor
        AzureBatchHelper(TaskClusterConfiguration config)
        {
            this.clusterConfig = config;
            this.addTaskHelper = new BackgroundAddTaskHelper(config, this);
            //this.taskCredentials = new BatchSharedKeyCredentials(config.TenantUri, config.AccountName, config.Key);

            var creds = new Microsoft.Azure.Batch.Protocol.BatchSharedKeyCredential(config.AccountName, config.Key);
            var restClient = new Microsoft.Azure.Batch.Protocol.BatchRestClient(creds, config.TaskTenantUri);
            var infoHdr = new System.Net.Http.Headers.ProductInfoHeaderValue("AzureBatchDAGBuilder", "1.0");
            restClient.UserAgent.Add(infoHdr);
            this.batchClient = BatchClient.Open(restClient);

            //this.batchClient = BatchClient.Open(this.taskCredentials);
            this.backgroundGetTaskStateThread = new System.Threading.Thread(new System.Threading.ThreadStart(GetTaskBackground));
            this.backgroundGetTaskStateThread.Start();
        }
        #endregion Constructor

        #region Singleton
        internal static AzureBatchHelper GetSingletonInstance(TaskClusterConfiguration config)
        {
            if (singletonAddTaskHelper == null)
            {
                lock (syncRoot)
                {
                    singletonAddTaskHelper = new AzureBatchHelper(config);
                }
            }

            return singletonAddTaskHelper;
        }


        internal static AzureBatchHelper GetNewInstance(TaskClusterConfiguration config)
        {
            return new AzureBatchHelper(config);            
        }

        #endregion Singleton

        #region BackgroundGetTaskThread

        public void GetTaskBackground()
        {
            try
            {
                while (true)
                {
                    DateTime start = DateTime.UtcNow;
                    //string nextMarker = null;
                    if (this.stopAllThreads == true)
                    {
                        Trace.WriteLine("GetTaskBackground thread is stopping");
                        break;
                    }
                    int numTasksListed = 0;
                  
                        var job = this.batchClient.JobOperations.GetJob(this.clusterConfig.JobName);
                        string filterClause = string.Format("executionInfo/endTime ge datetime'{0}'", this.maxTaskCompletionTimeObserved.Subtract(TimeSpan.FromMinutes(2)).ToString("o"));
                        var tasks = job.ListTasks(new ODATADetailLevel(filterClause));
                        
                        //foreach (Microsoft.Azure.Batch.Protocol.Entities.Task task in resp.Tasks)
                        foreach (var task in tasks)
                        {
                            numTasksListed++;
                            //AzureBatchTask pendingTask ;
                            //if (this.pendingTasks.TryGetValue(task.Id.ToLowerInvariant(), out pendingTask))
                            if (this.pendingTasks.Keys.Contains(task.Id.ToLowerInvariant()))
                            {
                                AzureBatchTask pendingTask = this.pendingTasks[task.Id.ToLowerInvariant()];
                                pendingTask.TaskResponseExecutionInfo = task.ExecutionInformation;

                                ComputeTaskState computetaskstate = ComputeTaskState.None;
                                switch (task.State)
                                {
                                    case TaskState.Active:
                                        computetaskstate = ComputeTaskState.Queued;
                                        break;
                                    case TaskState.Completed:
                                        Debug.Assert(task.ExecutionInformation.EndTime != null);
                                        if (task.ExecutionInformation.EndTime > this.maxTaskCompletionTimeObserved)
                                        {
                                            this.maxTaskCompletionTimeObserved = (DateTime)task.ExecutionInformation.EndTime;
                                        }
                                        computetaskstate = ComputeTaskState.Completed;
                                        break;
                                    case TaskState.Invalid:
                                        computetaskstate = ComputeTaskState.None;
                                        break;
                                    case TaskState.Running:
                                        computetaskstate = ComputeTaskState.Running;
                                        break;
                                }
                                if (pendingTask.State != computetaskstate)
                                {
                                    Trace.TraceInformation(String.Format("Marking State change of task - {0} from {1} to {2}", pendingTask.Name, pendingTask.State, computetaskstate));
                                    pendingTask.State = computetaskstate;
                                }
                            }
                        }
                    //    nextMarker = resp.SkipToken; //TODO: We can actually make this async so that we can get the next set while we parse the current response.
                    //} while (!string.IsNullOrEmpty(nextMarker));

                    Trace.WriteLine(string.Format("Number of tasks listed due to filter = {0}", numTasksListed));
                    DateTime end = DateTime.UtcNow;
                    
                    double totalMillisecs = ListTaskWaitInterval.Subtract(end.Subtract(start)).TotalMilliseconds;
                    if (totalMillisecs > 1000)
                    {
                        Thread.Sleep((int)totalMillisecs);
                    }

                    //Thread.Sleep(1000);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
        
        #endregion BackgroundGetTaskThread

        #region InternalMethods
        internal void ListTaskReqCallback(IAsyncResult result)
        {
            throw new NotImplementedException();
        }
        #endregion InternalMethods

        #region HelperMethods
        public ComputeTask CreateNewTask(/*ComputeSession session, */string taskName)
        {
            ComputeTask task;
            lock (this)
            {                 
                if (this.allTasks.Keys.Contains(taskName.ToLowerInvariant()))
                {
                    //task = this.allTasks[taskName.ToLowerInvariant()];
                    Trace.TraceError("The task with same name exists - {0}", taskName);
                    throw new InvalidOperationException(string.Format("The task with same name exists - {0}", taskName));
                }
                else
                {
                    task = new AzureBatchTask(this.clusterConfig, taskName);
                    this.allTasks[taskName.ToLowerInvariant()] = (AzureBatchTask)task;
                }
            }
            return task;
        }

        public ComputeTask GetCurrentTask(/*ComputeSession session*/)
        {
            throw new NotImplementedException();
        }

        public void ScheduleTask(AzureBatchTask task)
        {
            lock (this)
            {
                if (this.pendingTasks.Keys.Contains(task.Name.ToLowerInvariant()))
                {
                    throw new InvalidOperationException("The task is already added to be scheduled");
                }
                this.pendingTasks[task.Name.ToLowerInvariant()] = task;
                this.addTaskHelper.AddTask(task);
            }
        }

        public void CancelTask(ComputeTask task)
        {
            throw new NotImplementedException();
        }

        public IAsyncResult BeginWaitForStateChange(AzureBatchTask task, ComputeTaskState waitFor, TimeSpan timeout, AsyncCallback callback, object state)
        {
            return task.BeginWaitForTaskState(waitFor, timeout, callback, state);
        }

        public bool EndWaitForStateChange(AzureBatchTask task, IAsyncResult result)
        {
            return task.EndWaitForStateChange(result);
        }

        public void Stop()
        {
            lock (this)
            {
                this.stopAllThreads = true;
                this.addTaskHelper.StopBackgroundActivity();
            }
        }
        #endregion HelperMethods
        
        #region PropertiesAndVars
        TaskClusterConfiguration clusterConfig; //configuration settings
        bool stopAllThreads = false;
        static AzureBatchHelper singletonAddTaskHelper;
        static object syncRoot = new object(); //Lock for this class


        System.Threading.Thread backgroundGetTaskStateThread; //Background thread that polls for task state changes
        //Helper task that collects all tasks to be scheduled to the cluster
        BackgroundAddTaskHelper addTaskHelper; 

        //BatchSharedKeyCredentials taskCredentials;
        BatchClient batchClient;
        Dictionary<string, AzureBatchTask> allTasks = new Dictionary<string, AzureBatchTask>();
        Dictionary<string, AzureBatchTask> pendingTasks = new Dictionary<string, AzureBatchTask>();
        Dictionary<string, AzureBatchTask> completedTasks = new Dictionary<string, AzureBatchTask>();
        AutoResetEvent listTaskEvent = new AutoResetEvent(true);
        static int listTaskWaitSeconds = 5;
        TimeSpan ListTaskWaitInterval = TimeSpan.FromSeconds(listTaskWaitSeconds);
        //TODO: Make is minVal. But there is a bug in batch code that prevents this for now.
        DateTime maxTaskCompletionTimeObserved = DateTime.Now.Subtract(TimeSpan.FromDays(100));        
        #endregion PropertiesAndVars
    }
}
