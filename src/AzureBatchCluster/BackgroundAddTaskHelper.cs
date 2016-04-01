using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Batch.Protocol;
using Microsoft.Azure.Batch.Protocol.Models;
using System.Diagnostics;

namespace AzureBatchCluster
{
    class BackgroundAddTaskHelper
    {
        #region Constructor
        internal BackgroundAddTaskHelper(TaskClusterConfiguration config, AzureBatchHelper taskHelper)
        {
            this.clusterConfig = config;
            this.taskCredentials = new BatchSharedKeyCredential(config.AccountName, config.Key);
            this.taskHelper = taskHelper;
            this.batchClient = new BatchRestClient(this.taskCredentials, config.TaskTenantUri);
            var infoHdr = new System.Net.Http.Headers.ProductInfoHeaderValue("AzureBatchDAGBuilder", "1.0");
            this.batchClient.UserAgent.Add(infoHdr);
        }

        #endregion Constructor



        #region ThreadBackground


        internal async Task DoJobCommit(AzureBatchTask taskToCreate)        
        {

            Trace.WriteLine(String.Format("(ThreadId - {0})Adding Task - {1}", Thread.CurrentThread.ManagedThreadId, taskToCreate.Name));
            var cloudTask = new CloudTask(taskToCreate.Name, taskToCreate.TaskSchedulingInfo.CommandLine);

            //task.CommandLine = task.TaskSchedulingInfo.CommandLine;
            List<ResourceFile> files =
                new List<ResourceFile>();
            foreach (var file in taskToCreate.TaskSchedulingInfo.Files)
            {
                var resFile = new ResourceFile(file.FileSource, file.FileName);
                files.Add(resFile);
            }
            cloudTask.ResourceFiles = files;
            List<EnvironmentSetting> envSettings =
                new List<EnvironmentSetting>();
            foreach (var key in taskToCreate.TaskSchedulingInfo.EnvironmentSettings.Keys)
            {
                var env = new EnvironmentSetting(((string)key));
                env.Value = taskToCreate.TaskSchedulingInfo.EnvironmentSettings[(string)key];
                envSettings.Add(env);
            }
            cloudTask.EnvironmentSettings = envSettings;

            cloudTask.RunElevated = taskToCreate.RunElevated;

            //cl.Credentials.InitializeServiceClient(
            CloudTaskAddParameters taskAddParams =
                new CloudTaskAddParameters(cloudTask);
            var cl = new BatchRestClient(this.taskCredentials, clusterConfig.TaskTenantUri);
            try
            {
                var result = await this.batchClient.Tasks.AddAsync(this.clusterConfig.JobName, taskAddParams);            
                //var result = await cl.Tasks.AddAsync(this.clusterConfig.JobName, taskAddParams);
                //result.StatusCode

                if (result.StatusCode != System.Net.HttpStatusCode.Accepted &&
                        result.StatusCode != System.Net.HttpStatusCode.Created)
                {
                    lock (this)
                    {
                        taskToCreate.InternalSchedulingState = AzureTaskSchedulingState.ScheduleFailed;
                        this.failedAddTaskList[taskToCreate.Name] = taskToCreate;
                        workPendingEvent.Set();
                        Trace.WriteLine(String.Format("(ThreadId - {0})New Task Add Done. Task - {1}", Thread.CurrentThread.ManagedThreadId, taskToCreate.Name));
                    }
                }
                else
                {
                    numTasksAcceptedByBatchSystem++;
                    Trace.WriteLine(String.Format("Time Taken to submit {0} tasks = {1}", numTasksAcceptedByBatchSystem, DateTime.Now.Subtract(addTaskStartTime).TotalSeconds));
                }
            }
            catch (Exception e)
            {
                Trace.WriteLine(e.ToString());
                throw e;
            }

        }


        internal void AddTaskBackground()
        {
            try
            {
                addTaskStartTime = DateTime.Now;
                while (true)
                {
                    if (this.stopBackgroundActivity == true)
                    {
                        Trace.WriteLine("Stopping the AddTaskBackground thread.");
                        break;
                    }
                    workPendingEvent.WaitOne(TimeSpan.FromSeconds(1)); //wait for work pending or timeout
                    //List<CloudTask> tasksToCreate = new List<CloudTask>();
                    lock (this)
                    {
                        string[] taskNames = this.pendingAddTaskList.Keys.ToArray();

                        foreach (string taskName in taskNames)
                        {
                            if (numPendingAddTask >= 10) break; //Come back and add more                            
                            AzureBatchTask task = this.pendingAddTaskList[taskName];

                            var res = this.DoJobCommit(task);
                            
                            this.pendingAddTaskList.Remove(taskName);
                            Interlocked.Increment(ref numPendingAddTask);
                        }
                    }
                    //this.batchClient.JobOperations.AddTask(this.clusterConfig.JobName, tasksToCreate);                                            
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                throw e;
            }
        }


        #endregion ThreadBackground


        #region InternalMethods

        internal bool AddTask(AzureBatchTask task)
        {
            lock (this)
            {
                if (null == backgroundAddTaskThread)
                {
                    backgroundAddTaskThread = new System.Threading.Thread(new System.Threading.ThreadStart(AddTaskBackground));
                    backgroundAddTaskThread.Start();
                }

                if (!pendingAddTaskList.ContainsKey(task.Name.ToLower(System.Globalization.CultureInfo.InvariantCulture)))
                {
                    pendingAddTaskList[task.Name.ToLower(System.Globalization.CultureInfo.InvariantCulture)] = task;
                    //if (pendingAddTaskList.Count() >= BackgroundAddTaskHelper.MinPendingAddTaskQueueLength)
                    {
                        workPendingEvent.Set();
                    }
                }
                else
                    return false;
            }
            return true;
        }

        internal void StopBackgroundActivity()
        {
            this.stopBackgroundActivity = true;
        }
        #endregion InternalMethods

        #region PrivateMethods
       
        #endregion PrivateMethods

        #region PropertiesAndVars
        bool stopBackgroundActivity = false;
        System.Threading.Thread backgroundAddTaskThread;
        //This is the list of tasks that have not yet been scheduled with the Azure Task cluster
        Dictionary<string, AzureBatchTask> pendingAddTaskList = new Dictionary<string, AzureBatchTask>();
        //An event is signalled only when number of pending tasks is more than or equal to this value
        const uint MinPendingAddTaskQueueLength = 5;

        //These are the list of task sthat failed when an add task request was made to the Azure cluster.
        Dictionary<string, AzureBatchTask> failedAddTaskList = new Dictionary<string, AzureBatchTask>();

        TaskClusterConfiguration clusterConfig;
        AzureBatchHelper taskHelper;
        BatchRestClient batchClient;

        BatchCredentials taskCredentials;
        AutoResetEvent workPendingEvent = new AutoResetEvent(false);
        int numPendingAddTask = 0;

        DateTime addTaskStartTime;
        int numTasksAcceptedByBatchSystem = 0;

        #endregion PropertiesAndVars
    }
}
