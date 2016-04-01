using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using System.Diagnostics;
using Microsoft.Azure;
using DAGBuilderInterface;
using Microsoft.Azure.Batch.Auth;

namespace AzureBatchCluster
{    

    /// <summary>
    /// The ComputeCluster implementation for Azure Batch. This is given as a default implementaion with the Package.
    /// Azure Batch service is a highly reliable and scaleable service which is exposed as part of Azure, that 
    /// does both work and resource management, starting for small scale(1 VM) to hypersclae( 100,000 and above VM range)
    /// This system is ideal for running large scale, highly parallel workloads/Jobs
    /// For more info about azure batch plese see: https://azure.microsoft.com/en-us/services/batch/
    /// </summary>
    public class AzureBatchCluster: ComputeCluster
    {
        string AccountName { get; set; }
        string Key { get; set; }
        string JobName { get; set; }
        string WorkItemName { get; set; }
        bool IsRunningInCluster { get; set; }
        string ComponentName { get; set; }
        TaskClusterConfiguration UserConfiguration { get; set; }
        //AzureBatchClusterJob Job { get; set; }
        private Dictionary<string, AzureBatchTask> RunningTaskList { get; set; }
        private Dictionary<string, AzureBatchTask> CompleteTaskList { get; set; }
        private Dictionary<string, AzureBatchTask> NonSubmittedTaskList { get; set; }
        private BatchCredentials taskCredentials;
        private AzureBatchHelper azureTaskHelper;

        #region BackGroundWork
        internal void BackGroundTaskStateSync()
        {
        
        }

        #endregion BackGroundWork
        

        #region ComputeCluster Implementation

        private string ClusterInitialize(string componentName, TaskClusterConfiguration cfg)
        {
            this.UserConfiguration = cfg;            
            this.JobName = this.UserConfiguration.JobName;
            new System.Threading.Thread(new System.Threading.ThreadStart(this.BackGroundTaskStateSync));
            this.taskCredentials = new BatchSharedKeyCredentials(this.UserConfiguration.TenantUri, this.UserConfiguration.AccountName, this.UserConfiguration.Key);            
            AzureTaskBootupHelper setupWorkItemHelper = new AzureTaskBootupHelper();            
            this.WorkItemName = setupWorkItemHelper.BootUpWorkItemAndJob(this.UserConfiguration);
            this.UserConfiguration.JobName = this.WorkItemName;
            this.azureTaskHelper = AzureBatchHelper.GetNewInstance(this.UserConfiguration);
            return this.WorkItemName;
        }

        /// <summary>
        /// Initialize the Azure Batch Cluster. 
        /// </summary>
        /// <param name="componentName">Name of the component/Cluster</param>
        /// <param name="configurationFile">The configuratio file with various configuratio parameters
        /// refer to <paramref name="TaskClusterConfiguration"/> for the format of the configuration file
        /// NOTE: The configutaion file can have other settings and details related to the job, the Batch Cluster
        /// will pick the parameters that it requires.</param>
        /// <returns></returns>
        public string ClusterInitialize(string componentName, string configurationFile)
        {
            if(!System.IO.File.Exists(configurationFile))
                throw new InvalidDataException("configuration file does not exist");            
            TaskClusterConfiguration cfg = AzureBatchClusterGlobal.GetConfiguration(configurationFile);

            return this.ClusterInitialize(componentName, cfg);            
        }

        /// <summary>
        /// Initialize the Azure Batch Cluster
        /// </summary>
        /// <param name="componentName">Name of the component/Cluster</param>
        /// <param name="tenantUri">The Azure Batch service endpoint. 
        /// it is of the type https://[ACCOUNTNAME].region.batch.azure.net</param>
        /// <param name="batchAccountName">The name of batch account</param>
        /// <param name="batchAccountkey">The key for the batch account</param>
        /// <param name="wIName"></param>
        /// <param name="jobName">The jobName</param>
        /// <param name="poolName">The pool to use as compute nodes for this cluster. 
        /// NOTE: Azure batch system allows work to be scheduled even when the compute resources are not available.
        /// Please make sure that the pool exists. Use the Create Pool API to create a Pool.
        /// See https://azure.microsoft.com/en-us/services/batch/ for more details on Azure Batch</param>
        /// <returns></returns>
        public string ClusterInitialize(string componentName, string tenantUri, string batchAccountName, string batchAccountkey, string wIName, string jobName, string poolName)
        {            
            TaskClusterConfiguration  cfg = new TaskClusterConfiguration();            
            cfg.AccountName = batchAccountName;
            cfg.Key = batchAccountkey;
            cfg.JobName = jobName;
            cfg.PoolName = poolName;
            //cfg.WorkItemName = wIName;
            cfg.TenantUri = tenantUri;
            
            return this.ClusterInitialize(componentName, cfg);           
        }

        /// <summary>
        /// De-initializes the Azure Batch Cluster
        /// </summary>
        /// <param name="reason"></param>
        public void ClusterDeInitialize(string reason)
        {
            this.azureTaskHelper.Stop();
        }
       
        /// <summary>
        /// Creates a new Task container of type = <paramref name="AzureBatchTask"/> 
        /// </summary>
        /// <param name="taskName">the name of the task</param>
        /// <returns></returns>
        public ComputeTask CreateNewTask(/*ComputeSession session, */string taskName)
        {
            return this.azureTaskHelper.CreateNewTask(/*session,*/ taskName);
        }

        /// <summary>
        /// Queues the given task to be scheduled in the bacground on to the VMs of the Azure Batch Pool
        /// </summary>
        /// <param name="task">The task to schedule</param>
        public void ScheduleTask(ComputeTask task)
        {
            if (task as AzureBatchTask == null)
                throw new InvalidDataException("The supplied task is not an Azure Task");
            
            Trace.TraceInformation("Scheduling Task - {0}", task.Name);
            this.azureTaskHelper.ScheduleTask(task as AzureBatchTask);
        }

        /// <summary>
        /// Cancels the running task. 
        /// </summary>
        /// <param name="task"></param>
        public void CancelTask(ComputeTask task)
        {
            if (task as AzureBatchTask == null)
                throw new InvalidDataException("The supplied task is not an Azure Task");
            throw new NotImplementedException();
        }

        /// <summary>
        /// Begins wait for task to reach desired state.
        /// </summary>
        /// <param name="task">name of task</param>
        /// <param name="waitFor">state to wait for</param>
        /// <param name="timeout">amount of time to wait</param>
        /// <param name="callback">the callback to call when desired state or timeout is reached</param>
        /// <param name="state"></param>
        /// <returns></returns>
        public IAsyncResult BeginWaitForStateChange(ComputeTask task, ComputeTaskState waitFor, TimeSpan timeout, AsyncCallback callback, object state)
        {
            if (task as AzureBatchTask == null)
                throw new InvalidDataException("The supplied task is not an Azure Task");
            return (task as AzureBatchTask).BeginWaitForTaskState(waitFor, timeout, callback, state);

        }

        /// <summary>
        /// Get the result of BeginWaitForStateChange
        /// </summary>
        /// <param name="result">The task result</param>
        /// <returns></returns>
        public bool EndWaitForStateChange(IAsyncResult result)
        {
            return true;            
        }
        
        #endregion ComputeCluster Implementation

    }
}
