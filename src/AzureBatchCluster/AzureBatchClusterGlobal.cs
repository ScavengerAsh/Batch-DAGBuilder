using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using System.IO;

//using Microsoft.WindowsAzure.StorageClient;
using Microsoft.Azure;
using Microsoft.Azure.Batch;


namespace AzureBatchCluster
{
    /*
    public static class WaTaskStrings
    {
        public const string jobName = "WATASK_JOB_NAME";
        public const string wiName = "WATASK_WORKITEM_NAME";
        public const string taskName = "WATASK_TASK_NAME";
    }
     * */

    /// <summary>
    /// Used to determine how the Auzre Batch Cluster should initialize the job associated with it. 
    /// </summary>
    public enum JobInitializationChoices
    { 
        /// <summary>
        /// Create a new Job and delete the old job that is listed as the JobName in the config
        /// </summary>
        CreateNewDeleteOld,
        /// <summary>
        /// Create a new Job only if the one listed in the JobName does not exist. Otherwise use the existing one
        /// </summary>
        CreateNewIfNotExist,
        /// <summary>
        /// Create a new unique name for the Job that gets associated with this cluster. This is the recommended setting to use
        /// </summary>
        CreateNewWithUniqueName
    }

    /// <summary>
    /// Configuration for the Azure Batch Cluster. Initializes the cluster with various parameters
    /// 
    /// </summary>
    public class TaskClusterConfiguration
    {
        /// <summary>
        /// [Required]The Azure Batch Account Name to use
        /// See https://azure.microsoft.com/en-us/documentation/articles/batch-account-create-portal/ for 
        /// step by step process to create an Azure Batch Account.
        /// </summary>
        public string AccountName { get; set; }
        /// <summary>
        /// [Required]The Key associated with the Azure Batch account. 
        /// </summary>
        public string Key { get; set; }
        /// <summary>
        /// [Optional]The Name of the Job associated with the Azure Batch Cluster. The way this gets used is based on 
        /// the value of the <paramref name=" JobInitializationChoices"/> parameter
        /// </summary>
        public string JobName { get; set; }        
        /// <summary>
        /// [Required]The name of the Pool associated with the Azure Batch Cluster. The pool provides the compute nodes
        /// for the vertexes of the job to run on.
        /// </summary>
        public string PoolName { get; set; }
        /// <summary>
        /// [Required]The Azure Batch Cluster Url
        /// </summary>
        public string TenantUri { get; set; }
        /// <summary>
        /// The Azure Batch Cluster Uri
        /// </summary>
        public Uri TaskTenantUri { get { return new Uri(TenantUri); } }
        /// <summary>
        /// [Optional]Choice on how to initialize the Job associated with the cluster.
        /// </summary>
        private JobInitializationChoices jobInitChoice = JobInitializationChoices.CreateNewWithUniqueName;
        public JobInitializationChoices JobInitializationParams
        {
            get { return jobInitChoice; }
            set { this.jobInitChoice = value; }
        }

        /// <summary>
        /// [Optional]The Storage Account to be used. This is used for uploading resources (files) that are required 
        /// for the Job vertexes to run. if the user already has uploaded resources to a storage account
        /// and has a SAS, then this might not be needed
        /// </summary>
        public string StorageAccountName { get; set; }
        /// <summary>
        /// [Optional]The Key associated with the storage account
        /// </summary>
        public string StorageAccountKey { get; set; }
        /// <summary>
        /// Blob URL endpoint [optional]
        /// </summary>
        public string BlobUrl { get; set; }
        
        public TaskClusterConfiguration() { }
        public bool CheckConfiguration()
        {
            if (String.IsNullOrEmpty(this.AccountName) ||
                String.IsNullOrEmpty(this.Key) ||
                String.IsNullOrEmpty(this.JobName) ||                
                String.IsNullOrEmpty(this.PoolName) )
            {
                return false;
            }
            else
            {
                return true;
            }
        }
    }

    class AzureBatchClusterGlobal
    {
        private static object syncObj = new object();
        
        public static TaskClusterConfiguration GetConfiguration(string configurationFile)
        {
            lock (syncObj)
            {
                XmlSerializer deSerializer = new XmlSerializer(typeof(TaskClusterConfiguration));                
                TaskClusterConfiguration cfg = (TaskClusterConfiguration)deSerializer.Deserialize(File.OpenRead(configurationFile));
                if (String.IsNullOrEmpty(cfg.AccountName) ||
                    String.IsNullOrEmpty(cfg.Key))
                {
                    throw new InvalidDataException("Configuration file does not have AccountName/Key specified");
                }

                if (String.IsNullOrEmpty(cfg.JobName))
                {
                }
                
                if (String.IsNullOrEmpty(cfg.PoolName))
                {

                }
                return cfg;

            }            
        }
    }
}
