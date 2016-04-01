using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;
using System.Collections.Specialized;
using System.Net;
using System.IO;


 
 
namespace DAGBuilderInterface
{
    /// <summary>
    /// These are well know errors that the system uses to decide on how to retyr.
    /// For e.g. for PreviousVertexDataUnavailable error, the system re-runs the portion 
    /// of the graph to produce the required result     
    /// </summary>
    public class DagBuilderWellKnownErrors
    {
        /// <summary>
        /// The ComputeTask completed successfully
        /// </summary>
        public const int Passed = 0x0;
        /// <summary>
        /// The ComputeTask exited with failure
        /// </summary>
        public const int Failed = int.MaxValue;
        /// <summary>
        /// The Compute Task failed and Needs to be retired
        /// </summary>
        public const int NeedsRetry = 0x001;
        /// <summary>
        /// Compute task failed with previous ComputeTask data unavailable error
        /// </summary>
        public const int PreviousVertexDataUnavailable = 0x100;
    }

   /// <summary>
   /// Enumerates the various states that a AzureBatchTask can be in
   /// </summary>
    public enum ComputeTaskState
    {
        /// <summary>
        /// None
        /// </summary>
        None = 0,

        /// <summary>
        /// Active
        /// </summary>
        Queued = 1,

        /// <summary>
        /// Running
        /// </summary>
        Running = 2,

        /// <summary>
        /// Completed
        /// </summary>
        Completed = 3,
    };
    
    /// <summary>
    /// The interface for expressing a resource file for compute task.
    /// It defines the point where the resource exists (from where the resource needs to be copied)
    /// And it defines the name of the file on the ComputeNode once it is copied.
    /// </summary>
    public interface ComputeTaskResourceFile
    {
        /// <summary>
        /// Required. Specifies the source URL for a Windows Azure blob. 
        /// </summary>
        string FileSource { get; set; }

        /// <summary>
        /// Required. Specifies the target filename under which the blob contents will be downloaded when the JobManager is launched on a node. 
        /// </summary>
        string FileName { get; set; }
        
    }

    /// <summary>
    /// 
    /// </summary>
    public interface ComputeTaskAffinityInfo
    {
        /// <summary>
        /// Defines a set of affinites for the ComputeTask. 
        /// That means the ComputeTask depends on data from those tasks
        /// 
        /// </summary>
        NameValueCollection AffinityIds { get; set; }     
    }

    /// <summary>
    /// Constraints for a given Compute Task
    /// </summary>
    public interface ComputeTaskConstraints
    {
        /// <summary>
        /// Gets or sets the maximum duration of time for which a task is allowed to run from the time it is created.
        /// </summary>
        /// <value>The maximum duration of time for which a task is allowed to run from the time it is created.</value>    
        TimeSpan? MaxWallClockTime 
        { 
            get; 
            set; 
        }

        /// <summary>
        /// Gets or sets the max number of retries for the tasks. The default value is 0.  
        /// </summary>
        /// <value>The max number of retries for the tasks. </value>  
        int MaxTaskRetryCount
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the duration of time for which files in the task's working directory are retained, from the time 
        /// it completes execution. After this duration, the task’s working directory is reclaimed.
        /// </summary>
        /// <value>The duration of time for which files in the task’s working directory are retained.</value>    
        TimeSpan? RetentionTime 
        { 
            get; 
            set; 
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public enum ComputeTaskSchedulingErrorCategory
    {
        /// <summary>
        /// UserError
        /// </summary>
        UserError = 0,

        /// <summary>
        /// ServerError
        /// </summary>
        ServerError = 1,
    }

    /// <summary>
    /// Details related to tasks scheduling on the compute nodes.
    /// These errors are compute cluster specific
    /// </summary>
    public interface ComputeTaskSchedulingError
    {
        /// <summary>
        /// Gets the error category.
        /// </summary>
        ComputeTaskSchedulingErrorCategory Category 
        { 
            get; 
            set; 
        }
        
        /// <summary>
        /// Gets the error code.
        /// </summary>
        string Code 
        { 
            get; 
            set; 
        }
        
        /// <summary>
        /// Gets the error message.
        /// </summary>
        string Message 
        { 
            get; 
            set; 
        }

        /// <summary>
        /// Gets a set of the detailed error messages.
        /// </summary>
        /// <value>The detailed error messages</value> 
        NameValueCollection DetailedErrorMessages
        {
            get;
            set;
        }        
    }

    /// <summary>
    /// Task execution related properties
    /// </summary>
    public interface ComputeTaskExecutionInfo
    {
        /// <summary>
        /// The time at which the task started running.
        /// </summary>
        DateTime? StartTime { get; set; }

        /// <summary>
        /// An opaque string that represents a previously run task.  
        /// </summary>
        string AffinityId { get; set; }

        /// <summary>
        /// The time at which the task completed.  
        /// </summary>
        DateTime? EndTime { get; set; }

        /// <summary>
        /// The exit code of the task.  
        /// </summary>
        int? ExitCode { get; set; }

        /// <summary>
        /// The error encountered by the service in starting the task. 
        /// </summary>
        ComputeTaskSchedulingError SchedulingError { get; set; }

        /// <summary>
        /// The URL of the TVM on which the task was run. 
        /// </summary>
        string TVMUrl { get; set; }

        /// <summary>
        /// The number of times the task has been retried by the Task Service.  
        /// </summary>
        int RetryCount { get; set; }

        /// <summary>
        /// The most recent time at which this task’s execution was retried by the Task service.
        /// This is only returned if the RetryCount is not 0.
        /// </summary>
        DateTime? LastRetryTime { get; set; }

        /// <summary>
        /// The number of times the task has been retried by the Task Service.  
        /// </summary>
        int RequeueCount { get; set; }

        /// <summary>
        /// The most recent time at which this task’s execution was retried by the Task service.
        /// This is only returned if the RetryCount is not 0.
        /// </summary>
        DateTime? LastRequeueTime { get; set; }

        /// <summary>
        /// The number of times this task's execution was pre-empted by the Task service.
        /// </summary>
        long PreemptionCount { get; set; }

        /// <summary>
        /// The time at which this task's execution was previously pre-empted by the Task service.
        /// </summary>
        DateTime? LastPreemptionTime { get; set; }
        
    }

    /// <summary>
    /// Statistics related to a task run
    /// </summary>
    public interface ComputeTaskStatistics
    {
        /// <summary>
        /// Gets the start time of the time range for the statistics.
        /// </summary>
        DateTime? StartTime 
        { 
            get; 
            set; 
        }

        /// <summary>
        /// Gets the end time of the time range for the statistics.
        /// </summary>
        DateTime? EndTime
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the total user mode CPU time (per core) consumed by the task. 
        /// </summary>
        TimeSpan UserCPUTime
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the total kernel mode CPU time (per core) consumed by the task. 
        /// </summary>
        TimeSpan KernelCPUTime
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the wall clock time of the task execution. 
        /// </summary>
        TimeSpan WallClockTime
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the total number of disk I/O (network + disk) read operations made byfor the task.
        /// </summary>
        long ReadIOps
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the total number of disk I/O (network + disk) write operations made byfor the task.
        /// </summary>
        long WriteIOps
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the total bytes of I/O (network + disk) disk read for by the task.
        /// </summary>
        long ReadIOBytes
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the total bytes of I/O (network + disk) disk written for by the task.
        /// </summary>
        long WriteIOBytes
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the number of retries occurred on the task. 
        /// </summary>
        int NumRetries
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the wait time for a task is the time between the task creation and the start of the most recent task 
        /// execution (if the task is retried due to failures).
        /// </summary>
        TimeSpan WaitTime
        {
            get;
            set;
        }        
    }

    /// <summary>
    /// The state of a TVM
    /// </summary>
    public enum ComputeNodeState
    {
        /// <summary>
        /// None
        /// </summary>
        None = 0,

        /// <summary>
        /// Idle
        /// </summary>
        Idle = 1,

        /// <summary>
        /// Rebooting
        /// </summary>
        Rebooting = 2,

        /// <summary>
        /// Reimaging
        /// </summary>
        Reimaging = 3,

        /// <summary>
        /// Running
        /// </summary>
        Running = 4,

        /// <summary>
        /// Unknown
        /// </summary>
        Unknown = 5,
    };

    /// <summary>
    /// Type of Compute Node
    /// </summary>
    public enum ComputeNodeType
    {
        /// <summary>
        /// None
        /// </summary>
        None = 0,

        /// <summary>
        /// Dedicated
        /// </summary>
        Dedicated = 1,

        /// <summary>
        /// Preemptible
        /// </summary>
        Preemptible = 2,

        /// <summary>
        /// Any
        /// </summary>
        Any = 3,
    }

    /*
    public enum ComputeNodeSize
    {
        /// <summary>
        /// Small 
        /// </summary>
        Small = 1,

        /// <summary>
        /// Large
        /// </summary>
        Large = 2,

        /// <summary>
        /// ExtraLarge
        /// </summary>
        ExtraLarge = 3,
    };
     * */

    /// <summary>
    /// Defines a compute node for a given cluster
    /// </summary>
    public interface ComputeNode
    {
        /// <summary>
        /// The name of the TVM.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// The url of the compute node.
        /// </summary>
        string Url { get; }

        /// <summary>
        /// The current state of the TVM. 
        /// </summary>
        ComputeNodeState State { get; }

        /// <summary>
        /// The time at which the TVM entered the current state.
        /// </summary>
        DateTime StateTransitionTime { get; }

        /// <summary>
        /// The time at which the TVM was started.
        /// </summary>
        DateTime? LastBootTime { get; }

        /// <summary>
        /// The type of the TVM. 
        /// </summary>
        ComputeNodeType TVMType { get; }

        /// <summary>
        /// The time at which this TVM was allocated to the pool.
        /// </summary>
        DateTime TVMAllocationTime { get; }
        
        /// <summary>
        /// The IP address associated with the TVM.
        /// </summary>
        IPAddress IPAddress { get; }

        /// <summary>
        /// An opaque string that contains information about the location of the TVM.    
        /// </summary>
        string AffinityId { get; }

        /// <summary>
        /// The size of TVMs in the pool.  
        /// </summary>
        string TVMSize { get; }

        /// <summary>
        /// The number of tasks that have been run on this TVM from the time it was allocated to this pool.
        /// </summary>
        int TotalTasksRun { get; }        
    }

    /// <summary>
    /// Defines the interface for the details required to launch the task onto a compute cluster
    /// </summary>
    public interface ComputeTaskSchedulingInfo
    {
        /// <summary>
        /// Copy method
        /// </summary>
        /// <param name="to"></param>
        void CopyTo(ComputeTaskSchedulingInfo to);       

        /// <summary>
        /// Gets or sets the command-line used to launch the task.
        /// </summary>
        string CommandLine
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the set of Windows Azure blobs that are downloaded to run the task.
        /// </summary>
        List<ComputeTaskResourceFile> Files
        {
            get;
            //set;
        }

        /// <summary>
        /// Gets or sets a set of environment settings for the JobManager task.
        /// </summary>
        NameValueCollection EnvironmentSettings
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the locality hints specified by the user for the task.  
        /// </summary>
        ComputeTaskAffinityInfo AffinityInfo
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the type of TVM on which the task must be scheduled, as specified by the user.
        /// </summary>
        string TVMType
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the execution information for the task.
        /// </summary>
        ComputeTaskConstraints Constraints
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the name of the task.
        /// </summary>
        string Name
        {
            get;
            set;
        }
    }
    
    /// <summary>
    /// Defines a compute Task and properties that the Graph system requires. 
    /// Each cluster implemetation derives from this base type and enhances it to include appropriate 
    /// constructors and other properties.
    /// </summary>
    public interface ComputeTask
    {
        //TODO: Need to add this interface method so as to pass serialized channel information to task.
        /*
        /// <summary>
        /// Adds metadata to the task. The DAGBuilder will call this to set data that needs
        /// to be passed to the task.
        /// </summary>
        /// <param name="name">Name of the metadata element</param>
        /// <param name="value">The value of the metadata element</param>
        void AddTaskMetadata(string name, string value);
        */

        /// <summary>
        /// The Tasks scheduling related details
        /// </summary>
        ComputeTaskSchedulingInfo TaskSchedulingInfo
        {
            get;            
        }

        /// <summary>
        /// Flag that signals whether the task should be launched with elevated privilages
        /// </summary>
        bool RunElevated
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the url of the task.
        /// </summary>
        string Url 
        { 
            get; 
            //set; 
        }

        /// <summary>
        /// Gets the creation time of the job.
        /// </summary>
        DateTime? CreationTime 
        { 
            get; 
            //set; 
        }

        /// <summary>
        /// Gets the Etag of the task.
        /// </summary>
        string ETag 
        { 
            get;             
        }

        /// <summary>
        /// Gets the last-modified time(UTC) of the task.
        /// </summary>
        DateTime? LastModified 
        { 
            get;
            
        }

        /// <summary>
        /// Gets the current state of the task. 
        /// </summary>
        ComputeTaskState State
        {
            get;            
        }

        /// <summary>
        /// Gets the time at which the task entered the current state.
        /// </summary>
        DateTime? StateTransitionTime
        {
            get;            
        }

        /// <summary>
        /// Gets the previous state of the task.
        /// This property is not returned if the task is in its initial active state.
        /// </summary>
        ComputeTaskState? PreviousState
        {
            get;            
        }

        /// <summary>
        /// Gets the time at which the task entered its previous state.
        /// This property is not returned if the task is in its initial active state.
        /// </summary>
        DateTime? PreviousStateTransitionTime
        {
            get;            
        }

        /// <summary>
        /// Gets the execution information for the task.
        /// </summary>
        ComputeTaskExecutionInfo ExecutionInfo
        {
            get;            
        }

        /// <summary>
        /// Gets the resource usage statistics for the task.
        /// </summary>
        ComputeTaskStatistics Statistics
        {
            get;            
        }

        /// <summary>
        /// Gets the name of the task.
        /// </summary>
        string Name
        {
            get;            
        }
    }    

    /// <summary>
    /// Defines a Compute Cluster. These are the minimum set of methods that a compute cluster needs to implement
    /// so that the DAGBuilder can use it to schedule work on the cluster.
    /// </summary>
    public interface ComputeCluster
    {        
        /// <summary>
        /// Called as a signal to the cluster code that all background and foreground activities related to cluster 
        /// can be stopped. 
        /// </summary>
        /// <param name="reason">The reason for de-initialize. The cluster can use this to log cluster related information</param>
        void ClusterDeInitialize(string reason);        
        /// <summary>
        /// Called by the system to get a container for the task that will later be run on the compute nodes of the cluster.
        /// </summary>
        /// <param name="taskName">The name of the task to be assigned. The name is unique and will be used to query the task</param>
        /// <returns></returns>
        ComputeTask CreateNewTask(string taskName);        
        /// <summary>
        /// Called by the sytem when it needs to intiate the Task onto a compute node. Even though the call is synchronous, 
        /// the system does not expect any particular state of the task. Thus giving the opportunity for the 
        /// cluster implementation to queue the task for scheduling in the background
        /// </summary>
        /// <param name="task">The compute task to schedule. 
        /// This task would have been earlier obtained by the system via a call to the CreateNewTask cluster interface method</param>
        void ScheduleTask(ComputeTask task); 
        /// <summary>
        /// Called by a system, to cancel a running task. As part of optimizations, the system might start duplicate tasks.
        /// And on completion of a task in the set cancel the other duplicates.
        /// </summary>
        /// <param name="task"></param>
        void CancelTask(ComputeTask task);
        /// <summary>
        /// Once ScheduleTask is called, the system calls this to start waiting for the task to reach a particular state
        /// </summary>
        /// <param name="task">Name of the Task</param>
        /// <param name="waitFor">The State to wait for the Task to reach</param>
        /// <param name="timeout">The amount of time to wait for the task to reach desired state</param>
        /// <param name="callback">the callback to call on either a timeout or when the desired task state is reached</param>
        /// <param name="state">system specified object to pass back</param>
        /// <returns></returns>
        IAsyncResult BeginWaitForStateChange(ComputeTask task, ComputeTaskState waitFor, TimeSpan timeout, AsyncCallback callback, Object state);
        /// <summary>
        /// Called to get the results of BeginWaitForStateChange
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        bool EndWaitForStateChange(IAsyncResult result);
    }
}