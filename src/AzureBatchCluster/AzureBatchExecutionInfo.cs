using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Protocol;

using DAGBuilderInterface;

namespace AzureBatchCluster
{
    class AzureBatchExecutionInfo : ComputeTaskExecutionInfo
    {
        public void Copy(TaskExecutionInformation info)
        {
            lock (this)
            {
                this.StartTime = info.StartTime;
                //BUGBUG: We need to pull affinity ID outside, since this property has moved over
                //this.AffinityId = info.AffinityId;
                this.EndTime = info.EndTime;
                this.ExitCode = info.ExitCode;
                if (info.SchedulingError != null && this.SchedulingError == null)
                { 
                    this.SchedulingError = new AzureBatchSchedulingError();
                    this.schedulingError.Copy(info.SchedulingError);
                }
                //BUGBUG: We need to pull affinity ID outside, since this property has moved over
                //this.TVMUrl = info.TVMUrl;
                this.RetryCount = info.RetryCount;
                this.LastRetryTime = info.LastRetryTime;
                this.RequeueCount = info.RequeueCount;
                this.LastRequeueTime = info.LastRequeueTime;                
            }
        }
        public DateTime? StartTime
        {
            get;
            set;
        }

        public string AffinityId
        {
            get;
            set;
        }

        public DateTime? EndTime
        {
            get;
            set;
        }

        public int? ExitCode
        {
            get;
            set;
        }

        private AzureBatchSchedulingError schedulingError;
        public ComputeTaskSchedulingError SchedulingError
        {
            get
            {
                return this.schedulingError;
            }
            set
            {
                this.schedulingError = value as AzureBatchSchedulingError;
            }
        }

        public string TVMUrl
        {
            get;
            set;
        }

        public int RetryCount
        {
            get;
            set;
        }

        public DateTime? LastRetryTime
        {
            get;
            set;
        }

        public int RequeueCount
        {
            get;
            set;
        }

        public DateTime? LastRequeueTime
        {
            get;
            set;
        }
        
        public long PreemptionCount { get; set; }

        /// <summary>
        /// The time at which this task's execution was previously pre-empted by the Task service.
        /// </summary>
        public DateTime? LastPreemptionTime { get; set; }
    }
}
