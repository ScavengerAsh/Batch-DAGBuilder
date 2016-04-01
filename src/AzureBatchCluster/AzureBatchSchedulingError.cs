using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Protocol;
using DAGBuilderInterface;
using Microsoft.Azure.Batch.Common;

namespace AzureBatchCluster
{
    class AzureBatchSchedulingError: ComputeTaskSchedulingError
    {
        public void Copy(TaskSchedulingError error)
        {
            if (error.Category == SchedulingErrorCategory.ServerError)
                this.Category = ComputeTaskSchedulingErrorCategory.ServerError;
            else if (error.Category == SchedulingErrorCategory.UserError)
                this.Category = ComputeTaskSchedulingErrorCategory.UserError;

            this.Code = error.Code.ToString();
            if (error.Details != null)
            {
                this.DetailedErrorMessages = new System.Collections.Specialized.NameValueCollection();
                foreach (var errVal in error.Details)
                {
                    DetailedErrorMessages.Add(errVal.Name, errVal.Value);
                }
            }
            this.Message = error.Message;
        }

        public ComputeTaskSchedulingErrorCategory Category
        {
            get;
            set;
        }

        public string Code
        {
            get;
            set;            
        }

        public System.Collections.Specialized.NameValueCollection DetailedErrorMessages
        {
            get;
            set;            
        }

        public string Message
        {
            get;
            set;
        }
    }
}
