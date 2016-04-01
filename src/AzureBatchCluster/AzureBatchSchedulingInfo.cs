using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using DAGBuilderInterface;
namespace AzureBatchCluster
{
    class AzureBatchSchedulingInfo : ComputeTaskSchedulingInfo
    {

        public void CopyTo(ComputeTaskSchedulingInfo to)        
        {            
            //to.AffinityInfo = this.AffinityInfo;
            to.CommandLine = this.CommandLine;
            //to.Constraints = this.Constraints;
            to.EnvironmentSettings = this.EnvironmentSettings;
            if (this.Files != null)
                to.Files.AddRange(this.Files);
            //to.Name = this.Name;
            //to.TVMType = this.TVMType;            
        }        

        public string CommandLine
        {
            get;
            set;
        }

        List<ComputeTaskResourceFile> resourceFiles = new List<ComputeTaskResourceFile>();
        public List<ComputeTaskResourceFile> Files
        {
            get { return resourceFiles; }
            internal set { resourceFiles = value; }
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

        public ComputeTaskAffinityInfo AffinityInfo
        {
            get;
            set;
        }

        public string TVMType
        {
            get;
            set;
        }

        public ComputeTaskConstraints Constraints
        {
            get;
            set;
        }

        public string Name
        {
            get;
            set;
        }
    }
}
