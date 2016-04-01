//
// © Microsoft Corporation.  All rights reserved.
//

using System;
using System.Collections.Generic;

namespace DAGBuilderDataProviders
{
    /// <summary>
    /// Encapsulates information about a data partition. This 
    /// interface contains all the information required to 
    /// operate on the partition. All classes implementing 
    /// this interface should be marked serializable.
    /// </summary>       
    public interface IXCDataPartition
    {
        /// <summary>
        /// Represents a list of opaque affinity strings. The 
        /// affinity string can be passed to XCompute for 
        /// selecting the compute node performing the operation 
        /// on the partition. It can be a name of a compute node. 
        /// </summary>
        String[] Affinities { get; }

        /// <summary>
        /// Weight is represented as the estimated size of the 
        /// data in bytes that the partition holds.
        ///</summary>
        Int64 Weight { get; set; }

        /// <summary>
        /// Each data partition is a collection of sub data
        /// partitions. This notion can support multiple 
        /// levels of partitioning. For example, a list of 
        /// blobs can be partitioned to multiple blobs and 
        /// each blob can be partitioned to multiple blocks.
        /// </summary>
        IEnumerable<IXCDataPartition> Partitions { get; }
    }


    /// <summary>
    /// Encapsulates the information about a Range partition
    /// A Range partition is a specialized data partition
    /// where each partition contains a low and high value.
    /// </summary>
    public interface IXCRangePartition<T1, T2> : IXCDataPartition
    {
        /// <summary>
        /// Low value of the range
        /// </summary>
        T1 Low { get; }

        /// <summary>
        /// High value of the range
        /// </summary>
        T1 High { get; }

        /// <summary>
        /// The resource which is being range partitioned. For 
        /// example, the resource of a blob range partition 
        /// has blob properties (blob name, container name etc)
        /// and a table range partition contains the table 
        /// properties
        /// </summary>
        T2 Resource { get; }
    }
}
