using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using System.IO;
using Microsoft.WindowsAzure;
using System.Runtime.Serialization;
using System.Collections.Concurrent;
using System.Threading;
using System.Net;
using System.Security.Cryptography;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace DAGBuilderDataProviders
{
    /// <summary>
    /// This is the main class in the blob data provider API 
    /// for XCompute. It extends the IXCDataPartition class.
    /// It provides interfaces for reading and writing data to blobs 
    /// and partitioning and affinities
    /// </summary>
    [Serializable]
    public class XCBlobProvider : IXCDataPartition, ISerializable
    {
        #region Member Variables

        private List<ICloudBlob> m_blobList;
        private UInt32 m_numPartitions;
        private List<XCBlobPartition> m_blobPartitions;
        private ICloudBlob m_outputBlob;
        private BlobProviderConstructorType m_constructorUsed;
        private XCBlobPartition m_curReadBlobPartition;
        private Exception m_exception;
        private int m_pendingAsyncWrites;
        private XCBlobReader m_blobReader;

        #endregion

        #region Helper Types

        [Serializable]
        enum BlobProviderConstructorType
        {
            ReadBlobList,
            ReadBlobPartitions,
            Write
        }

        #endregion

        #region Constructors

        /// <summary>
        /// Constructor which takes a list of blobs and number of partitions.
        /// Can be used only for input data source.
        /// </summary>
        /// <param name="inputBlobs">A list of Cloud blob objects which 
        /// contains the blob properties</param>
        /// <param name="numPartitions">Number of data partitions</param>
        public XCBlobProvider(List<ICloudBlob> inputBlobs, UInt32 numPartitions)
        {
            //Validate blob list
            ValidateBlobList(inputBlobs);
            this.m_blobList = inputBlobs;

            //Validate num partitions
            ValidateNumPartitions(numPartitions);
            this.m_numPartitions = numPartitions;

            this.m_constructorUsed = BlobProviderConstructorType.ReadBlobList;
        }

        /// <summary>
        /// Constructor which takes actual partitions containing the blob byte.
        /// ranges. Can be used only for input data source.
        /// </summary>
        /// <param name="inputBlobPartitions">A collection of blob partitions 
        /// containing multiple blob byte ranges</param>
        public XCBlobProvider(List<XCBlobPartition> inputBlobPartitions)
        {
            //Validate blob partitions
            ValidatePartitions(inputBlobPartitions);
            this.m_blobPartitions = inputBlobPartitions;

            //Set num partitions
            this.m_numPartitions = (uint)inputBlobPartitions.Count;

            this.m_constructorUsed = BlobProviderConstructorType.ReadBlobPartitions;
        }

        /// <summary>
        /// Constructor which takes the output blob. Can be used only
        /// for output data source.
        /// </summary>
        /// <param name="outputBlob">The output cloud blob object</param>
        public XCBlobProvider(ICloudBlob outputBlob)
        {
            if (outputBlob == null)
            {
                throw new Exception("Output blob cannot be null");
            }

            this.m_blobList = new List<ICloudBlob>();
            this.m_blobList.Add(outputBlob);

            //Number of partitions is set as 1. User can override it by 
            //using the NumPartitions property
            this.m_numPartitions = 1;

            this.m_outputBlob = outputBlob;
            this.m_constructorUsed = BlobProviderConstructorType.Write;
        }

        #endregion

        #region Serialization Methods

        protected XCBlobProvider(SerializationInfo info, StreamingContext context)
        {
            //Deserialize the output blob
            this.m_outputBlob = XCBlobHelpers.DeSerializeBlob(info);

            if (this.m_outputBlob != null)
            {
                this.m_constructorUsed = BlobProviderConstructorType.Write;
            }

            //Deserialize all the settings
            XCBlobHelpers.DeSerializeBlobSettings(info);

            //Deserialize constructor type
            this.m_constructorUsed = (BlobProviderConstructorType)info.GetValue(XCBlobHelpers.s_blobConstructorType,
                                                        typeof(BlobProviderConstructorType));
        }

        /// <summary>
        /// Gets the object data. Used during serialization
        /// </summary>
        /// <param name="info">The Serialization info</param>
        /// <param name="context">The StreamingContext</param>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            //Serialize only the output blob. Other properties are not required
            XCBlobHelpers.SerializeBlob(info, m_outputBlob);

            //Serialize all the settings
            XCBlobHelpers.SerializeBlobSettings(info);

            //Serialize constructor type
            info.AddValue(XCBlobHelpers.s_blobConstructorType, m_constructorUsed);
        }

        #endregion

        #region Properties

        /// <summary>
        /// Number of blob partitions
        /// </summary>
        public uint NumPartitions
        {
            get
            {
                return m_numPartitions;
            }
            set
            {
                if (this.m_constructorUsed == BlobProviderConstructorType.ReadBlobList ||
                    this.m_constructorUsed == BlobProviderConstructorType.Write)
                {
                    ValidateNumPartitions(value);
                    this.m_numPartitions = value;
                }
                else
                {
                    throw new InvalidOperationException("NumPartitions cannot be changed when " +
                                                        "blob partitions constructor is used");
                }
            }
        }

        /// <summary>
        /// Returns a list of Blob Partitions
        /// </summary>
        public List<XCBlobPartition> BlobPartitions
        {
            get
            {
                if (this.m_constructorUsed == BlobProviderConstructorType.ReadBlobPartitions)
                {
                    return this.m_blobPartitions;
                }
                else
                {
                    throw new InvalidOperationException("BlobPartitions cannot be obtained when " +
                                                        "blob partitions constructor is not used");
                }
            }
            set
            {
                if (this.m_constructorUsed == BlobProviderConstructorType.ReadBlobPartitions)
                {
                    //Validate the blob partitions
                    ValidatePartitions(value);
                    this.m_blobPartitions = value;

                    //Set num partitions
                    this.m_numPartitions = (uint)this.m_blobPartitions.Count;
                }
                else
                {
                    throw new InvalidOperationException("BlobPartitions cannot be changed when " +
                                                        "blob partitions constructor is not used");
                }
            }
        }

        /// <summary>
        /// Reutnrs a list of ICloudBlobs
        /// </summary>
        public List<ICloudBlob> BlobList
        {
            get
            {
                if (this.m_constructorUsed == BlobProviderConstructorType.ReadBlobList)
                {
                    return this.m_blobList;
                }
                else
                {
                    throw new InvalidOperationException("BlobList cannot be obtained when " +
                                                        "blob list constructor is not used");
                }
            }
            set
            {
                if (this.m_constructorUsed == BlobProviderConstructorType.ReadBlobList)
                {
                    //Validate the blob list
                    ValidateBlobList(value);
                    this.m_blobList = value;

                    //Set to null so that the Partitions will be recomputed
                    this.m_blobPartitions = null;
                }
                else
                {
                    throw new InvalidOperationException("BlobList cannot be changed when " +
                                                        "blob list constructor is not used");
                }
            }
        }

        /// <summary>
        /// Specifies the output blob. Valid only on an output data provider. 
        /// </summary>
        public ICloudBlob OutputBlob
        {
            get
            {
                if (this.m_constructorUsed == BlobProviderConstructorType.Write)
                {
                    return this.m_outputBlob;
                }
                else
                {
                    throw new InvalidOperationException("Output blob cannot be obtained when " +
                                                        "output blob constructor is not used");
                }
            }
        }

        /// <summary>
        /// Affinities is invalid on the blob data provider. It always returns null.
        /// </summary>
        public String[] Affinities
        {
            get
            {
                return null;
            }
        }

        /// <summary>
        /// Weight is invalid on the blob data provider. It always returns 0;
        /// </summary>
        public long Weight
        {
            get
            {
                return 0;
            }
            set
            {
                //Do nothing
            }
        }

        #endregion

        #region Validation Methods

        private void ValidatePartitions(List<XCBlobPartition> inputBlobPartitions)
        {
            if (inputBlobPartitions == null || inputBlobPartitions.Count == 0)
            {
                throw new Exception("BlobPartitions cannot be empty");
            }

            foreach (var partition in inputBlobPartitions)
            {
                if (partition == null)
                {
                    throw new Exception("Partitions in the input BlobPartitions cannot be null");
                }
            }
        }

        private void ValidateBlobList(List<ICloudBlob> inputBlobs)
        {
            if (inputBlobs == null || inputBlobs.Count == 0)
            {
                throw new Exception("Bloblist cannot be empty");
            }

            foreach (var blob in inputBlobs)
            {
                if (blob == null)
                {
                    throw new Exception("Blobs in the input bloblist cannot be null");
                }
            }
        }

        private void ValidateNumPartitions(UInt32 numPartitions)
        {
            if (numPartitions == 0)
            {
                throw new Exception("Number of partitions cannot be zero");
            }
        }

        
        private bool IsBlockBlob(ICloudBlob blob, out bool blobExists)
        {
            try
            {
                BlobRequestOptions reqOps = XCBlobHelpers.GetBlobRequestOptions();                

                //Invoke the GetProperties method
                blob.FetchAttributes();                
                    /*
                if (blob.ServiceClient.Credentials is StorageCredentials)
                {
                    Uri blobUri = new Uri(blob.ServiceClient.Credentials.TransformUri(blob.Uri.AbsoluteUri));
                    webRequest = BlobRequest.GetProperties(blobUri, reqOps.Timeout.Value.Milliseconds, null, null);
                }
                else
                {
                    //Create the request and sign with the request with the key
                    var accAndKeyCred = (StorageCredentialsAccountAndKey)blob.ServiceClient.Credentials;
                    webRequest = BlobRequest.GetProperties(blob.Uri, reqOps.Timeout.Value.Milliseconds, null, null);
                    accAndKeyCred.SignRequest(webRequest);
                }

                var resp = webRequest.GetResponse();

                //If the response is obtained, it implies that blob exists
                blobExists = true;

                //Verify for blob type by looking at response headers
                String blobType = resp.Headers["x-ms-blob-type"];
                resp.Close();
                     * */
                blobExists = true;
                return (blob.Properties.BlobType == BlobType.BlockBlob);
            }
            catch (WebException e)
            {
                HttpWebResponse response = (HttpWebResponse)e.Response;
                if (response != null &&
                    response.StatusCode == HttpStatusCode.NotFound)
                {
                    blobExists = false;
                    return false;
                }
                throw e;
            }
        }
        

        private bool BlobExists(ICloudBlob blob)
        {
            try
            {
                return blob.Exists();                
            }
            catch (StorageException )
            {
              return false;              
            }            
        }

        #endregion

        #region Partition Methods

        /// <summary>
        /// Perform initializations such as setting partition affinities
        /// </summary>
        public void InitializeAffinities()
        {
            //If the blob partitions are not computed yet or if the
            //number of partitions has been changed, find the partitions
            //This is required because we first need to know the 
            //partitions before determining the affinities.
            if (this.m_blobPartitions == null ||
                this.m_blobPartitions.Count != this.m_numPartitions)
            {
                DetermineBlobPartitions();
            }

            for (int i = 0; i < m_blobPartitions.Count; ++i)
            {
                //Set the affinities to null for now.
                this.m_blobPartitions[i].SetAffinities(null);
            }
        }

        private void DetermineBlobPartitions()
        {
            this.m_blobPartitions = new List<XCBlobPartition>();

            List<ulong>[] blockSizes = new List<ulong>[m_blobList.Count];
            ulong numBlocks = 0;

            //Find the sizes of all the blocks in all the blobs.
            for (int i = 0; i < m_blobList.Count; ++i)
            {
                ICloudBlob blob = m_blobList[i];
                if (blob == null) continue;
                /*

                //Get the block list
                IEnumerable<ListBlockItem> blockItems = null;
                try
                {
                    //blockItems = blob.DownloadBlockList(AccessCondition.GenerateEmptyCondition(), XCBlobHelpers.GetBlobRequestOptions());
                    blockItems = blob.DownloadBlockList();
                }
                catch (Exception e)
                {
                    throw new Exception("Failed to download blocklist for blob: " + blob.Uri, e);
                }
                blockSizes[i] = new List<ulong>();
                foreach (var block in blockItems)
                {
                    blockSizes[i].Add((ulong)block.Length);
                    ++numBlocks;
                }
                 * */
                blockSizes[i] = new List<ulong>();
                blockSizes[i].Add((ulong)blob.Properties.Length);
                ++numBlocks;
            }

            uint effNumPartitions = m_numPartitions;
            if (numBlocks < m_numPartitions)
            {
                //If number of blocks is less than number of partitions, then
                //the effective number of partitions is number of blocks
                effNumPartitions = (uint)numBlocks;
            }

            ulong numBlocksInPar, curLow = 0, curHigh = 0, numBlocksConsumed = 0;
            int blobIndex = 0, blockIndex = 0;

            //Do an equi-partition of the blobs. The division is done on 
            //block boundaries.
            for (uint i = 0; i < effNumPartitions; ++i)
            {
                List<XCBlobRange> blobRanges = new List<XCBlobRange>();

                //Determine the number of blocks for this partition
                if (i == effNumPartitions - 1)
                {
                    //last partition
                    numBlocksInPar = (ulong)(numBlocks - numBlocksConsumed);
                }
                else
                {
                    //Share the remaining blocks equally among remaining partitions
                    numBlocksInPar = (ulong)((numBlocks - numBlocksConsumed) / (effNumPartitions - i));
                }

                //Assign the specified number of blocks to the partition
                for (ulong j = 0; j < numBlocksInPar; ++j)
                {
                    //If we reached the end of a blob
                    if (blockIndex == blockSizes[blobIndex].Count)
                    {
                        //If the current range is not an empty range
                        if (curHigh > curLow)
                        {
                            //Create a new range and add to the list of blob ranges
                            blobRanges.Add(new XCBlobRange(m_blobList[blobIndex], curLow, curHigh - 1));
                        }

                        //Advance to the next blob in the list
                        while (true)
                        {
                            ++blobIndex;
                            curHigh = 0;
                            blockIndex = 0;
                            curLow = 0;
                            if (blockSizes[blobIndex].Count > 0)
                            {
                                break;
                            }
                        }
                    }

                    //Advance to the next block in the blob
                    if (blockSizes[blobIndex].Count > 0)
                    {
                        curHigh += blockSizes[blobIndex][blockIndex];
                        ++blockIndex;
                    }
                }

                numBlocksConsumed += numBlocksInPar;

                //If the last blob range is not empty
                if (curHigh > curLow)
                {
                    //Create the last blob range and add it to the list of blob ranges
                    blobRanges.Add(new XCBlobRange(m_blobList[blobIndex], curLow, curHigh - 1));
                }

                //Create a new blob partition and add the partition to partitions
                m_blobPartitions.Add(new XCBlobPartition(blobRanges));

                curLow = curHigh;
            }
        }

        /// <summary>
        /// This will return the list of blob partitions. It will also set the
        /// affinities of each data partition
        /// </summary>
        public IEnumerable<IXCDataPartition> Partitions
        {
            get
            {
                //If the blob partitions are not computed yet or if the
                //number of partitions has been changed, find the partitions
                if (this.m_blobPartitions == null ||
                    this.m_blobPartitions.Count != this.m_numPartitions)
                {
                    DetermineBlobPartitions();
                }

                for (int i = 0; i < m_blobPartitions.Count; ++i)
                {
                    yield return m_blobPartitions[i];
                }
            }
        }

        #endregion

        #region GetRecords

        /// <summary>
        /// Uses the Azure GetBlob API and returns all the data present in
        /// the blob partition which can contain multiple blob ranges.
        /// </summary>
        /// <returns>An enumerable of blob's byte stream</returns>
        public IEnumerable<byte[]> GetRecords()
        {
            //Get the data present in all the partitions
            if (XCBlobHelpers.UseParallelRead)
            {
                this.m_blobReader = new XCBlobReader();
                List<XCBlobRange> allBlobRanges = new List<XCBlobRange>();
                foreach (var partition in Partitions)
                {
                    foreach (XCBlobRange range in partition.Partitions)
                    {
                        allBlobRanges.Add(range);
                    }
                }
                var records = this.m_blobReader.GetRecords(allBlobRanges);
                foreach (var rec in records)
                {
                    yield return rec;
                }
            }
            else
            {
                foreach (var partition in Partitions)
                {
                    this.m_curReadBlobPartition = ((XCBlobPartition)partition);
                    var partStream = this.m_curReadBlobPartition.GetRecords();

                    foreach (var buffer in partStream)
                    {
                        yield return buffer;
                    }
                }
            }
        }

        #endregion

        #region WriteRecords

        /// <summary>
        /// Writes the given data to the outputBlob but does not commit the data
        /// </summary>
        /// <param name="source">The source byte stream that 
        /// needs to be written to the blob</param>
        /// <param name="partitionIndex">The index of the
        /// writer vertex performing this write.</param>
        /// <returns>The list of block Ids written to the output blob</returns>
        public Object WriteRecords(IEnumerable<byte[]> source, long partitionIndex)
        {
            CloudBlockBlob blockBlob = this.OutputBlob as CloudBlockBlob;
            List<string> blockIdList = new List<string>();

            //Increase the default connection limit as we perform multiple async writes
            ServicePointManager.DefaultConnectionLimit = XCBlobHelpers.DefaultConnectionLimit;

            //Turn off 100-continue to save 1 roundtrip
            ServicePointManager.Expect100Continue = false;

            //ServicePointManager.UseNagleAlgorithm = false;

            this.m_pendingAsyncWrites = 0;

            long totalBytesWritten = 0;
            int seqNum = 0;

            if (source == null)
            {
                throw new Exception("XcBlobProvider.WriteRecords: source cannot be null");
            }

            foreach (byte[] data in source)
            {
                lock (this)
                {
                    ++this.m_pendingAsyncWrites;

                    //Limit the number of outstanding async writes
                    while (this.m_pendingAsyncWrites > XCBlobHelpers.DefaultConnectionLimit)
                    {
                        Monitor.Wait(this);
                    }
                }

                if (data == null)
                {
                    throw new Exception("XcBlobProvider.WriteRecords: source cannot contain null byte stream");
                }

                VerifyAndThrowException();

                //Set the block ID as the MD5 of the data appended by a GUID to avoid collissions
                string curBlockId = XCBlobHelpers.MD5(data) + Guid.NewGuid().ToString("N");

                MemoryStream stream = new MemoryStream(data);

                ++seqNum;

                //Make an async call to the PutBlock to write
                //the byte stream to the blob with the given blockId
                /*
                blockBlob.BeginPutBlock(curBlockId, stream, null, AccessCondition.GenerateEmptyCondition(),
                                        XCBlobHelpers.GetBlobRequestOptions(),
                                        new AsyncCallback(this.BlobWriteAsyncCallBack),
                                        new XCBlobWriteAsyncState(blockBlob, seqNum, data.Length, curBlockId));*/

                blockBlob.BeginPutBlock(curBlockId, stream, null, new AsyncCallback(this.BlobWriteAsyncCallBack),
                                        new XCBlobWriteAsyncState(blockBlob, seqNum, data.Length, curBlockId));

                totalBytesWritten += data.Length;

                //Add the blockId to the list
                blockIdList.Add(curBlockId);
            }

            if (XCBlobHelpers.LogLevel == XCLogLevel.Verbose)
            {
                Console.WriteLine("XCBlobProvider: Waiting for all async writers to finish " +
                                  " at :" + DateTime.Now);
            }

            lock (this)
            {
                //Wait for all outstanding pending writes to finish
                while (this.m_pendingAsyncWrites > 0)
                {
                    Monitor.Wait(this);
                    VerifyAndThrowException();
                }
            }

            VerifyAndThrowException();

            if (XCBlobHelpers.LogLevel >= XCLogLevel.Minimal)
            {
                Console.WriteLine("XCBlobProvider: Finished writing " + totalBytesWritten + " bytes to " +
                                  this.m_outputBlob.Uri + " at: " + DateTime.Now);
            }

            //Return the list of block Ids which is used by MergePartitions
            //to commit the block list.
            return blockIdList;
        }

        private void VerifyAndThrowException()
        {
            //Throw exceptions if any async write failed
            if (this.m_exception != null)
            {
                throw new Exception("Failed to write output to blob:", this.m_exception);
            }
        }

        private void BlobWriteAsyncCallBack(IAsyncResult asyncResult)
        {
            XCBlobWriteAsyncState state = (XCBlobWriteAsyncState)asyncResult.AsyncState;
            try
            {
                state.BlockBlob.EndPutBlock(asyncResult);

                if (XCBlobHelpers.LogLevel == XCLogLevel.Verbose)
                {
                    Console.WriteLine("XCBlobProvider: Finished writing block of size: " + state.BlockSize +
                                      " with seqNum: " + state.SeqNum + "and blockId: " + state.BlockId +
                                      " at :" + DateTime.Now);
                }
            }
            catch (Exception e)
            {
                this.m_exception = e;
            }
            finally
            {
                lock (this)
                {
                    --this.m_pendingAsyncWrites;
                    Monitor.Pulse(this);
                }
            }
        }

        #endregion

        #region MergePartitions

        /// <summary>
        /// Commits the blocks written by WriteRecords interfaces.
        /// </summary>
        /// <param name="blockIdLists">The list of blockIds</param>
        public void MergePartitions(IEnumerable<Object> blockIdLists)
        {
            CloudBlockBlob blockBlob = this.OutputBlob as CloudBlockBlob;

            //List of all the block Ids
            List<String> allBlockIds = new List<string>();
            foreach (List<String> idList in blockIdLists)
            {
                allBlockIds.AddRange(idList);
            }

            if (XCBlobHelpers.LogLevel >= XCLogLevel.Minimal)
            {
                Console.WriteLine("XCBlobProvider: Committing " + allBlockIds.Count + " blocks to output blob:" + this.m_outputBlob.Uri);
            }

            //Use the PutBlockList API in storage client library to
            //commit the blocks written by the first stage output vertices
            //blockBlob.PutBlockList(allBlockIds, AccessCondition.GenerateEmptyCondition(), XCBlobHelpers.GetBlobRequestOptions());
            blockBlob.PutBlockList(allBlockIds);
        }

        #endregion

        #region Close
        /// <summary>
        /// Closes the Blob Provider. 
        /// </summary>
        public void Close()
        {
            if (XCBlobHelpers.UseParallelRead)
            {
                if (this.m_blobReader != null)
                {
                    this.m_blobReader.Closed = true;
                }
            }
            else
            {
                if (this.m_curReadBlobPartition != null)
                {
                    //Close required only for input blob data provider
                    this.m_curReadBlobPartition.Close();
                }
            }
        }

        #endregion
    }

    /// <summary>
    /// This represents a blob partition. A blob partition contains 
    /// multiple blob ranges
    ///</summary>
    [Serializable]
    public class XCBlobPartition : IXCDataPartition, ISerializable
    {
        #region Member Variables

        private List<XCBlobRange> m_blobRanges;

        [NonSerialized]
        private string[] m_affinities;

        [NonSerialized]
        private long m_weight;

        [NonSerialized]
        private XCBlobRange m_curReadBlobRange;

        [NonSerialized]
        private XCBlobReader m_blobReader;

        #endregion

        #region Constructors

        /// <summary>
        /// Constructor for XCBlobPartition
        /// </summary>
        /// <param name="blobRanges">A collection of blob ranges</param>
        public XCBlobPartition(List<XCBlobRange> blobRanges)
        {
            this.m_blobRanges = blobRanges;

            //The default weight of the partition is the sum of 
            //weight of the ranges. A user can override it.
            this.m_weight = blobRanges.Sum(x => x.Weight);
        }

        #endregion

        #region Serialization Methods

        /// <summary>
        /// XCBlobPartition Constructor
        /// </summary>
        /// <param name="info">The Serialization info</param>
        /// <param name="context">The Streaming context</param>
        protected XCBlobPartition(SerializationInfo info, StreamingContext context)
        {
            //Deserialize the blob ranges
            this.m_blobRanges = (List<XCBlobRange>)info.GetValue("m_blobRanges", typeof(List<XCBlobRange>));

            //Deserialize all the settings
            XCBlobHelpers.DeSerializeBlobSettings(info);
        }
        /// <summary>
        /// GEt object data
        /// </summary>
        /// <param name="info">The Serialization info</param>
        /// <param name="context">The Streaming context</param>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            //Serialize only the blob ranges. Other properties are not required
            info.AddValue("m_blobRanges", this.m_blobRanges);

            //Serialize all the settings
            XCBlobHelpers.SerializeBlobSettings(info);
        }

        #endregion

        #region Properties

        /// <summary>
        /// Represents the affinities of the partition.
        /// </summary>
        public String[] Affinities
        {
            get
            {
                return this.m_affinities;
            }
        }

        internal void SetAffinities(String[] affinities)
        {
            this.m_affinities = affinities;
        }

        /// <summary>
        /// Represents the size of the data represented by
        /// the partition.
        /// </summary>
        public Int64 Weight
        {
            get
            {
                return this.m_weight;
            }
            set
            {
                this.m_weight = value;
            }
        }

        #endregion

        #region DataProvider Methods

        /// <summary>
        /// List of blob ranges
        /// </summary>
        public IEnumerable<IXCDataPartition> Partitions
        {
            get
            {
                foreach (var range in this.m_blobRanges)
                {
                    yield return range;
                }
            }
        }

        /// <summary>
        /// Gets a enumerated Byte Array
        /// </summary>
        /// <returns></returns>
        public IEnumerable<byte[]> GetRecords()
        {
            if (XCBlobHelpers.UseParallelRead)
            {
                this.m_blobReader = new XCBlobReader();
                var records = this.m_blobReader.GetRecords(this.m_blobRanges);
                foreach (var rec in records)
                {
                    yield return rec;
                }
            }
            else
            {
                foreach (var range in this.m_blobRanges)
                {
                    this.m_curReadBlobRange = range;
                    var rangeStream = this.m_curReadBlobRange.GetRecords();
                    foreach (var buffer in rangeStream)
                    {
                        yield return buffer;
                    }
                }
            }
        }

        /// <summary>
        /// Closes the DataProvider
        /// </summary>
        public void Close()
        {
            if (XCBlobHelpers.UseParallelRead)
            {
                if (this.m_blobReader != null)
                {
                    this.m_blobReader.Closed = true;
                }
            }
            else
            {
                if (this.m_curReadBlobRange != null)
                {
                    this.m_curReadBlobRange.Close();
                }
            }
        }

        #endregion
    }

    /// <summary>
    /// Represents a blob range. It contains the start
    /// and end byte position in the blob.
    /// </summary>
    [Serializable]
    public class XCBlobRange : IXCDataPartition, IXCRangePartition<ulong, ICloudBlob>, ISerializable
    {
        #region Member Variables

        private ICloudBlob m_blob;
        private ulong m_low;
        private ulong m_high;
        private long m_weight;

        private BlockingCollection<byte[]> m_queue;
        private Exception m_exception;
        private bool m_isClosed;
        private XCBlobReader m_blobReader;

        //Constants used by serialization/deserialization routines
        private const string s_low = "Low";
        private const string s_high = "High";
        private const string s_weight = "Weight";

        //Constants used for reading data from a blob range
        internal static int s_readBufferSize = 2 * 1024 * 1024;  //2MB
        internal static int s_maxItemsToBuffer = 50;

        #endregion

        #region Constructors

        /// <summary>
        /// Constructor for blob range
        /// </summary>
        /// <param name="blob">The Cloud blob object containing the cloudblob and 
        /// the storage account key</param>
        /// <param name="low">start byte position of the range</param>
        /// <param name="high">end byte position of the range</param>
        public XCBlobRange(ICloudBlob blob, ulong low, ulong high)
        {
            if (blob == null)
            {
                throw new Exception("blob cannot be null");
            }
            this.m_blob = blob;

            if (low > high)
            {
                throw new Exception("High value in the blob range should be " +
                                    "greater than or equal to low value");
            }
            this.m_low = low;
            this.m_high = high;
            this.m_weight = (long)(high - low) + 1;
        }

        #endregion

        #region Serialization Methods

        /// <summary>
        /// XCBlobRange constructor
        /// </summary>
        /// <param name="info">The SerializationInfo </param>
        /// <param name="context">The StreamingContext </param>
        protected XCBlobRange(SerializationInfo info, StreamingContext context)
        {
            //Deserialization
            this.m_blob = XCBlobHelpers.DeSerializeBlob(info);
            this.m_low = info.GetUInt64(s_low);
            this.m_high = info.GetUInt64(s_high);
            this.m_weight = info.GetInt64(s_weight);
        }

        /// <summary>
        /// GetObjectData
        /// </summary>
        /// <param name="info">The SerializationInfo </param>
        /// <param name="context">The StreamingContext </param>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            //Serialize blob, low, high and weight
            XCBlobHelpers.SerializeBlob(info, this.m_blob);
            info.AddValue(s_low, this.m_low);
            info.AddValue(s_high, this.m_high);
            info.AddValue(s_weight, this.m_weight);
        }

        #endregion

        #region Properties

        /// <summary>
        /// Represents the start byte position in the range
        /// </summary>
        public ulong Low
        {
            get
            {
                return this.m_low;
            }
        }

        /// <summary>
        /// Represents the start byte position in the range
        ///</summary>
        public ulong High
        {
            get
            {
                return this.m_high;
            }
        }

        /// <summary>
        /// The resource for a blob range partition is the cloud blob 
        /// object representing the blob
        /// </summary>
        public ICloudBlob Resource
        {
            get
            {
                return this.m_blob;
            }
        }

        /// <summary>
        ///  A blob range cannot be partitioned be further. This
        ///  always returns null;
        /// </summary>
        public IEnumerable<IXCDataPartition> Partitions
        {
            get
            {
                return null;
            }
        }

        /// <summary>
        /// Represents the weight of the blob range
        /// </summary>
        public Int64 Weight
        {
            get
            {
                return this.m_weight;
            }
            set
            {
                this.m_weight = value;
            }
        }

        /// <summary>
        /// Affinities are invalid for a blob range. This
        /// always returns null.
        /// </summary>
        public String[] Affinities
        {
            get
            {
                return null;
            }
        }

        #endregion

        #region DataProvider Methods

        private Stream GetBlobReadResponseStream()
        {

            return this.m_blob.OpenRead();
            /*
            CloudBlockBlob blockBlob = this.m_blob.ToBlockBlob;            

            //Blob request options
            BlobRequestOptions reqOps = XCBlobHelpers.GetBlobRequestOptions();
            long totalBytesToRead = (long)((this.m_high - this.m_low) + 1);
            HttpWebRequest webRequest;

            //Use Blob protocol GET API in storage client library to read blob range
            //from low to high byte position.
            if (blockBlob.ServiceClient.Credentials is StorageCredentialsSharedAccessSignature)
            {
                //Use the SAS in the blob
                Uri blobUri = new Uri(blockBlob.ServiceClient.Credentials.TransformUri(blockBlob.Uri.AbsoluteUri));
                webRequest = BlobRequest.Get(blobUri, reqOps.Timeout.Value.Milliseconds,
                                             null, (long)this.m_low, totalBytesToRead, null);
            }
            else
            {
                var accAndKeyCred = (StorageCredentialsAccountAndKey)blockBlob.ServiceClient.Credentials;
                webRequest = BlobRequest.Get(blockBlob.Uri, reqOps.Timeout.Value.Milliseconds,
                                             null, (long)this.m_low, totalBytesToRead, null);
                //Sign the request with the key
                accAndKeyCred.SignRequest(webRequest);
            }
            try
            {
                return webRequest.GetResponse().GetResponseStream();
            }
            catch (WebException e)
            {
                HttpWebResponse response = (HttpWebResponse)e.Response;
                if (response != null)
                {
                    Console.WriteLine(String.Format("XCBlobProvider: Failed to obtain blob read response stream: " +
                                                    "StatusCode = {0}, StatusDescription = \"{1}\"",
                                                    (int)response.StatusCode, response.StatusDescription));
                }
                throw e;
            }
             * */
        }

        private void AddBufferToQueue(byte[] buffer)
        {
            //Keep trying to add the buffer to the queue until the
            //add is successful(queue is not full)
            while (!this.m_queue.TryAdd(buffer, TimeSpan.FromSeconds(1)))
            {
                //If the main thread is closed, do not try more as this
                //is a bounded queue
                if (this.m_isClosed)
                {
                    return;
                }
            }
        }

        private void ReadBlobChunks()
        {
            try
            {
                if (this.m_high < this.m_low)
                {
                    throw new Exception("High value: " + this.m_high + " is less than: " + this.m_low);
                }

                //Issue one request to the server to read the
                //entire blob range and get the response stream
                Stream stream = GetBlobReadResponseStream();
                long totalBytesToRead = (long)((this.m_high - this.m_low) + 1);

                //Find the current buffer size
                int curBuffSize = s_readBufferSize;
                if (totalBytesToRead < s_readBufferSize)
                {
                    curBuffSize = (int)totalBytesToRead;
                }

                byte[] buffer = new byte[curBuffSize];
                int offset = 0;

                //Read data from the stream
                int numRead = stream.Read(buffer, offset, buffer.Length);
                long totalRead = numRead;

                //Read until all the bytes in the range are read
                while (totalRead < totalBytesToRead)
                {
                    //If the main thread is closed, return
                    if (this.m_isClosed)
                    {
                        return;
                    }

                    if ((offset + numRead) == curBuffSize)
                    {
                        //Add the buffer to the queue
                        AddBufferToQueue(buffer);

                        //Reset offset and create a new buffer with 
                        //the appropriate buffer size
                        offset = 0;
                        curBuffSize = s_readBufferSize;
                        if ((totalBytesToRead - totalRead) < s_readBufferSize)
                        {
                            curBuffSize = (int)(totalBytesToRead - totalRead);
                        }
                        buffer = new byte[curBuffSize];
                    }
                    else
                    {
                        offset += numRead;
                    }

                    //Read data from the stream
                    numRead = stream.Read(buffer, offset, buffer.Length - offset);
                    totalRead += numRead;
                }

                //Add the last buffer
                AddBufferToQueue(buffer);

                //Close the stream
                stream.Close();
            }
            catch (Exception e)
            {
                //Propogate the exception to the main thread
                this.m_exception = e;
            }
            finally
            {
                //Always set the complete adding so that the main thread unblocks
                this.m_queue.CompleteAdding();
            }
        }

        private IEnumerable<byte[]> GetRecordsUsingSingleThread()
        {
            this.m_isClosed = false;
            this.m_queue = new BlockingCollection<byte[]>(s_maxItemsToBuffer);

            //Create a thread to read the blob chunks from the response stream
            Thread thread = new Thread(new ThreadStart(ReadBlobChunks));

            thread.Start();

            long bytesRead = 0;

            try
            {
                foreach (byte[] buffer in this.m_queue.GetConsumingEnumerable())
                {
                    bytesRead += buffer.Length;

                    if (XCBlobHelpers.LogLevel == XCLogLevel.Verbose)
                    {
                        Console.WriteLine("XCBlobProvider: Read " + buffer.Length + " bytes from blobRange: " +
                                          this.m_blob.Uri + "[" + this.m_low + "," + this.m_high + "] at: " + DateTime.Now);
                    }

                    VerifyAndThrowException();

                    yield return buffer;
                }

                //Propogate any exceptions thrown by the thread to the application
                VerifyAndThrowException();
            }
            finally
            {
                if (XCBlobHelpers.LogLevel >= XCLogLevel.Minimal)
                {
                    Console.WriteLine("XCBlobProvider: Finished reading " + bytesRead + " bytes from blobRange: " + this.m_blob.Uri +
                                      "[" + this.m_low + "," + this.m_high + "] at: " + DateTime.Now);
                }
            }
        }

        private void VerifyAndThrowException()
        {
            if (this.m_isClosed)
            {
                throw new Exception("Cannot read more data after Close is called");
            }

            if (this.m_exception != null)
            {
                throw new Exception("Failed to read blob range", this.m_exception);
            }
        }

        /// <summary>
        /// Enumerable Byte data
        /// </summary>
        /// <returns>IEnumberable of Bytes </returns>
        public IEnumerable<byte[]> GetRecords()
        {
            if (XCBlobHelpers.UseParallelRead)
            {
                this.m_blobReader = new XCBlobReader();

                var blobRanges = new List<XCBlobRange>();
                blobRanges.Add(this);

                return this.m_blobReader.GetRecords(blobRanges);
            }
            else
            {
                return GetRecordsUsingSingleThread();
            }
        }

        /// <summary>
        /// Closes the XCBlobRange DataProvider
        /// </summary>
        public void Close()
        {
            if (XCBlobHelpers.UseParallelRead)
            {
                this.m_blobReader.Closed = true;
            }
            else
            {
                this.m_isClosed = true;
            }
        }

        #endregion
    }

    internal class XCBlobReader
    {
        #region Member Variables

        private bool m_isClosed;

        private Exception m_exception;

        //The format of the entries in the table is : 
        //<BlobUri + Range-Id + Range-Low, <Range-High, Data-Buffer>>
        private Dictionary<string, KeyValuePair<ulong, byte[]>> m_dataTable;

        //Number of pending async readers
        private int m_curPendingAsyncReaders;

        private IEnumerable<XCBlobRange> m_blobRanges;
        internal static int s_readBufferSize = 4 * 1000 * 1000;  //4MB

        #endregion

        #region Properties

        internal bool Closed
        {
            get
            {
                return this.m_isClosed;
            }
            set
            {
                this.m_isClosed = value;
            }
        }

        internal Exception Exception
        {
            get
            {
                return this.m_exception;
            }
        }

        #endregion

        #region GetRecords

        public IEnumerable<byte[]> GetRecords(IEnumerable<XCBlobRange> blobRanges)
        {
            //Initializations
            this.m_isClosed = false;
            this.m_exception = null;
            this.m_blobRanges = blobRanges;
            this.m_dataTable = new Dictionary<string, KeyValuePair<ulong, byte[]>>();

            //Increase the default connection limit as we perform multiple async reads
            ServicePointManager.DefaultConnectionLimit = XCBlobHelpers.DefaultConnectionLimit;

            //Create a thread to read the blob chunks from the response stream
            Thread thread = new Thread(new ThreadStart(ReadBlobInChunks));

            thread.Start();

            int rangeNum = 0;

            foreach (var range in blobRanges)
            {
                ulong bytesRead = 0, curChunkPos = range.Low;

                var bufferInfo = new KeyValuePair<ulong, byte[]>(0, null);

                try
                {
                    while (curChunkPos <= range.High)
                    {
                        if (XCBlobHelpers.LogLevel == XCLogLevel.Verbose)
                        {
                            Console.WriteLine("Will read byte " + curChunkPos + " from " + range.Resource.Uri +
                                              "at: " + DateTime.Now);
                        }

                        lock (this)
                        {
                            //Wait until the current chunk is available
                            String curKey = range.Resource.Uri.ToString() + rangeNum.ToString() + curChunkPos.ToString();

                            while (!this.m_isClosed &&
                                   !this.m_dataTable.TryGetValue(curKey, out bufferInfo))
                            {
                                VerifyAndThrowException();
                                Monitor.Wait(this, TimeSpan.FromMilliseconds(100));
                            }

                            //Remove the buffer from the table
                            this.m_dataTable.Remove(curKey);
                        }

                        if (XCBlobHelpers.LogLevel == XCLogLevel.Verbose)
                        {
                            Console.WriteLine("Remove data starting at byte " + curChunkPos + " from " + range.Resource.Uri +
                                              "at: " + DateTime.Now);
                        }

                        VerifyAndThrowException();

                        bytesRead += (ulong)bufferInfo.Value.Length;

                        yield return bufferInfo.Value;

                        //Proceed to the next chunk
                        curChunkPos = bufferInfo.Key + 1;
                    }

                    if (!this.m_isClosed && bytesRead != (range.High - range.Low) + 1)
                    {
                        throw new Exception("Blob read internal error. Did not read complete data:" +
                                            "Read:" + bytesRead + ", Actual:" + ((range.High - range.Low) + 1));
                    }

                    VerifyAndThrowException();
                }
                finally
                {
                    if (XCBlobHelpers.LogLevel >= XCLogLevel.Minimal)
                    {
                        Console.WriteLine("XCBlobProvider: Finished reading " + bytesRead + " bytes from blobRange: " + range.Resource.Uri +
                                          "[" + range.Low + "," + range.High + "] at: " + DateTime.Now);
                    }
                }

                ++rangeNum;
            }
        }

        private void ReadBlobInChunks()
        {
            int rangeNum = 0;

            //Read all the blob ranges
            foreach (var range in this.m_blobRanges)
            {
                try
                {
                    if (range.High < range.Low)
                    {
                        throw new Exception("High value: " + range.High + " is less than: " + range.Low);
                    }

                    ulong curPos = range.Low, nextHigh = 0;

                    while (curPos <= range.High)
                    {
                        //Check if the read is closed or if any async read failed
                        if (this.m_isClosed || this.m_exception != null)
                        {
                            return;
                        }

                        //Create a new blobStream for every parallel read
                        Stream blobStream = range.Resource.OpenRead();

                        //Seek to the appropriate pos
                        long pos = blobStream.Seek((long)curPos, System.IO.SeekOrigin.Begin);

                        //Set the high for the current read
                        nextHigh = curPos + (ulong)(s_readBufferSize - 1);

                        if (nextHigh > range.High)
                        {
                            nextHigh = range.High;
                        }

                        lock (this)
                        {
                            //Limit the number of pending async readers
                            while (this.m_curPendingAsyncReaders > XCBlobHelpers.DefaultConnectionLimit)
                            {
                                if (this.m_isClosed || this.m_exception != null)
                                {
                                    return;
                                }
                                Monitor.Wait(this);
                            }
                            ++this.m_curPendingAsyncReaders;
                        }

                        byte[] buff = new byte[1 + (nextHigh - curPos)];

                        if (XCBlobHelpers.LogLevel == XCLogLevel.Verbose)
                        {
                            Console.WriteLine("XCBlobProvider: ReadBlobInChunks Issuing read from blobRange: " +
                                              range.Resource.Uri + "[" + curPos + "," + nextHigh + "] at: " + DateTime.Now);
                        }

                        blobStream.ReadTimeout = XCBlobHelpers.TimeOutInSeconds * 1000;

                        //Make an async read call
                        blobStream.BeginRead(buff, 0, buff.Length, new AsyncCallback(BlobReadCallBack),
                                             new XCBlobReadAsyncState(range.Resource, blobStream,
                                                                      curPos, nextHigh, buff, rangeNum, 0));

                        curPos = 1 + nextHigh;
                    }

                    if (XCBlobHelpers.LogLevel == XCLogLevel.Verbose)
                    {
                        Console.WriteLine("XCBlobProvider: ReadBlob: Finished submitting all async reads:" +
                                          range.Resource.Uri + "[" + range.Low + "," + range.High + "] at: " + DateTime.Now);
                    }
                }
                catch (Exception e)
                {
                    this.m_exception = e;
                }
                ++rangeNum;
            }
        }

        private void VerifyAndThrowException()
        {
            if (this.m_isClosed)
            {
                throw new Exception("Cannot read more data after Close is called");
            }

            if (this.m_exception != null)
            {
                throw new Exception("Failed to read blob range", this.m_exception);
            }
        }

        private void BlobReadCallBack(IAsyncResult asyncResult)
        {
            XCBlobReadAsyncState state = (XCBlobReadAsyncState)asyncResult.AsyncState;

            try
            {
                //Finish the async read operation
                int bytesRead = 0;

                try
                {
                    bytesRead = state.BlobStream.EndRead(asyncResult);
                }
                catch (Exception e)
                {
                    if (state.RetryCount == XCBlobHelpers.MaxRetries)
                    {
                        throw e;
                    }

                    if (XCBlobHelpers.LogLevel == XCLogLevel.Verbose)
                    {
                        Console.WriteLine("Retrying: " + state.RetryCount + " for low " + state.Low + ": " +
                                          state.Blob.Uri + " at: " + DateTime.Now);
                    }
                }

                if (XCBlobHelpers.LogLevel == XCLogLevel.Verbose)
                {
                    Console.WriteLine("XCBlobProvider: BlobReadCallBack " + bytesRead + " bytes from blobRange: " +
                                      state.Blob.Uri + "[" + state.Low + "," + state.High + "] at: " + DateTime.Now);
                }

                ulong newHigh = state.Low + (ulong)(bytesRead - 1);

                if (newHigh < state.High)
                {
                    //If all the bytes in the current read range are not read
                    //issue a new async request to read the remaining read range
                    Stream blobStream = state.BlobStream;

                    lock (this)
                    {
                        ++this.m_curPendingAsyncReaders;
                    }

                    //Issue a new async request to read the remaining part of the blob
                    int bytesToRead = (int)(state.High - newHigh);

                    int curPos = state.Data.Length - bytesToRead;

                    if (XCBlobHelpers.LogLevel == XCLogLevel.Verbose)
                    {
                        Console.WriteLine("XCBlobProvider: BlobReadCallBack: Issuing new async call to read" + bytesToRead +
                                          state.Blob.Uri + "[" + (newHigh + 1) + "," + state.High + "] at: " + DateTime.Now);
                    }

                    //Issue a new async read request
                    blobStream.BeginRead(state.Data, curPos, bytesToRead,
                                         new AsyncCallback(BlobReadCallBack),
                                         new XCBlobReadAsyncState(state.Blob, blobStream, newHigh + 1,
                                                                  state.High, state.Data, state.BlobReadIdentifier, state.RetryCount + 1));
                }
                else
                {
                    if (newHigh != state.High)
                    {
                        throw new Exception("Blob read internal error: " + newHigh + "," + state.High);
                    }

                    ulong lowPos = state.High - (ulong)(state.Data.Length - 1);

                    String key = state.Blob.Uri.ToString() + state.BlobReadIdentifier.ToString() + lowPos.ToString();

                    lock (this)
                    {
                        //Add the buffer to the hash table
                        this.m_dataTable.Add(key, new KeyValuePair<ulong, byte[]>(state.High, state.Data));
                    }
                }
            }
            catch (Exception e)
            {
                this.m_exception = e;
            }
            finally
            {
                lock (this)
                {
                    //Release a pending async reader
                    --this.m_curPendingAsyncReaders;

                    Monitor.PulseAll(this);
                }
            }
        }

        #endregion
    }

    /// <summary>
    /// Various XCBlobHelpers helpers
    /// </summary>
    public static class XCBlobHelpers
    {
        #region Configuration Parameters

        //Constants for blob request options
        private static int timeOutInSeconds = 40;
        private static int defaultConnectionLimit = 48;

        //Timeout in seconds
        public static int TimeOutInSeconds
        {
            get
            {
                return timeOutInSeconds;
            }
            set
            {
                if (value < 1)
                {
                    throw new Exception("Timeout cannot be less than 1");
                }

                timeOutInSeconds = value;
            }
        }

        /// <summary>
        /// The default connection limit
        /// </summary>
        public static int DefaultConnectionLimit
        {
            get
            {
                return defaultConnectionLimit;
            }
            set
            {
                if (value < 1)
                {
                    throw new Exception("DefaultConnectionLimit cannot be less than 1");
                }

                defaultConnectionLimit = value;
            }
        }

        /// <summary>
        /// WaitTime in seconds between retries
        /// </summary>
        public static uint WaitTimeInSecsBetweenRetries = 5;
        /// <summary>
        /// Maximum number of retries
        /// </summary>
        public static uint MaxRetries = 2;
        /// <summary>
        /// The default WebProxy
        /// </summary>
        public static WebProxy DefaultProxy = null;
        /// <summary>
        /// The logging level to use
        /// </summary>
        public static XCLogLevel LogLevel = XCLogLevel.Minimal;
        /// <summary>
        /// Should use parallel reads
        /// </summary>
        public static bool UseParallelRead = true;

        #endregion

        #region Constants

        internal const int MaxBlockSize = 4 * 1024 * 1024;    //4MB

        //Constants used by serialization/deserialization routines
        internal const string s_isNull = "IsNull";
        internal const string s_isKeyBased = "IsKeyBased";
        internal const string s_uri = "Uri";
        internal const string s_accountName = "AccountName";
        internal const string s_key = "Key";
        internal const string s_blobAddress = "BlobAddress";
        internal const string s_timeOut = "TimeOut";
        internal const string s_maxRetries = "MaxRetries";
        internal const string s_waitTime = "WaitTime";
        internal const string s_isProxyNull = "IsProxyNull";
        internal const string s_webProxy = "WebProxy";
        internal const string s_connectionLimit = "ConnectionLimit";
        internal const string s_logLevel = "LogLevel";
        internal const string s_blobConstructorType = "BlobConstructorType";

        #endregion

        #region Methods

        internal static BlobRequestOptions GetBlobRequestOptions()
        {
            BlobRequestOptions reqOps = new BlobRequestOptions();

            //Set the timeout
            reqOps.ServerTimeout = TimeSpan.FromSeconds(TimeOutInSeconds);
            
            //Set the retry policy
            //reqOps.RetryPolicy = RetryPolicies.Retry((int)MaxRetries, TimeSpan.FromSeconds(WaitTimeInSecsBetweenRetries));
            
            reqOps.RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(1), 10);
            return reqOps;
        }

        internal static void SerializeBlob(SerializationInfo info, ICloudBlob blob)
        {
            if (blob == null)
            {
                info.AddValue(s_isNull, true);
            }
            else
            {
                info.AddValue(s_isNull, false);

                if (blob.ServiceClient.Credentials.IsSharedKey)
                {
                    //Key based
                    info.AddValue(s_isKeyBased, true);

                    //Base Uri
                    info.AddValue(s_uri, blob.ServiceClient.BaseUri);

                    //Account name
                    info.AddValue(s_accountName, blob.ServiceClient.Credentials.AccountName);

                    //Key
                    String key = ((StorageCredentials)blob.ServiceClient.Credentials).ExportBase64EncodedKey();
                    info.AddValue(s_key, key);

                    //Blob path containing the container
                    String blobPath = blob.Uri.AbsolutePath;

                    if (blobPath.StartsWith("/" + blob.ServiceClient.Credentials.AccountName))
                    {
                        //Path Style. Uri looks like /<account>/<container>/<blobpath>
                        int index = blobPath.IndexOf('/', 1);
                        if (index == -1)
                        {
                            throw new Exception("Invalid blobPath :" + blobPath);
                        }
                        blobPath = blobPath.Substring(index + 1);
                    }
                    else
                    {
                        //DNS style. Uri looks like /<container>/<blobpath>
                        blobPath = blobPath.Substring(1);
                    }
                    info.AddValue(s_blobAddress, blobPath);
                }
                else if(blob.ServiceClient.Credentials.IsSAS)
                {
                    //Signature based
                    info.AddValue(s_isKeyBased, false);
                    
                    //Signed Uri                    
                    info.AddValue(s_uri, blob.ServiceClient.Credentials.SASToken);
                }
            }
        }

        internal static ICloudBlob DeSerializeBlob(SerializationInfo info)
        {
            bool isNull = info.GetBoolean(s_isNull);

            if (isNull)
            {
                return null;
            }
            else
            {
                bool isKeyBased = info.GetBoolean(s_isKeyBased);
                if (isKeyBased)
                {
                    Console.WriteLine("Blob is Key based");                    
                    Uri baseUri = (Uri)info.GetValue(s_uri, typeof(Uri));
                    Console.WriteLine("Got base URI " + baseUri.AbsoluteUri);                    
                    String accountName = info.GetString(s_accountName);
                    String key = info.GetString(s_key);
                    String blobPath = info.GetString(s_blobAddress);

                    StorageCredentials credentials = new StorageCredentials(accountName, key);
                    CloudBlobClient blobClient = new CloudBlobClient(baseUri, credentials);                    

                    return blobClient.GetBlobReferenceFromServer(new Uri(baseUri.AbsoluteUri +  blobPath));
                    //return blobClient.GetBlobReferenceFromServer(new StorageUri(new Uri(blobPath)));
                }
                else
                {
                    Console.WriteLine("Blob is SAS ");
                    String signedUri = info.GetString(s_uri);
                    Console.WriteLine("Got signed URI" + signedUri);                    
                    Uri baseUri = (Uri)info.GetValue(s_uri, typeof(Uri));
                    Console.WriteLine("Got base URI" + baseUri.AbsoluteUri);                    
                    StorageCredentials credentials = new StorageCredentials(signedUri);
                    CloudBlobClient blobClient = new CloudBlobClient(baseUri, credentials);
                    return blobClient.GetBlobReferenceFromServer(baseUri);
                }
            }
        }

        internal static void SerializeBlobSettings(SerializationInfo info)
        {
            info.AddValue(s_timeOut, TimeOutInSeconds);
            info.AddValue(s_maxRetries, MaxRetries);
            info.AddValue(s_waitTime, WaitTimeInSecsBetweenRetries);
            info.AddValue(s_connectionLimit, DefaultConnectionLimit);
            info.AddValue(s_logLevel, LogLevel);
            info.AddValue("UseParallelRead", UseParallelRead);
            if (DefaultProxy == null)
            {
                info.AddValue(s_isProxyNull, true);
            }
            else
            {
                info.AddValue(s_isProxyNull, false);
                info.AddValue(s_webProxy, DefaultProxy);
            }
        }

        internal static void DeSerializeBlobSettings(SerializationInfo info)
        {
            TimeOutInSeconds = info.GetInt32(s_timeOut);
            MaxRetries = info.GetUInt32(s_maxRetries);
            WaitTimeInSecsBetweenRetries = info.GetUInt32(s_waitTime);
            DefaultConnectionLimit = info.GetInt32(s_connectionLimit);
            LogLevel = (XCLogLevel)info.GetValue(s_logLevel, typeof(XCLogLevel));
            UseParallelRead = info.GetBoolean("UseParallelRead");

            bool isNull = info.GetBoolean(s_isProxyNull);

            if (!isNull)
            {
                DefaultProxy = (WebProxy)info.GetValue(s_webProxy, typeof(WebProxy));
                HttpWebRequest.DefaultWebProxy = DefaultProxy;
            }
        }

        internal static string MD5(byte[] payload)
        {
            MD5 md5 = new MD5CryptoServiceProvider();
            byte[] result = md5.ComputeHash(payload);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < result.Length; i++)
            {
                sb.Append(result[i].ToString("X2"));
            }
            return sb.ToString();
        }

        #endregion
    }

    /// <summary>
    /// The logging levels
    /// </summary>
    [Serializable]
    public enum XCLogLevel
    {
        NoLogging,
        Minimal,
        Verbose
    }

    internal class XCBlobReadAsyncState
    {
        #region Properties

        public ICloudBlob Blob
        {
            get;
            set;
        }

        public Stream BlobStream
        {
            get;
            set;
        }

        public ulong Low
        {
            get;
            set;
        }

        public ulong High
        {
            get;
            set;
        }

        public byte[] Data
        {
            get;
            set;
        }

        public int BlobReadIdentifier
        {
            get;
            set;
        }

        public int RetryCount
        {
            get;
            set;
        }

        #endregion

        #region Constructors

        public XCBlobReadAsyncState(ICloudBlob blob, Stream blobStream, ulong low, ulong high, byte[] data, int blobReadIdentifier, int retryCount)
        {
            this.Blob = blob;
            this.BlobStream = blobStream;
            this.Low = low;
            this.High = high;
            this.Data = data;
            this.BlobReadIdentifier = blobReadIdentifier;
            this.RetryCount = retryCount;
        }

        #endregion
    }

    internal class XCBlobWriteAsyncState
    {
        #region Properties

        public CloudBlockBlob BlockBlob
        {
            get;
            set;
        }

        public int SeqNum
        {
            get;
            set;
        }

        public int BlockSize
        {
            get;
            set;
        }

        public string BlockId
        {
            get;
            set;
        }

        #endregion

        #region Constructors

        public XCBlobWriteAsyncState(CloudBlockBlob blockBlob, int seqNum, int blockSize, String blockId)
        {
            this.BlockBlob = blockBlob;
            this.SeqNum = seqNum;
            this.BlockSize = blockSize;
            this.BlockId = blockId;
        }

        #endregion
    }
}
