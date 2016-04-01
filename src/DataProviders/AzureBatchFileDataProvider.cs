using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Threading;

namespace DAGBuilderDataProviders
{    

    /// <summary>
    /// Details related the the AzureBatchTask. This info is used to serialize DataProvider related data
    /// such that other task running on a different machine can get to data produced by the current running Task.
    /// </summary>
    [Serializable]
    public class AzureBatchTaskDetails: ISerializable
    {
        /// <summary>
        /// AzureBatchTaskDetails Constructor
        /// </summary>
        /// <param name="azureBatchUrl">Url for the Azure Batch account</param>
        /// <param name="accountName">Azure batch account name</param>
        /// <param name="accountKey">The azure batch account Key</param>
        /// <param name="jobName">The name of the job under which the task is running</param>
        /// <param name="taskName">The name of the task</param>
        public AzureBatchTaskDetails(string azureBatchUrl, string accountName, string accountKey, string jobName, string taskName)
        {
            this.AzureBatchUrl = azureBatchUrl;
            this.AccountName = accountName;
            this.AccountKey = accountKey;
            this.JobName = jobName;
            this.TaskName = taskName;            
        }

        /// <summary>
        /// AzureBatchTaskDetails Constructor
        /// </summary>
        /// <param name="info">The serialization info</param>
        /// <param name="context">The serialization context</param>
        public AzureBatchTaskDetails(SerializationInfo info, StreamingContext context)
        {            
            this.AzureBatchUrl = info.GetString(AzureBatchTaskDetails.batchurl);            
            string acc = info.GetString(AzureBatchTaskDetails.account);
            string key = info.GetString(AzureBatchTaskDetails.key);
            this.TaskName = info.GetString(AzureBatchTaskDetails.taskname);
            this.JobName = info.GetString(AzureBatchTaskDetails.job);

            this.Credentials = new BatchSharedKeyCredentials(this.AzureBatchUrl, acc, key);
            this.IsReader = true;
        }

        /// <summary>
        /// Gets the Job related data
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {            
            info.AddValue(AzureBatchTaskDetails.batchurl, this.AzureBatchUrl);            
            info.AddValue(AzureBatchTaskDetails.account, this.AccountName);            
            info.AddValue(AzureBatchTaskDetails.key, this.AccountKey);            
            info.AddValue(AzureBatchTaskDetails.job, this.JobName);
            info.AddValue(AzureBatchTaskDetails.taskname, this.TaskName);
        }


        private const string batchurl = "batchurl";
        private const string account = "account";
        private const string key = "key";        
        private const string job = "job";
        private const string taskname = "taskname";

        /// <summary>
        /// Url for the Azure Batch account
        /// </summary>
        public string AzureBatchUrl { get; private set; }
        /// <summary>
        /// Azure Batch account name
        /// </summary>
        public string AccountName { get; private set; }
        /// <summary>
        /// Azure Batch account Key
        /// </summary>
        public string AccountKey { get; private set; }
        /// <summary>
        /// Azure Batch Job name
        /// </summary>
        public string JobName { get; private set; }
        /// <summary>
        /// Azure batch task name
        /// </summary>
        public string TaskName { get; private set; }
        internal BatchSharedKeyCredentials Credentials { get; set; }
        internal bool IsReader { get; set; }
    }

    /// <summary>
    /// The AzureBatch Task file reader. This reader gives the user the ability to read
    /// data from files that were produced by a previously run Azure Batch task.
    /// The Azure Batch task might have run on a differnt machine form where this task is running.
    /// </summary>
    [Serializable]
    public class BatchTaskFileReader : IDisposable
    {
        /// <summary>
        /// BatchTaskFileReader Constructor
        /// </summary>
        /// <param name="taskFileDetails">The Task File related details.</param>
        /// <param name="fileName">The name of the file to read.</param>
        public BatchTaskFileReader(AzureBatchTaskDetails taskFileDetails, string fileName)
        {
            this.TaskFileDetails = taskFileDetails;            
            this.FileName = fileName;
        }

        internal void InitializeStream()
        {
            if (!this.TaskFileDetails.IsReader) throw new InvalidOperationException("Object needs to be written to. Can only be read on the other side");

            if (this.memoryStream != null)
            {
                return;
            }

            lock (this)
            {
                if (this.memoryStream != null)
                {
                    return;
                }

                this.memoryStream = new MemoryStream();
                
                Console.WriteLine("Getting file {0}...", this.FileName);
                try
                {
                    Console.WriteLine("JobName - {0}, TaskName - {1}, File - {2}", this.TaskFileDetails.JobName, this.TaskFileDetails.TaskName, this.FileName);
                    BatchClient batchClient = BatchClient.Open(this.TaskFileDetails.Credentials);
                    var job = batchClient.JobOperations.GetJob(this.TaskFileDetails.JobName);
                    var task = job.GetTask(this.TaskFileDetails.TaskName);                    
                    var file = task.GetNodeFile("wd\\" + this.FileName);
                    
                    file.CopyToStream(this.memoryStream);
                    this.memoryStream.Seek(0, SeekOrigin.Begin);                    
                }
                catch (Exception e)
                {
                    Console.WriteLine("Initialize reader failed");
                    Console.WriteLine(e.ToString());
                }
            }
            
        }

        internal void InitializeReader()
        {
            if (this.Reader != null) return;

            lock (this)
            {
                if (this.Reader != null) return;
                this.InitializeStream();
                this.Reader = new StreamReader(this.memoryStream);
            }
        }
        
        /// <summary>
        /// Helper method to copy the remote file data to a fileStream.
        /// </summary>
        /// <param name="fs">The FileStream to copy to</param>
        public void CopyToStream(FileStream fs)
        {
            this.InitializeStream();
            this.memoryStream.CopyTo(fs);            
        }

        /// <summary>
        /// Helper method to copy the remote file data to a local file
        /// </summary>
        /// <param name="filePath">File to cpy to</param>
        /// <param name="fileMode">File Mode to use</param>
        public void CopyDataToLocalFile(string filePath, FileMode fileMode)
        {
            FileStream destStream = new FileStream(filePath, fileMode);
            using (destStream)
            {
                CopyToStream(destStream);
                destStream.Flush();
                destStream.Close();
            }
        }        

        /// <summary>
        /// Read the contents of the remote file to a byte buffer
        /// </summary>
        /// <param name="buffer">the byte buffer to read the file contents to</param>
        /// <param name="index">the start index in the buffer to put data into</param>
        /// <param name="count">the count of bytes to read</param>
        /// <returns>The number of bytes read into the buffer.</returns>
        public int Read(byte[] buffer, int index, int count)
        {            
            InitializeStream();
            return this.memoryStream.Read(buffer, index, count);            
        }

        /// <summary>
        /// Async Read the contents of the remote file to a byte buffer
        /// </summary>
        /// <param name="buffer">the byte buffer to read the file contents to</param>
        /// <param name="index">the start index in the buffer to put data into</param>
        /// <param name="count">the count of bytes to read</param>
        /// <returns>The number of bytes read into the buffer.</returns>
        public Task<int> ReadAsync(byte[] buffer, int index, int count)
        {            
            InitializeStream();
            return this.memoryStream.ReadAsync(buffer, index, count);
        }


        /// <summary>
        /// Async Reads a line of the remote file as a string and advances the read pointer to next line
        /// </summary>
        /// <returns>The async task</returns>
        public Task<string> ReadLineAsync()
        {
            if (Reader == null)
            {
                InitializeReader();
            }
            return this.Reader.ReadLineAsync();
        }

        /// <summary>
        /// Reads a line from the remote file as a string and advances the read pointer to next line
        /// </summary>
        /// <returns></returns>
        public string ReadLine()
        {            
            if (Reader == null)
            {
                InitializeReader();
            }
            return this.Reader.ReadLine();
        }

        #region private
        private AzureBatchTaskDetails TaskFileDetails {get;set;}        
        private Uri ClusterUri{get;set;}
        private string FileName{get;set;}

        [NonSerialized]
        StreamReader reader;        
        StreamReader Reader
        {
            get { return reader; }
            set { this.reader = value; }
        }
        [NonSerialized]
        MemoryStream memoryStream;
        #endregion private

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            this.reader.Close();
        }
    }

    /// <summary>
    /// Helper class to write a Azure Batch TaskFile
    /// </summary>
    [Serializable]
    public class BatchTaskFileWriter : IDisposable
    {
        /// <summary>
        /// BatchTaskFileWriter constructor
        /// </summary>
        /// <param name="fileName">The name of the file to write to</param>
        public BatchTaskFileWriter(string fileName)
        {   this.FileName = fileName;
        }

        internal void InitializeFileStream()
        {
            if (this.InternalFileStream != null) return;

            lock (this)
            {
                if (this.InternalFileStream != null) return;
                this.InternalFileStream = File.OpenWrite(this.FileName);
            }
        }
        
        /// <summary>
        /// Writes a string as a line into the file
        /// </summary>
        /// <param name="line"></param>
        public void WriteLine(string line)
        {
            this.InitializeFileStream();
            StreamWriter sw = new StreamWriter(this.InternalFileStream);
            sw.WriteLine(line);
            sw.Flush();            
        }

        /// <summary>
        /// Writes an array of bytes to the file
        /// </summary>
        /// <param name="data">Byte data to write</param>
        /// <param name="offset">index offset into the byte buffer to start from </param>
        /// <param name="count">Count of bytes to write</param>
        public void Write(byte[] data, int offset, int count)
        {
            this.InitializeFileStream();
            this.InternalFileStream.Write(data, offset, count);
        }

        /// <summary>
        /// Async Writes an array of bytes to the file
        /// </summary>
        /// <param name="data">Byte data to write</param>
        /// <param name="offset">index offset into the byte buffer to start from </param>
        /// <param name="count">Count of bytes to write</param>
        public Task WriteAsync(byte[] data, int offset, int count)
        {
            this.InitializeFileStream();
            return this.InternalFileStream.WriteAsync(data, offset, count);
        }

        #region private

        private FileStream InternalFileStream
        {
            get { return this.filestream; }
            set { this.filestream = value; }
        }
        [NonSerialized]
        private FileStream filestream;


        private string FileName { get; set; }

        [NonSerialized]
        StreamWriter writer;
        StreamWriter Writer
        {
            get { return writer; }
            set { this.writer = value; }
        }
        #endregion private

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            this.Writer.Close();
        }
    }    


    /// <summary>
    /// Data provider for Azure Batch Task
    /// </summary>
    [Serializable]
    public class AzureBatchTaskDataProvider : IXCDataPartition
    {
        #region Member Variables        
        private UInt32 NumPartitions { get; set; }
        private List<AzureBatchTaskFilePartition> FilePartitions { get; set; }        

        #endregion

        #region Constructor
        /// <summary>
        /// AzureBatchTaskDataProvider constructor
        /// </summary>
        /// <param name="taskDetails">Various details realted to the Azure Batch Task</param>
        /// <param name="numPartitions">The number of partitions to create</param>
        /// <param name="filePrefix">The prefix of files to use in the Azure Batch task's working directory</param>
        public AzureBatchTaskDataProvider(AzureBatchTaskDetails taskDetails , uint numPartitions, string filePrefix)
        {
            List<AzureBatchTaskFilePartition> partitions = new List<AzureBatchTaskFilePartition>();            

            for(int i=0;i<numPartitions;i++)
            {
                string fileName = filePrefix + i.ToString();
                AzureBatchTaskFilePartition par = new AzureBatchTaskFilePartition(taskDetails, fileName);                
                partitions.Add(par);
            }

            this.Initialize(partitions);
            
        }

        /// <summary>
        /// AzureBatchTaskDataProvider
        /// </summary>
        /// <param name="batchPartitions">List of partitions to use</param>
        public AzureBatchTaskDataProvider(List<AzureBatchTaskFilePartition> batchPartitions)
        {
            this.Initialize(batchPartitions);
        }

        private void Initialize(List<AzureBatchTaskFilePartition> batchPartitions)
        {
            this.FilePartitions = batchPartitions;
            this.NumPartitions = (uint)batchPartitions.Count();            
        }

        #endregion Constructor

        #region Helper Types

        [Serializable]
        enum AzureBatchFileProviderConstructorType
        {
            ReadFileList,
            ReadFilePartitions,
            Write
        }

        #endregion


        #region IXCDataPartition
        /// <summary>
        /// List of affinity strings
        /// </summary>
        public string[] Affinities
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// THe size of the partition
        /// </summary>
        public long Weight
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Enumeration of the paritions
        /// </summary>
        public IEnumerable<IXCDataPartition> Partitions
        {
            get { return this.FilePartitions; }
        }

        #endregion IXCDataPartition

        #region DataProviderMethods
        //TODO: IEnumerable<byte[]>ReadRecords(){};

        /// <summary>
        /// Provides an enumeration of the data partition as lines of strings
        /// </summary>
        /// <returns></returns>
        IEnumerable<string> ReadLines()
        {
            foreach (var par in this.FilePartitions)
            {
                string line = par.ReadLine();
                if (line != null)
                    yield return line;
            }
            yield return null;
            //return this.TaskFileReader.ReadLine();
        }
        #endregion DataProviderMethods

        
    }

    
    //TODO: The AzureBatchTaskFilePartition, can be a collection of partitions. So there needs to be
    //enhancements around how data is read. At present it assumes a partition == a file on the TVM.
    //But it could be pointing to a directory, in which case the CopyToStream method, may be does not
    //make sense.
    //This class needs more work.

    /// <summary>
    /// AzureBatchTaskFilePartition class provides a AzureBatchTask File Partition. It represents a collection
    /// of files that are produced in the AzureBatchTask working directory that belong to a single partition. 
    /// This is generally useful in the distribution phase, whre the task distributes data into various files 
    /// that are organized by partitions, so that the next set of tasks can consume the files, for the partition
    /// that they are responsible for
    /// </summary>
    [Serializable]
    public class AzureBatchTaskFilePartition : IXCDataPartition
    {
        #region Constants

        internal const int MaxBlockSize = 4 * 1024 * 1024;    //4MB        

        #endregion

        #region Member Variables
        /// <summary>
        /// Azure Batch Task related details
        /// </summary>
        public AzureBatchTaskDetails BatchTaskDetails { get; private set; }
        /// <summary>
        /// Name of the file
        /// </summary>
        public string FileName { get; private set; }
        /// <summary>
        /// The associated reader
        /// </summary>
        private BatchTaskFileReader TaskFileReader { get; set; }
        /// <summary>
        /// The associated writer
        /// </summary>
        private BatchTaskFileWriter TaskFileWriter { get; set; }
        #endregion Member Variables


        #region Constructor
        /// <summary>
        /// AzureBatchTaskFilePartition constructor
        /// </summary>
        /// <param name="batchTaskDetails">Azure Batch Task related details</param>
        /// <param name="fileName">The name ofthe file</param>
        public AzureBatchTaskFilePartition(AzureBatchTaskDetails batchTaskDetails, string fileName)
        {
            this.BatchTaskDetails = batchTaskDetails;
            this.FileName = fileName;
            this.TaskFileReader = new BatchTaskFileReader(this.BatchTaskDetails, this.FileName);
            this.TaskFileWriter = new BatchTaskFileWriter(this.FileName);
        }
        #endregion Constructor
        #region IXCDataPartition
        /// <summary>
        /// Affinities (Not Implemented)
        /// </summary>
        public string[] Affinities
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Weight  (Not Implemented)
        /// </summary>
        public long Weight
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Partitions (Not Implemented)
        /// </summary>
        public IEnumerable<IXCDataPartition> Partitions
        {
            get { throw new NotImplementedException(); }
        }

        #endregion IXCDataPartition

        #region PartitionMethods

        /// <summary>
        /// Async Read Line of string from the partition 
        /// </summary>
        /// <returns>Async Task to read the line from</returns>
        public Task<string> AsyncReadLine()
        {
            return this.TaskFileReader.ReadLineAsync();
        }

        /// <summary>
        /// Read Line of string from the partition 
        /// </summary>
        /// <returns>The line of string</returns>
        public string ReadLine()
        {
            return this.TaskFileReader.ReadLine();
        }

        /// <summary>
        /// Copy remote file to local FileStream
        /// </summary>
        /// <param name="fs">The FileStream to copy to</param>
        public void CopyToStream(FileStream fs)
        {
            this.TaskFileReader.CopyToStream(fs);
        }

        /// <summary>
        /// Read data as an array of bytes from the remote file
        /// </summary>
        /// <param name="buffer">The Byte buffer to read the data into</param>
        /// <param name="index">The array index of the Byte buffer to start copying data to</param>
        /// <param name="count">The count of bytes to read</param>
        /// <returns>The number of bytes read</returns>
        public int Read(byte[] buffer, int index, int count)
        {
            return this.TaskFileReader.Read(buffer, index, count);
        }

        /// <summary>
        /// Async Read data as an array of bytes from the remote file
        /// </summary>
        /// <param name="buffer">The Byte buffer to read the data into</param>
        /// <param name="index">The array index of the Byte buffer to start copying data to</param>
        /// <param name="count">The count of bytes to read</param>
        /// <returns>The number of bytes read</returns>
        public Task<int> ReadAsync(byte[] buffer, int index, int count)
        {
            return this.TaskFileReader.ReadAsync(buffer, index, count);
        }

        /// <summary>
        /// Write a line of string to the partition
        /// </summary>
        /// <param name="line"></param>
        public void WriteLine(string line)
        {
            this.TaskFileWriter.WriteLine(line);
        }



        #endregion PartitionMethods
        
    }

    /// <summary>
    /// Represents a directory Partition for an Azure Batch Task
    /// An AzureBatchTaskDirectoryPartition consists of a collection of AzureBatchTaskFilePartition 
    /// </summary>
    [Serializable]
    public class AzureBatchTaskDirectoryPartition : IXCDataPartition
    {
        #region Constants
        internal const int MaxBlockSize = 4 * 1024 * 1024;    //4MB        
        #endregion

        #region Member Variables
        private static int filePartitionFileNameGenerator = 0;
        /// <summary>
        /// Determines whether this partition will be used for read or write
        /// </summary>
        public enum DirectoryInitializationType {read, write};
        /// <summary>
        /// Sets the DirectoryInitializationType  type
        /// </summary>
        private DirectoryInitializationType InitializationType { set; get; }

        /// <summary>
        /// Azure Batch Task details
        /// </summary>
        public AzureBatchTaskDetails BatchTaskDetails { get; private set; }
        /// <summary>
        /// Name of the directory
        /// </summary>
        public string DirectoryName { get; private set; }
        

        [NonSerialized]
        List<AzureBatchTaskFilePartition> filePartitions = new List<AzureBatchTaskFilePartition>();
        [NonSerialized]
        bool havePartitionsBeenListed = false;
        #endregion Member Variables

        /// <summary>
        /// AzureBatchTaskDirectoryPartition constructor
        /// </summary>
        /// <param name="directoryName">The name of the dihrectory</param>
        public AzureBatchTaskDirectoryPartition(string directoryName)
        {
            this.DirectoryName = directoryName;
        }

        /// <summary>
        /// Initializes the Partition 
        /// </summary>
        /// <param name="initType"></param>
        public void Initialize(DirectoryInitializationType initType)
        {
            this.InitializationType = initType;
            if (this.InitializationType == DirectoryInitializationType.write)
            {
                if (!Directory.Exists(this.DirectoryName))
                {
                    Directory.CreateDirectory(this.DirectoryName);
                }
            }
        }

        /// <summary>
        /// Adds to the collection of File partitions. It also autogenerates a unique file name
        /// A directory partition consists of many AzureBatchTaskFilePartition 
        /// </summary>
        /// <returns>The newly created AzureBatchTaskFilePartition </returns>
        public AzureBatchTaskFilePartition AddFilePartition()
        {
            int previous = Interlocked.Increment(ref AzureBatchTaskDirectoryPartition.filePartitionFileNameGenerator);
            AzureBatchTaskFilePartition filePartition = new AzureBatchTaskFilePartition(this.BatchTaskDetails, this.DirectoryName+"\\"+previous.ToString());
            this.filePartitions.Add(filePartition);
            return filePartition;
        }

        /// <summary>
        /// Adds to the collection of File partitions. 
        /// A directory partition consists of many AzureBatchTaskFilePartition 
        /// </summary>
        /// <param name="fileName">The name of the file to use</param>
        /// <returns>The newly created AzureBatchTaskFilePartition </returns>
        public AzureBatchTaskFilePartition AddFilePartition(string fileName)
        {
            AzureBatchTaskFilePartition filePartition = new AzureBatchTaskFilePartition(this.BatchTaskDetails, this.DirectoryName + "\\" + fileName);
            this.filePartitions.Add(filePartition);
            return filePartition;
        }

        /// <summary>
        /// Returns the list of AzureBatchTaskFilePartition associated with this AzureBatchDirectoryPartition
        /// </summary>
        /// <returns></returns>
        public List<AzureBatchTaskFilePartition> ListPartitions()
        {
            if (this.havePartitionsBeenListed) return this.filePartitions;
            lock (this)
            {
                if (this.havePartitionsBeenListed) return this.filePartitions;

                try
                {
                    Console.WriteLine("Enumerating Directory Partition for JobName - {0}, TaskName - {1}, Directory - {2}", this.BatchTaskDetails.JobName, this.BatchTaskDetails.TaskName, this.DirectoryName);
                    BatchClient batchClient = BatchClient.Open(this.BatchTaskDetails.Credentials);
                    var job = batchClient.JobOperations.GetJob(this.BatchTaskDetails.JobName);
                    var task = job.GetTask(this.BatchTaskDetails.TaskName);
                    var files = task.ListNodeFiles(true, new ODATADetailLevel("wd/" + this.DirectoryName));
                    
                    foreach(var file in files)
                    {                        
                        if (file.IsDirectory.HasValue && file.IsDirectory.Value)
                        {
                            Console.WriteLine("Skipping directory - {0}", file.Name);
                        }

                        AzureBatchTaskFilePartition filePartition = new AzureBatchTaskFilePartition(this.BatchTaskDetails, file.Name);
                        this.filePartitions.Add(filePartition);
                    }                    
                }
                catch (Exception e)
                {
                    Console.WriteLine("Faile in the ListTaskFilesRequest call");
                    Console.WriteLine(e.ToString());
                }
                
            }

            return this.filePartitions;
        }

        #region IXcDataPartition implementation
        /// <summary>
        /// Affinities (Not Implemented)
        /// </summary>
        public string[] Affinities
        {
            get { throw new NotImplementedException(); }
        }

        /// <summary>
        /// Weight (Not Implemented)
        /// </summary>
        public long Weight
        {
            get
            {
                throw new NotImplementedException();
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Enumerable list of FileParititons (Not Implemented)
        /// </summary>

        public IEnumerable<IXCDataPartition> Partitions
        {
            get { throw new NotImplementedException(); }
        }
        #endregion IXcDataPartition implementation
    }
}
