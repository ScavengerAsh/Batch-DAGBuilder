using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure;
using System.Xml.Serialization;
using DAGBuilderCommunication;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.XCDataProviders;

namespace DocParser
{

    [Serializable()]
    [XmlInclude(typeof(BlobProviderChannel))]
    public class BlobProviderChannel: Channel
    {
        public List<XCBlobPartition> partitions { get; set; }
    }


    [Serializable()]
    [XmlInclude(typeof(BatchFileProviderChannel))]
    public class BatchFileProviderChannel : Channel
    {
        public AzureBatchTaskFilePartition partition { get; set; }
    }


    [Serializable()]
    [XmlInclude(typeof(BlockBlobStorageChannel))]
    public class ContainerBlobList
    {
        public ContainerBlobList(string containerName, string[] blobList)
        {
            this.ContainerName = containerName;
            this.BlobList = blobList;
        }

        public string ContainerName { get; private set; }
        public string[] BlobList{ get; private set; }
    }

    [Serializable()]
    [XmlInclude(typeof(BlockBlobStorageChannel))]
    public class BlockBlobStorageChannel : Channel
    {
        public string SAS { get; private set; }        
        public string StorageAccount { get; private set; }
        public string StorageKey { get; private set; }

        public BlockBlobStorageChannel(string channelFileId, int channelIdx, string storageAcc, string storageKey):
            base(ChannelType.Custom)
        {            
            this.StorageAccount = storageAcc;
            this.StorageKey = storageKey;
        }

        public BlobStorageBlockReader GetChannelBlockReader(List<ContainerBlobList> blobList)
        {
            BlobList = blobList;
            return new BlobStorageBlockReader(StorageAccount, StorageKey, BlobList);
        }

        public BlobStorageLineRecordReader GetChannelLineRecordReader()
        {
            throw new NotImplementedException();
        }

        public List<ContainerBlobList> BlobList { get; set; }

    }



    public class BlobStorageBlockReader
    {
        public BlobStorageBlockReader(string accName, string accKey, List<ContainerBlobList> blobList)
        {
            StorageCredentials stgAccount =
                    new StorageCredentials(accName, accKey);
            CloudStorageAccount acc = new CloudStorageAccount(stgAccount, true);

            this.Client = acc.CreateCloudBlobClient(); //new CloudBlobClient(endPoint, stgAccount);
            this.BlobList = blobList;
            
        }

        List<ContainerBlobList> BlobList { get; set; }
        CloudBlobClient Client { get; set; }

        public Stream GetBlobStream()
        {
            ContainerBlobList container = this.BlobList[0];            
            CloudBlobContainer cloudContainer = this.Client.GetContainerReference(container.ContainerName);            
            CloudBlockBlob blob = cloudContainer.GetBlockBlobReference(container.BlobList[0]);
            return blob.OpenRead();            
        }    
    }

    public class BlobStorageLineRecordReader 
    {
        public BlobStorageLineRecordReader (){}
    }
}
