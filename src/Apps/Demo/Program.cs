using System;
using System.Net;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

using DAGBuilderInterface;
using DAGBuilder;
using AzureBatchCluster;
using System.Diagnostics;
using System.IO;
using DAGBuilderCommunication;
using FakeCluster;


namespace DocParser
{
    class Program
    {

        static void DoRatioJoin_FakeCluster()
        {
            int S0Count = 10000;
            int S1Count = 2;

            MyFakeComputeCluster fakeCluster = new MyFakeComputeCluster();            
            JobGraph graph = new JobGraph("Demo", fakeCluster);
            var S0 = graph.AddStage("Stage0", (uint)S0Count);

            int counter = 0;
            foreach (var Vertex in S0.StageVertexes.Values)
            {
                string cmdLine = "Cmd /C ";
                if (counter < S0Count / S1Count)
                    cmdLine += "ping -n 30 127.0.0.100";
                else
                    cmdLine += "ping -n 1 127.0.0.100";
                Vertex.SetCommandLine(cmdLine);
                Vertex.AddOutputs(typeof(Channel), true, 1);
                counter++;
            }

            var S1 = graph.AddStage("Stage1", (uint)S1Count);
            foreach (var Vertex in S1.StageVertexes.Values)
            {
                Vertex.SetCommandLine("Cmd /C dir");
            }

            //Ratio  join the vertexes            
            graph.RatioJoinStages(S0, S1, S0Count / S1Count, ChannelType.NULL);




            graph.Execute();

        }

        static void DoCrossJoin(AzureBatchCluster.AzureBatchCluster cluster)
        {
            try
            {
                int S0Count = 4;
                int S1Count = 2;
                cluster.ClusterInitialize("Demo", "MyApp.config");
                
                JobGraph graph = new JobGraph("Demo", cluster);
                var S0 = graph.AddStage("Stage0", (uint)S0Count);

                int counter = 0;
                foreach (var Vertex in S0.StageVertexes.Values)
                {
                    string cmdLine = "Cmd /C ";
                    if (counter < S0Count / S1Count)
                        cmdLine += "ping -n 30 127.0.0.100";
                    else
                        cmdLine += "ping -n 1 127.0.0.100";
                    Vertex.SetCommandLine(cmdLine);
                    Vertex.AddOutputs(typeof(Channel), true, 2);
                    counter++;
                }

                var S1 = graph.AddStage("Stage1", (uint)S1Count);
                foreach (var Vertex in S1.StageVertexes.Values)
                {
                    Vertex.SetCommandLine("Cmd /C dir");
                }

                //Ratio  join the vertexes
                graph.JoinStages(S0, S1, StageVertexJoin.Cross, ChannelType.NULL);


                graph.Execute();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            cluster.ClusterDeInitialize("Demo work done");

        }       

        static void DoRatioJoin(AzureBatchCluster.AzureBatchCluster cluster)
        {
            try
            {
                int numVertexesStg0 = 4;
                int numVertexesStg1 = 2;

                //Create Cluster
                cluster.ClusterInitialize("Demo", "MyApp.config");                
                JobGraph graph = new JobGraph("Demo", cluster);

                //Add Stage Zero
                var Stage0 = graph.AddStage("Stage0", (uint)numVertexesStg0);
                int counter = 0;
                foreach (var Vertex in Stage0.StageVertexes.Values)
                {
                    //Alternate vertexes in this stage sleep for 30sec and 1sec respectively
                    string cmdLine = "Cmd /C ";
                    if (counter < numVertexesStg0 / numVertexesStg1)
                        cmdLine += "ping -n 30 127.0.0.100";
                    else
                        cmdLine += "ping -n 1 127.0.0.100";
                    Vertex.SetCommandLine(cmdLine);
                    Vertex.AddOutputs(typeof(Channel), true, 1); //Each vertex gets 1 output (null channel)
                    counter++;
                }
                //Adding the next stage
                var Stage1 = graph.AddStage("Stage1", (uint)numVertexesStg1);
                foreach (var Vertex in Stage1.StageVertexes.Values)
                {
                    Vertex.SetCommandLine("Cmd /C dir");
                }

                //Ratio  join the vertexes from Stage 0 to Stage 1. 
                //In this case 2 Stage0 vertexes join to 1 Stage1                 
                graph.RatioJoinStages(Stage0, Stage1, numVertexesStg0/numVertexesStg1, ChannelType.NULL);
                
                //Execute the graph
                graph.Execute();
                cluster.ClusterDeInitialize("Demo work done");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }           

        }      


        static void DoCustomJoin(AzureBatchCluster.AzureBatchCluster cluster)
        {
            try
            {
                int S0Count = 4;
                int S1Count = 2;
                cluster.ClusterInitialize("Demo", "MyApp.config");
                //DAGBuilder.DAGBuilder.Initialize(cluster);
                JobGraph graph = new JobGraph("Demo", cluster);
                var S0 = graph.AddStage("Stage0", (uint)S0Count);

                int counter = 0;
                List<Channel> outputChannels = new List<Channel>();
                foreach (var Vertex in S0.StageVertexes.Values)
                {
                    string cmdLine = "Cmd /C ";
                    if (0== (counter %2))
                        cmdLine += "ping -n 30 127.0.0.100";
                    else
                        cmdLine += "ping -n 1 127.0.0.100";
                    Vertex.SetCommandLine(cmdLine);
                    outputChannels.AddRange(Vertex.AddOutputs(typeof(Channel), true, 1));
                    counter++;
                }

                var S1 = graph.AddStage("Stage1", (uint)S1Count);
                foreach (var Vertex in S1.StageVertexes.Values)
                {
                    Vertex.SetCommandLine("Cmd /C dir");
                }
                
                foreach (var vertex in S0.StageVertexes.Values)
                {
                    int vertexNumber = counter % S1Count;
                    vertex.JoinOutputChannelToVertex(S1.StageVertexes[vertexNumber], vertex.Outputs[0]);
                }


                graph.Execute();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            cluster.ClusterDeInitialize("Demo work done");

        }

        static void DoMulticlusterRun()
        {
            try
            {
                int S0Count = 4;
                int S1Count = 2;
                AzureBatchCluster.AzureBatchCluster cluster1 = new AzureBatchCluster.AzureBatchCluster();
                cluster1.ClusterInitialize("Demo", "MyApp.config");

                AzureBatchCluster.AzureBatchCluster cluster2 = new AzureBatchCluster.AzureBatchCluster();
                cluster2.ClusterInitialize("Demo", "MyApp1.config");
                
                JobGraph graph = new JobGraph("Demo", null);
                var S0 = graph.AddStage("Stage0", (uint)S0Count, null, cluster1);

                int counter = 0;
                List<Channel> outputChannels = new List<Channel>();
                foreach (var Vertex in S0.StageVertexes.Values)
                {
                    string cmdLine = "Cmd /C ";
                    if (0 == (counter % 2))
                        cmdLine += "ping -n 30 127.0.0.100";
                    else
                        cmdLine += "ping -n 1 127.0.0.100";
                    Vertex.SetCommandLine(cmdLine);
                    outputChannels.AddRange(Vertex.AddOutputs(typeof(Channel), true, 1));
                    counter++;
                }

                var S1 = graph.AddStage("Stage1", (uint)S1Count, null, cluster2);
                foreach (var Vertex in S1.StageVertexes.Values)
                {
                    Vertex.SetCommandLine("Cmd /C dir");
                }

                foreach (var vertex in S0.StageVertexes.Values)
                {
                    int vertexNumber = counter % S1Count;
                    vertex.JoinOutputChannelToVertex(S1.StageVertexes[vertexNumber], vertex.Outputs.ElementAt(0).Value);
                }

                graph.Execute();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

            //cluster1.ClusterDeInitialize("Demo work done");

        }

    /// <summary>
    /// This provides sample code for working/using DAGBuilder. It provides sample on how to use the FakeCluster, a BatchCluster
    /// </summary>
    /// <param name="args"></param>
    

        static void Main(string[] args)
        {
            //DoRatioJoin_FakeCluster();
            DoMulticlusterRun();
            
            //AzureBatchCluster.AzureBatchCluster cluster = new AzureBatchCluster.AzureBatchCluster();            
            
            //Console.WriteLine("============ Cross Join ================");
            //DoCrossJoin(cluster);
            //Console.WriteLine("Done. Press Enter to continue");
            //Console.ReadLine();

            //Console.WriteLine("============ Ratio Join ================");
            //DoRatioJoin(cluster);
            //Console.WriteLine("Done. Press Enter to continue");
            //Console.ReadLine();
            
            //Console.WriteLine("============ Custom Join ================");
            //DoCustomJoin(cluster);
            //Console.WriteLine("Done. Press Enter to continue");
            //Console.ReadLine();
             
        }
    }
}
