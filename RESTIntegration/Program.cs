using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Server;

namespace RESTIntegration
{
    public class Order
    {
        [JsonProperty("OrderNumber")]
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public string Description { get; set; }
    }


    internal class Program
    {
        static WireMockServer Server { get; set; }

        static void Main(string[] args)
        {
            StartWebServer();
            JsonSource<Order> source = new JsonSource<Order>("https://www.etlbox.net/demo/api/orders", ResourceType.Http);
            source.HttpClient = CreateDefaultHttpClient();

            ColumnRename<Order> rename = new ColumnRename<Order>();
            rename.RenameColumns = new[]
            {
                new RenameColumn() { CurrentName = "Id", NewName ="OrderId" },
                new RenameColumn() { CurrentName = "CustomerId", NewName ="CId" },
                new RenameColumn() { CurrentName = "Description", RemoveColumn = true }
            };
            JsonDestination destination = new JsonDestination();
            destination.ResourceType = ResourceType.Http;
            destination.HttpClient = CreateDefaultHttpClient();
            destination.HttpRequestMessage.Method = HttpMethod.Post;
            destination.HasNextUri = (streamMetaData, row) => true;
            destination.GetNextUri = (streamMetaData, row) =>
            {
                streamMetaData.HttpRequestMessage.Headers.Authorization = new AuthenticationHeaderValue("Bearer", "Some token");
                return $"http://localhost:61456/post/{streamMetaData.ProgressCount}";
            };

            source.LinkTo(rename);
            rename.LinkTo(destination);
            Network.Execute(source);

            WriteServerLog();
        }

        static void StartWebServer()
        {
            Server = WireMockServer.Start(61456);
            Server
                .Given(Request.Create().WithPath("/post/*").UsingPost())
                .RespondWith(
                    Response.Create()
                    .WithStatusCode(200)
                );
        }        

        private static HttpClient CreateDefaultHttpClient()
        {
            var httpClient = new HttpClient(new HttpClientHandler()
            {
                AutomaticDecompression = DecompressionMethods.All
            });
            httpClient.Timeout = TimeSpan.FromSeconds(1000);
            httpClient.DefaultRequestHeaders.Connection.Add("keep-alive");
            httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("*/*"));
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("gzip"));
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("deflate"));
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("br"));
            httpClient.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("MyImporter", "1.1"));
            httpClient.DefaultRequestHeaders.CacheControl = new CacheControlHeaderValue() { NoCache = true };
            return httpClient;
        }

        private static void WriteServerLog()
        {
            var requests = Server.FindLogEntries(
                Request.Create().WithPath("/post/*").UsingAnyMethod()
            );
            foreach (var req in requests)
            {
                Console.WriteLine("Url: " + req.RequestMessage.Path);
                foreach (var header in req.RequestMessage.Headers)
                    Console.WriteLine("Key:" + header.Key + ", Value:" + header.Value);
                Console.WriteLine(req.RequestMessage.Body);
                Console.WriteLine("------------------------------");
            }
        }
    }
}
