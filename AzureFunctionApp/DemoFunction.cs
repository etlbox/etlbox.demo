using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Net.Http;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow;
using System.Net;

namespace AzureFunctionApp
{
    public static class DemoFunction
    {
        [FunctionName("DemoFunction")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("Received a json file to transform.");

            string filename = Guid.NewGuid().ToString() + ".csv";

            log.LogInformation("Temp file used to hold data: " + filename);

            var jsonSource = new JsonSource() {
                CreateStreamReader = _ => new StreamReader(req.Body)
            };
            var csvFileDest = new CsvDestination(filename);

            jsonSource.LinkTo(csvFileDest);
            await Network.ExecuteAsync(jsonSource);

            log.LogInformation("Successfully stored data in file - now returning the content as response.");

            return new HttpResponseMessage() {
                StatusCode = HttpStatusCode.OK,
                Content = new StreamContent(new FileStream(filename, FileMode.Open))
            };
        }
    }
}
