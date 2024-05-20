using System.Formats.Asn1;
using System.Globalization;
using System.Net;
using System.Text;
using Azure.Storage.Blobs;
using CsvHelper;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace IngestionServices
{
    public class CsvToJsonConverterActivity
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger _logger;
        private readonly BlobServiceClient _blobServiceClient;

        public CsvToJsonConverterActivity(ILoggerFactory loggerFactory, BlobServiceClient blobServiceClient, IConfiguration configuration)
        {
            _logger = loggerFactory.CreateLogger<CsvToJsonConverterActivity>();
            _blobServiceClient = blobServiceClient;
            _configuration = configuration;
        }

        [Function("CsvToJsonConverterActivity")]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            var outputStreamContainer = _blobServiceClient.GetBlobContainerClient(_configuration["AZURE_STORAGE_TARGET_CONTAINER_NAME"]);
            await outputStreamContainer.CreateIfNotExistsAsync();

            var inputContainer = _blobServiceClient.GetBlobContainerClient(_configuration["AZURE_STORAGE_CSV_CONTAINER_NAME"]);
            var response = await inputContainer.GetBlobClient(_configuration["AZURE_STORAGE_CSV_FILE_NAME"]).DownloadAsync();

            using (var reader = new StreamReader(response.Value.Content))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                var records = csv.GetRecords<dynamic>().ToList();

                Parallel.ForEach(records, async (record) =>
                {
                    string fileName = $"{Guid.NewGuid()}.json";
                    var sb = new StringBuilder();
                    sb.AppendLine($"AssetName: {record.AssetName}");
                    sb.AppendLine($"AssetTypeName: {record.AssetTypeName}");
                    sb.AppendLine($"ActionImpact: {record.ActionImpact}");
                    sb.AppendLine($"Problem: {record.Problem}");
                    sb.AppendLine($"Recommendation: {record.Recommendation}");
                    var targetSchema = new
                    {
                        content = sb.ToString(),
                    };
                    string json = JsonConvert.SerializeObject(targetSchema);

                    BlobClient outputBlobClient = outputStreamContainer.GetBlobClient(fileName);
                    using (var ms = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)))
                    {
                        await outputBlobClient.UploadAsync(ms, true);
                    }
                });
            }

            var httpResponse = req.CreateResponse(HttpStatusCode.OK);
            return httpResponse;
        }
    }
}
