using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices(services =>
    {
        services.AddApplicationInsightsTelemetryWorkerService();
        services.ConfigureFunctionsApplicationInsights();
        services.AddSingleton(x =>
        {
            var configuration = x.GetRequiredService<IConfiguration>();
            string connectionString = configuration["AZURE_STORAGE_CONNECTION_STRING"];
            return new BlobServiceClient(connectionString);
        });
    })
    .Build();

host.Run();
