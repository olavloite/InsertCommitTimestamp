using DotNet.Testcontainers.Builders;
using Google.Api.Gax;
using Google.Api.Gax.ResourceNames;
using Google.Cloud.Spanner.Admin.Database.V1;
using Google.Cloud.Spanner.Admin.Instance.V1;
using Google.Cloud.Spanner.Common.V1;
using Google.Cloud.Spanner.Data;
using Grpc.Core;

// Start the Spanner emulator in a Docker container.
var container = new ContainerBuilder()
    .WithImage("gcr.io/cloud-spanner-emulator/emulator")
    // Bind port 9010 of the container to a random port on the host.
    .WithPortBinding(9010, true)
    .WithEnvironment(new Dictionary<string, string>{{"TZ", "America/Chicago"}})
    .Build();

// Start the container.
await container.StartAsync().ConfigureAwait(false);

// Set the environment variable to point to the randomly assigned port number of the emulator.
Environment.SetEnvironmentVariable("SPANNER_EMULATOR_HOST", $"localhost:{container.GetMappedPublicPort(9010)}");

var projectId = "sample-project";
var instanceId = "sample-instance";
var databaseId = "sample-database";
DatabaseName databaseName = DatabaseName.FromProjectInstanceDatabase(projectId, instanceId, databaseId);
var dataSource = $"Data Source={databaseName}";
var connectionStringBuilder = new SpannerConnectionStringBuilder(dataSource)
{
    EmulatorDetection = EmulatorDetection.EmulatorOnly,
};
await MaybeCreateInstanceOnEmulatorAsync(databaseName.ProjectId, databaseName.InstanceId);
await MaybeCreateDatabaseOnEmulatorAsync(databaseName);

using (var connection = new SpannerConnection(connectionStringBuilder.ConnectionString))
{
    await connection.CreateDdlCommand(
        "create table if not exists test " +
        "(id int64, " +
        "value string(max), " +
        "ts timestamp options (allow_commit_timestamp=true)) " +
        "primary key (id)").ExecuteNonQueryAsync();

    var id = Guid.NewGuid();
    var cmd = connection.CreateInsertCommand("test", new SpannerParameterCollection
    {
        { "id", SpannerDbType.Int64, id.GetHashCode() },
        { "value", SpannerDbType.String, id.ToString() },
        { "ts", SpannerDbType.Timestamp, SpannerParameter.CommitTimestamp },
    });
    await connection.RunWithRetriableTransactionAsync(async transaction =>
    {
        cmd.Transaction = transaction;
        await cmd.ExecuteNonQueryAsync();
    });

    var reader = await connection.CreateSelectCommand("select value, ts from test order by ts desc").ExecuteReaderAsync();
    while (await reader.ReadAsync())
    {
        Console.WriteLine($"Row {reader.GetFieldValue<string>(0)} inserted at {reader.GetFieldValue<DateTime>(1)}");
    }
}

await container.StopAsync();

static async Task MaybeCreateInstanceOnEmulatorAsync(string projectId, string instanceId)
{
    // Try to create an instance on the emulator and ignore any AlreadyExists error.
    var adminClientBuilder = new InstanceAdminClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOnly
    };
    var instanceAdminClient = await adminClientBuilder.BuildAsync();

    var instanceName = InstanceName.FromProjectInstance(projectId, instanceId);
    try
    {
        await instanceAdminClient.CreateInstance(new CreateInstanceRequest
        {
            InstanceId = instanceName.InstanceId,
            ParentAsProjectName = ProjectName.FromProject(projectId),
            Instance = new Instance
            {
                InstanceName = instanceName,
                ConfigAsInstanceConfigName = new InstanceConfigName(projectId, "emulator-config"),
                DisplayName = "Sample Instance",
                NodeCount = 1,
            },
        }).PollUntilCompletedAsync();
    }
    catch (RpcException e) when (e.StatusCode == StatusCode.AlreadyExists)
    {
        // Ignore
    }
}

static async Task MaybeCreateDatabaseOnEmulatorAsync(DatabaseName databaseName)
{
    // Try to create a database on the emulator and ignore any AlreadyExists error.
    var adminClientBuilder = new DatabaseAdminClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOnly
    };
    var databaseAdminClient = await adminClientBuilder.BuildAsync();

    var instanceName = InstanceName.FromProjectInstance(databaseName.ProjectId, databaseName.InstanceId);
    try
    {
        await databaseAdminClient.CreateDatabase(new CreateDatabaseRequest
        {
            ParentAsInstanceName = instanceName,
            CreateStatement = $"CREATE DATABASE `{databaseName.DatabaseId}`",
        }).PollUntilCompletedAsync();
    }
    catch (RpcException e) when (e.StatusCode == StatusCode.AlreadyExists)
    {
        // Ignore
    }
}
