using DistributedWorker.Cluster;
using DistributedWorker.Worker;
using dotnet_etcd.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

var etcdConnectionString = builder.Configuration.GetConnectionString("Etcd");

builder.Services.AddEtcdClient(options => {
    options.ConnectionString = etcdConnectionString;
    options.UseInsecureChannel = true;
});

builder.Services
    .AddOptions<ClusterOptions>()
    .Bind(builder.Configuration.GetSection("Cluster"))
    .ValidateDataAnnotations()
    .ValidateOnStart();

builder.Services.AddSingleton<ClusterState>();

builder.Services.AddHostedService<LeaseManagerService>();
builder.Services.AddHostedService<LeaderElectionService>();
builder.Services.AddHostedService<WatchService>();
builder.Services.AddHostedService<AssignmentManagerService>();
builder.Services.AddHostedService<WorkerService>();

var app = builder.Build();


app.UseHttpsRedirection();
app.Run();