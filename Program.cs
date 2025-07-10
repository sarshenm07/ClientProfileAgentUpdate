using ClientProfileAgentV2;
using ClientProfileAgentV2.Bot;
using ClientProfileAgentV2.Controllers;
using ClientProfileAgentV2.Filters;
using ClientProfileAgentV2.Plugins;
using Microsoft.Agents.Builder;
using Microsoft.Agents.Hosting.AspNetCore;
using Microsoft.Agents.Storage;
using Microsoft.SemanticKernel;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddHttpClient("WebClient", client => client.Timeout = TimeSpan.FromSeconds(600));
builder.Services.AddHttpContextAccessor();
builder.Services.AddCloudAdapter();
builder.Logging.AddConsole();

var sqlFilter = new Filter();
var runtimeDB = new RuntimeDB(builder.Configuration);
var sqlPlugin = new SQLPlugin(builder.Configuration, runtimeDB);
var runtimeDBPlugin = new RuntimeDBPlugin(builder.Configuration);
var longTimeoutClient = new HttpClient { Timeout = TimeSpan.FromMinutes(5) };

IKernelBuilder kernelBuilder = Kernel.CreateBuilder()
    .AddAzureOpenAIChatCompletion(
        deploymentName: builder.Configuration["Foundry:DeploymentName"],
        apiKey: builder.Configuration["Foundry:Key"],
        endpoint: builder.Configuration["Foundry:EndPoint"],
        httpClient: longTimeoutClient
    );

kernelBuilder.Services.AddSingleton<IFunctionInvocationFilter>(sqlFilter);
kernelBuilder.Plugins.AddFromObject(sqlPlugin);
kernelBuilder.Plugins.AddFromObject(runtimeDBPlugin);

Kernel kernel = kernelBuilder.Build();

builder.Services.AddBotAspNetAuthentication(builder.Configuration);
builder.Services.AddSingleton<Kernel>(kernel);
builder.Services.AddSingleton<RuntimeDB>(runtimeDB);
builder.Services.AddSingleton(sqlFilter);
builder.Services.AddSingleton<IStorage, MemoryStorage>();

builder.AddAgentApplicationOptions();
builder.AddAgent<ClientProfile>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
}

app.UseRouting();
app.UseAuthentication();
app.UseAuthorization();

app.MapPost("/api/messages", async (HttpRequest request, HttpResponse response, IAgentHttpAdapter adapter, IAgent agent, CancellationToken cancellationToken) =>
{
    await adapter.ProcessAsync(request, response, agent, cancellationToken);
});

if (app.Environment.IsDevelopment() || app.Environment.EnvironmentName == "Playground")
{
    app.MapGet("/", () => "Echo Agent");
    app.UseDeveloperExceptionPage();
    app.MapControllers().AllowAnonymous();
}
else
{
    app.MapControllers();
}

app.Run();