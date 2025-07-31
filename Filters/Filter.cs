using ClientProfileAgentV2.Controllers;
using Microsoft.Agents.Builder;
using Microsoft.Agents.Core.Models;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.ChatCompletion;
using Microsoft.SemanticKernel.Connectors.OpenAI;
using System.Text.Json;

namespace ClientProfileAgentV2.Filters
{
    public class Filter : IFunctionInvocationFilter
    {
        private ITurnContext _turnContext;
        private CancellationToken _cancellationToken;
        private Kernel _kernel;
        private const string DatabaseConnectionFunction = "DatabaseConnection";
        private RuntimeDB _runtime;
        private IConfiguration _configuration;

        #region Context Management
        public void SetContext(ITurnContext turnContext, CancellationToken cancellationToken, Kernel kernel, RuntimeDB runtime, IConfiguration Config)
        {
            _turnContext = turnContext;
            _cancellationToken = cancellationToken;
            _kernel = kernel;
            _runtime = runtime;
            _configuration = Config;
        }
        #endregion

        #region Function Invocation
        public async Task OnFunctionInvocationAsync(FunctionInvocationContext context, Func<FunctionInvocationContext, Task> next)
        {
            try
            {
                if (context.Function.Name == DatabaseConnectionFunction)
                {
                    if (_turnContext != null)
                    {
                        var response = await GenerateDatabaseAssistantResponseAsync();
                        await SendResponseWithMetadataAsync(response);
                    }
                }
            }
            catch {}
            finally
            {
                await next(context);
            }
        }
        #endregion

        #region Response Generation
        private async Task<string> GenerateDatabaseAssistantResponseAsync()
        {
            try
            {
                var chatHistory = new ChatHistory();
                chatHistory.AddUserMessage($"The user asked:{_turnContext.Activity.Text}");
                var chatCompletion = _kernel.GetRequiredService<IChatCompletionService>();
                string systemPrompt = _configuration["WaitingDBPrompt"];

                var promptSettings = new OpenAIPromptExecutionSettings
                {
                    ChatSystemPrompt = systemPrompt
                };

                var response = await chatCompletion.GetChatMessageContentAsync(chatHistory, executionSettings: promptSettings, kernel: _kernel);
                return response.ToString();
            }
            catch
            {
                return "Database Initiated, Please wait...";
            }
        }

        private async Task SendResponseWithMetadataAsync(string responseText)
        {
            var reply = MessageFactory.Text(responseText, responseText);
            reply.Entities = CreateMessageEntities();

            await _turnContext.SendActivityAsync(reply, _cancellationToken);
        }

        private static List<Entity> CreateMessageEntities()
        {
            const string entityJson = """
        {
            "type": "https://schema.org/Message",
            "@type": "Message",
            "@context": "https://schema.org",
            "additionalType": [ "AIGeneratedContent" ]
        }
        """;

            var entityDict = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(entityJson);

            return new List<Entity>
        {
            new Entity
            {
                Type = "https://schema.org/Message",
                Properties = entityDict
            }
        };
        }
        #endregion
    }
}