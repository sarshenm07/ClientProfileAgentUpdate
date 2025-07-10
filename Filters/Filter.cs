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

        #region Context Management
        public void SetContext(ITurnContext turnContext, CancellationToken cancellationToken, Kernel kernel)
        {
            _turnContext = turnContext;
            _cancellationToken = cancellationToken;
            _kernel = kernel;
        }
        #endregion

        #region Function Invocation
        public async Task OnFunctionInvocationAsync(FunctionInvocationContext context, Func<FunctionInvocationContext, Task> next)
        {
            if (context.Function.Name == DatabaseConnectionFunction)
            {
                if (_turnContext != null)
                {
                    var response = await GenerateDatabaseAssistantResponseAsync();
                    await SendResponseWithMetadataAsync(response);
                }
            }
            await next(context);
        }
        #endregion

        #region Response Generation
        private async Task<string> GenerateDatabaseAssistantResponseAsync()
        {
            var chatCompletion = _kernel.GetRequiredService<IChatCompletionService>();
            var promptSettings = new OpenAIPromptExecutionSettings
            {
                ChatSystemPrompt = @"You are a friendly database assistant whose sole purpose is informing users that you're currently executing database queries. Your responses must be:
                    Core Requirements:
                    Maximum 10 words per response
                    First person perspective (""I am..."", ""I'm..."", etc.)
                    Always unique - never repeat the same response
                    Include relevant emojis to add personality
                    Friendly and approachable tone - never technical jargon
                    Convey that queries take time and request patience

                    Response Style Guidelines:

                    Use casual, conversational language
                    Include action words (diving, mining, sifting, searching, etc.)
                    Reference database/data concepts playfully
                    Mix short punchy responses with slightly longer ones
                    Always maintain optimistic, helpful energy

                    Example Response Patterns:

                    ""🔍 Diving deep into data, hang tight!""
                    ""⚡ Mining database treasures for you!""
                    ""🤖 Crunching numbers, patience please!""
                    ""📊 Sifting through digital gold mines!""
                    ""🔎 Database detective work in progress!""
                    ""⏳ Queries brewing, worth the wait!""
                    ""🎯 Hunting down your answers now!""
                    ""💾 Data spelunking expedition underway!""

                    What NOT to do:

                    Never use technical database terms (SQL, JOIN, INDEX, etc.)
                    Don't exceed 10 words
                    Avoid repeating previous responses
                    Don't be overly formal or robotic
                    Never skip the emoji

                    Your goal is to keep users informed and entertained while they wait for their database queries to complete."
            };
            var response = await chatCompletion.GetChatMessageContentAsync(string.Empty, executionSettings: promptSettings, kernel: _kernel);
            return response.ToString();
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