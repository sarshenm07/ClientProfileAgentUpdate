using ClientProfileAgentV2.Controllers;
using ClientProfileAgentV2.Filters;
using Microsoft.Agents.Builder;
using Microsoft.Agents.Builder.App;
using Microsoft.Agents.Builder.State;
using Microsoft.Agents.Core.Models;
using Microsoft.SemanticKernel;
using Microsoft.SemanticKernel.ChatCompletion;
using Microsoft.SemanticKernel.Connectors.OpenAI;
using System.Text.Json;

namespace ClientProfileAgentV2.Bot;

public class ClientProfile : AgentApplication
{
    private const int TYPING_DELAY_SECONDS = 1;
    private const int TIME_LIMIT_MINUTES = 20;
    private const int CHAT_HISTORY_LIMIT = 10;

    private readonly Kernel _kernel;
    private readonly RuntimeDB _runtimeDB;
    private readonly Filter _sqlFilter;

    public ClientProfile(AgentApplicationOptions options, Kernel kernel, RuntimeDB runtimeDB, Filter filter) : base(options)
    {
        _kernel = kernel;
        _runtimeDB = runtimeDB;
        _sqlFilter = filter;
        OnConversationUpdate(ConversationUpdateEvents.MembersAdded, WelcomeMessageAsync);
        OnActivity(ActivityTypes.Message, OnMessageAsync);
    }

    protected async Task WelcomeMessageAsync(ITurnContext turnContext, ITurnState turnState, CancellationToken cancellationToken)
    {
        foreach (ChannelAccount member in turnContext.Activity.MembersAdded)
        {
            if (member.Id != turnContext.Activity.Recipient.Id)
            {
                await turnContext.SendActivityAsync(MessageFactory.Text("Hi, I'm the Agent Client Profile—I'm here to help bridge the gap between you and your client data by referencing databases to deliver the information you need."), cancellationToken);
            }
        }
    }

    protected async Task OnMessageAsync(ITurnContext turnContext, ITurnState turnState, CancellationToken cancellationToken)
    {
        var channelId = turnContext.Activity.Conversation.Id;
        var userText = turnContext.Activity.Text;

        // Start typing indicator
        using var typingCts = new CancellationTokenSource();
        var typingTask = SendTypingIndicatorAsync(turnContext, typingCts.Token);

        //Get recent chat history
        var chatHistory = await _runtimeDB.GetRecentUserChatHistoryAsync(channelId, CHAT_HISTORY_LIMIT, TimeSpan.FromMinutes(TIME_LIMIT_MINUTES));
        chatHistory.AddAssistantMessage($"ChannelID: {channelId}");
        chatHistory.AddAssistantMessage($"Current Date: {DateTime.Now}");
        chatHistory.AddUserMessage(userText);

        //Set Filter
        _sqlFilter.SetContext(turnContext, cancellationToken, _kernel);


        if (!await _runtimeDB.SaveMessageAsync(channelId, $"{userText}", true, "User", string.Empty))
        {
            await turnContext.SendActivityAsync(MessageFactory.Text("Failed to record transaction"), cancellationToken);
        }

        var response = await GenerateAIResponseAsync(chatHistory, cancellationToken);
        await SendResponseWithMetadataAsync(turnContext, response.ToString(), cancellationToken);

        typingCts.Cancel();
        await typingTask;

        if (!await _runtimeDB.SaveMessageAsync(channelId, $"{response}", true, "Assistant", string.Empty))
        {
            await turnContext.SendActivityAsync(MessageFactory.Text("Failed to record transaction"), cancellationToken);
        }
    }

    private async Task<ChatMessageContent> GenerateAIResponseAsync(ChatHistory chatHistory, CancellationToken cancellationToken)
    {
        var chatCompletion = _kernel.GetRequiredService<IChatCompletionService>();
        var executionSettings = new OpenAIPromptExecutionSettings
        {
            ToolCallBehavior = ToolCallBehavior.AutoInvokeKernelFunctions,
            ChatSystemPrompt = await _runtimeDB.GetLatestSystemPromptAsync()
        };

        return await chatCompletion.GetChatMessageContentAsync(
            chatHistory,
            executionSettings: executionSettings,
            kernel: _kernel,
            cancellationToken: cancellationToken);
    }

    private static async Task SendResponseWithMetadataAsync(ITurnContext turnContext, string responseText, CancellationToken cancellationToken)
    {
        var reply = MessageFactory.Text(responseText, responseText);
        reply.Entities = CreateMessageEntities();

        await turnContext.SendActivityAsync(reply, cancellationToken);
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

    private static async Task SendTypingIndicatorAsync(ITurnContext context, CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var typingActivity = new Activity { Type = ActivityTypes.Typing };
                await context.SendActivityAsync(typingActivity, cancellationToken);
                await Task.Delay(TimeSpan.FromSeconds(TYPING_DELAY_SECONDS), cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested — safe to ignore
        }
    }
}
