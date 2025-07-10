using Microsoft.Data.SqlClient;
using Microsoft.Identity.Client;
using Microsoft.SemanticKernel.ChatCompletion;

namespace ClientProfileAgentV2.Controllers
{
    public class RuntimeDB
    {
        private readonly IConfiguration _configuration;

        public RuntimeDB(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        #region SQL Connection

        private async Task<string> AcquireAccessTokenAsync()
        {
            var tenantId = _configuration["FabricSettings:TenantId"];
            var clientId = _configuration["FabricSettings:ClientId"];
            var clientSecret = _configuration["FabricSettings:ClientSecret"];
            var authority = $"https://login.microsoftonline.com/{tenantId}";
            var scopes = new[] { "https://database.windows.net//.default" };

            var app = ConfidentialClientApplicationBuilder
                .Create(clientId)
                .WithClientSecret(clientSecret)
                .WithAuthority(authority)
                .Build();

            var authResult = await app.AcquireTokenForClient(scopes).ExecuteAsync();
            return authResult.AccessToken;
        }

        private async Task<SqlConnection> GetOpenConnectionAsync()
        {
            var accessToken = await AcquireAccessTokenAsync();

            var builder = new SqlConnectionStringBuilder
            {
                DataSource = _configuration["FabricSettings:SqlServer"],
                InitialCatalog = _configuration["FabricSettings:Db"],
                Encrypt = true,
                TrustServerCertificate = false,
                ConnectTimeout = 30,
                MultipleActiveResultSets = true
            };

            var connection = new SqlConnection(builder.ConnectionString)
            {
                AccessToken = accessToken
            };

            await connection.OpenAsync();
            return connection;
        }

        #endregion

        #region Database Operations

        public async Task<string> GetLatestSystemPromptAsync()
        {
            const string query = "SELECT TOP 1 [System prompt] FROM dbo.Configuration ORDER BY Id DESC";

            try
            {
                using var conn = await GetOpenConnectionAsync();
                using var cmd = new SqlCommand(query, conn);

                var result = await cmd.ExecuteScalarAsync();
                return result?.ToString() ?? string.Empty;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[GetLatestSystemPromptAsync] Error: {ex}");
                return "Please inform user that the system prompt cannot be accessed.";
            }
        }

        public async Task<bool> SaveMessageAsync(string channelId, string message, bool isActive, string role, string sql)
        {
            const string query = @"
                INSERT INTO dbo.History (ChannelID, Message, IsActive, Role, [SQL])
                VALUES (@ChannelID, @Message, @IsActive, @Role, @SQL);";

            try
            {
                using var conn = await GetOpenConnectionAsync();
                using var cmd = new SqlCommand(query, conn);

                cmd.Parameters.AddWithValue("@ChannelID", channelId);
                cmd.Parameters.AddWithValue("@Message", message);
                cmd.Parameters.AddWithValue("@IsActive", isActive);
                cmd.Parameters.AddWithValue("@Role", role);
                cmd.Parameters.AddWithValue("@SQL", sql);

                var rows = await cmd.ExecuteNonQueryAsync();
                return rows > 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[SaveMessageAsync] Error: {ex}");
                return false;
            }
        }

        public async Task<ChatHistory> GetRecentUserChatHistoryAsync(string channelId, int limit, TimeSpan maxAge)
        {
            const string query = @"
               SELECT TOP (@Limit)
               ChannelID,
               Message,
               IsActive,
               Role,
               SQL
               FROM dbo.History
               WHERE ChannelID = @ChannelID
               AND Timestamp >= @Threshold
               AND IsActive = 1
               ORDER BY Timestamp DESC;";

            try
            {
                using var conn = await GetOpenConnectionAsync();
                using var cmd = new SqlCommand(query, conn);

                var threshold = DateTime.UtcNow - maxAge;
                cmd.Parameters.AddWithValue("@ChannelID", channelId);
                cmd.Parameters.AddWithValue("@Threshold", threshold);
                cmd.Parameters.AddWithValue("@Limit", limit);

                var history = new ChatHistory();
                var entries = new List<(string Message, string Role)>();

                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var message = reader.GetString(1);
                    string role = !reader.IsDBNull(3) ? reader.GetString(3) : null;
                    entries.Add((message, role));
                }

                entries.Reverse();
                foreach (var (message, role) in entries)
                {
                    if (string.Equals(role, "Assistant", StringComparison.OrdinalIgnoreCase))
                    {
                        history.AddAssistantMessage(message);
                    }
                    else
                    {
                        history.AddUserMessage(message);
                    }
                }

                return history;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[GetRecentUserChatHistoryAsync] Error: {ex}");

                var errorHistory = new ChatHistory();
                errorHistory.AddDeveloperMessage("Error retrieving chat history: " + ex.Message);
                errorHistory.AddAssistantMessage("An error occurred while retrieving chat history. Please inform the user.");
                return errorHistory;
            }
        }

        #endregion
    }
}
