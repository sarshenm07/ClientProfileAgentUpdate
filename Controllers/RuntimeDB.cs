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

        /// <summary>
        /// Returns a single string describing schema.table, column and type for the whole DB,
        /// using literal "\r\n" separators to match your Semantic Kernel description format.
        /// </summary>
        public async Task<string> GetSchemaDescriptionAsync()
        {
            const string sql = @"
                   DECLARE @SchemaName sysname = NULL

                    ;WITH cols AS (
                                    SELECT
                                        sch.name AS SchemaName,
                                        t.name   AS TableName,
                                        c.name   AS ColumnName,
                                        typ.name AS TypeName,
                                        c.max_length,
                                        c.precision,
                                        c.scale,
                                        c.column_id
                                    FROM sys.schemas sch
                                    JOIN sys.tables  t   ON t.schema_id  = sch.schema_id
                                    JOIN sys.columns c   ON c.object_id  = t.object_id
                                    JOIN sys.types   typ ON typ.user_type_id = c.user_type_id
                                    WHERE (@SchemaName IS NULL OR sch.name = @SchemaName)
                                  ),
                         schemas AS (
                             SELECT DISTINCT SchemaName FROM cols
                                    )
                         SELECT
                               N'Database Name: ' + DB_NAME() + N'\r\n'
                             + N'Database Schema: '
                             + COALESCE(
                                 @SchemaName,
                                 (SELECT STRING_AGG(SchemaName, N', ') FROM schemas)
                               ) + N'\r\n'
                             + N'Database Structure: Name of table Name Of column Type\r\n'
                             + COALESCE((
                                 SELECT STRING_AGG(
                                     cols.SchemaName + N'.' + cols.TableName + N' ' + cols.ColumnName + N' ' +
                                     cols.TypeName +
                                     CASE
                                         WHEN cols.TypeName IN (N'varchar',N'char',N'varbinary',N'binary') AND cols.max_length <> -1
                                             THEN N'(' + CAST(cols.max_length AS NVARCHAR(10)) + N')'
                                         WHEN cols.TypeName IN (N'nvarchar',N'nchar') AND cols.max_length <> -1
                                             THEN N'(' + CAST(cols.max_length/2 AS NVARCHAR(10)) + N')' 
                                         WHEN cols.max_length = -1 AND cols.TypeName IN (N'varchar',N'nvarchar',N'varbinary')
                                             THEN N'(MAX)'
                                         WHEN cols.TypeName IN (N'decimal',N'numeric')
                                             THEN N'(' + CAST(cols.precision AS NVARCHAR(10)) + N',' + CAST(cols.scale AS NVARCHAR(10)) + N')'
                                         ELSE N''
                                     END,
                                     N'\r\n'
                                 )
                                 FROM cols
                               ), N'');";

            try
            {
                using var conn = await GetOpenConnectionAsync();
                using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
                var result = await cmd.ExecuteScalarAsync();
                return result?.ToString() ?? string.Empty;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[GetSchemaDescriptionAsync] Error: {ex}");
                return string.Empty;
            }
        }

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
