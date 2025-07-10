using Microsoft.Data.SqlClient;
using Microsoft.Identity.Client;
using Microsoft.SemanticKernel;
using System.ComponentModel;

namespace ClientProfileAgentV2.Plugins
{
    public class RuntimeDBPlugin
    {
        private readonly IConfiguration _configuration;
        private const int DefaultConnectionTimeout = 30;

        public RuntimeDBPlugin(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        #region Plugins
        [KernelFunction("clearcontext")]
        [Description("Forget/ clear the chat history")]
        public async Task<bool> ClearContextAsync(string channelId)
        {
            const string query = @"
                UPDATE dbo.History
                   SET IsActive = 0
                 WHERE ChannelID = @ChannelID;";
            try
            {
                using var conn = await GetOpenConnectionAsync();
                using var command = new SqlCommand(query, conn);
                command.Parameters.AddWithValue("@ChannelID", channelId);
                var rowsAffected = await command.ExecuteNonQueryAsync();
                return rowsAffected > 0;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[ClearContextAsync] Error: {ex}");
                return false;
            }
        }
        #endregion

        #region Connection Management
        private async Task<string> AcquireAccessTokenAsync()
        {
            var config = GetFabricConfiguration();
            var authority = $"https://login.microsoftonline.com/{config.TenantId}";
            var scopes = new[] { "https://database.windows.net//.default" };

            var app = ConfidentialClientApplicationBuilder
                .Create(config.ClientId)
                .WithClientSecret(config.ClientSecret)
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
                ConnectTimeout = DefaultConnectionTimeout,
                MultipleActiveResultSets = false
            };

            var connection = new SqlConnection(builder.ConnectionString)
            {
                AccessToken = accessToken
            };

            await connection.OpenAsync();
            return connection;
        }

        private (string TenantId, string ClientId, string ClientSecret) GetFabricConfiguration()
        {
            return (
                _configuration["FabricSettings:TenantId"],
                _configuration["FabricSettings:ClientId"],
                _configuration["FabricSettings:ClientSecret"]
            );
        }
        #endregion
    }
}