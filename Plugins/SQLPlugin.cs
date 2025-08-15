using ClientProfileAgentV2.Controllers;
using Microsoft.Data.SqlClient;
using Microsoft.Identity.Client;
using Microsoft.SemanticKernel;
using System.ComponentModel;
using System.Data;
using System.Text;

namespace ClientProfileAgentV2.Plugins
{
    public class SQLPlugin
    {
        private readonly IConfiguration _configuration;
        private readonly RuntimeDB _runtimeDB;
        private const int DefaultCommandTimeout = 500;
        private const int MaxQueryLogLength = 2000;

        public SQLPlugin(IConfiguration configuration, RuntimeDB runtimeDB)
        {
            _configuration = configuration;
            _runtimeDB = runtimeDB;
        }

        #region Plugins


        [KernelFunction("GetDatabaseStructure")]
        [Description("Returns schema.table column types as a single string with literal \\r\\n separators.")]
        public async Task<string> GetDatabaseStructureAsync()
        {
            return await _runtimeDB.GetSchemaDescriptionAsync();
        }

        [KernelFunction("DatabaseConnection")]
        [Description("Executes a SQL query against Fabric. Call GetDatabaseStructure first if schema info is needed.")]

        public async Task<string> ExecuteSQL(string sqlQuery, string ChannelID)
        {
            try
            {
                using var connection = await GetLakehouseConnectionAsync();
                if (connection == null)
                    return "\"Error\"\n\"Failed to establish database connection\"";

                await connection.OpenAsync();
                if (connection.State != ConnectionState.Open)
                    return "\"Error\"\n\"Connection failed to open\"";

                var result = await ExecuteQueryAsync(connection, sqlQuery);
                var csvData = QueryToCSV(result);

                await LogQueryAsync(ChannelID, sqlQuery);
                return csvData;
            }
            catch (SqlException sqlEx)
            {
                var errorMessage = $"SQL Error: {sqlEx.Message.Replace("\"", "\"\"")}";
                Console.WriteLine($"SQL Exception: {sqlEx.Message}");
                return $"\"Error\"\n\"{errorMessage}\"";
            }
            catch (Exception ex)
            {
                var errorMessage = $"Unexpected Error: {ex.Message.Replace("\"", "\"\"")}";
                Console.WriteLine($"General Exception: {ex.Message}");
                return $"\"Error\"\n\"{errorMessage}\"";
            }
        }
        #endregion

        #region Connection Management
        private async Task<SqlConnection> GetLakehouseConnectionAsync()
        {
            try
            {
                var config = GetFabricConfiguration();
                if (string.IsNullOrEmpty(config.TenantId) || string.IsNullOrEmpty(config.ClientId) ||
                    string.IsNullOrEmpty(config.ClientSecret) || string.IsNullOrEmpty(config.SqlLakehouse) ||
                    string.IsNullOrEmpty(config.LhName))
                {
                    Console.WriteLine("One or more configuration values are missing");
                    return null;
                }

                var accessToken = await AcquireAccessTokenAsync(config);
                if (string.IsNullOrEmpty(accessToken))
                {
                    Console.WriteLine("Failed to acquire access token");
                    return null;
                }

                return CreateSqlConnection(config, accessToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating connection: {ex.Message}");
                return null;
            }
        }

        private (string TenantId, string ClientId, string ClientSecret, string SqlLakehouse, string LhName) GetFabricConfiguration()
        {
            return (
                _configuration["FabricSettings:TenantId"],
                _configuration["FabricSettings:ClientId"],
                _configuration["FabricSettings:ClientSecret"],
                _configuration["FabricSettings:SqlLakehouse"],
                _configuration["FabricSettings:LhName"]
            );
        }

        private static async Task<string> AcquireAccessTokenAsync((string TenantId, string ClientId, string ClientSecret, string SqlLakehouse, string LhName) config)
        {
            var authority = $"https://login.microsoftonline.com/{config.TenantId}";
            var scopes = new[] { "https://database.windows.net//.default" };

            var app = ConfidentialClientApplicationBuilder.Create(config.ClientId)
                .WithClientSecret(config.ClientSecret)
                .WithAuthority(authority)
                .Build();

            var authResult = await app.AcquireTokenForClient(scopes).ExecuteAsync();
            return authResult.AccessToken;
        }

        private static SqlConnection CreateSqlConnection((string TenantId, string ClientId, string ClientSecret, string SqlLakehouse, string LhName) config, string accessToken)
        {
            var connectionString = $"Server=tcp:{config.SqlLakehouse},1433;Database={config.LhName};Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
            return new SqlConnection(connectionString)
            {
                AccessToken = accessToken
            };
        }
        #endregion

        #region Query Execution
        private static async Task<SqlDataReader> ExecuteQueryAsync(SqlConnection connection, string sqlQuery)
        {
            var command = new SqlCommand(sqlQuery, connection)
            {
                CommandTimeout = DefaultCommandTimeout
            };

            return await command.ExecuteReaderAsync();
        }

        private static string QueryToCSV(SqlDataReader reader)
        {
            var csvBuilder = new StringBuilder();

            AppendHeaders(reader, csvBuilder);
            var rowCount = AppendDataRows(reader, csvBuilder);

            if (!reader.HasRows)
                Console.WriteLine($"Query executed successfully but returned no rows");

            Console.WriteLine($"Query returned {rowCount} rows");
            return csvBuilder.ToString();
        }

        private static void AppendHeaders(SqlDataReader reader, StringBuilder csvBuilder)
        {
            for (int i = 0; i < reader.FieldCount; i++)
            {
                csvBuilder.Append(reader.GetName(i));
                if (i < reader.FieldCount - 1)
                    csvBuilder.Append(",");
            }
            csvBuilder.AppendLine();
        }

        private static int AppendDataRows(SqlDataReader reader, StringBuilder csvBuilder)
        {
            int rowCount = 0;
            while (reader.Read())
            {
                rowCount++;
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.IsDBNull(i) ? "" : reader.GetValue(i)?.ToString() ?? "";

                    value = value.Replace("\"", "\"\"");
                    if (value.Contains(",") || value.Contains("\"") || value.Contains("\n") || value.Contains("\r"))
                        value = $"\"{value}\"";

                    csvBuilder.Append(value);

                    if (i < reader.FieldCount - 1)
                        csvBuilder.Append(",");
                }
                csvBuilder.AppendLine();
            }
            return rowCount;
        }
        #endregion

        #region Logging
        private async Task LogQueryAsync(string channelId, string sqlQuery)
        {
            var trimmedQuery = sqlQuery.Length > MaxQueryLogLength
                ? sqlQuery.Substring(0, MaxQueryLogLength)
                : sqlQuery;

            await _runtimeDB.SaveMessageAsync(channelId, string.Empty, true, "Assistant", trimmedQuery);
        }
        #endregion
    }
}