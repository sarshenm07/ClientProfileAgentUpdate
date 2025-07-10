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

        [KernelFunction("DatabaseConnection")]
        [Description("Database Name: lh_enterprise_analytics_data_monthlyclientprofile" +
            "Database Schema: dbo" +
            "Database Structure: Name of table   Name Of column  Type\r\ndate    monthdatekey    int\r\ndate    monthenddate    date\r\ndate    calendarmonthname       varchar\r\ndate    monthofyearnumber       int\r\ndate    calendaryearnumber      int\r\ndate    fiscalmonthname varchar\r\ndate    fiscalmonthofyearnumber int\r\ndate    fiscalyearnumber        int\r\ndim_app_outflows        appoutflowskey  bigint\r\ndim_app_outflows        appoutflowsamountband   varchar\r\ndim_app_outflows        averagemonthlyappoutflowsamountband     varchar\r\ndim_app_outflows        appoutflowscountband    varchar\r\ndim_app_outflows        averagemonthlyappoutflowscountband      varchar\r\ndim_atm_withdrawals     atmwithdrawalskey       bigint\r\ndim_atm_withdrawals     atmwithdrawalamountband varchar\r\ndim_atm_withdrawals     averagemonthlyatmwithdrawalamountband   varchar\r\ndim_atm_withdrawals     atmwithdrawalcountband  varchar\r\ndim_atm_withdrawals     averagemonthlyatmwithdrawalcountband    varchar\r\ndim_banking_definiton   clientbankingdefinitionkey      bigint\r\ndim_banking_definiton   bankingdetaildefinition varchar\r\ndim_banking_definiton   bankingdefinition       varchar\r\ndim_cashback    cashbackkey     bigint\r\ndim_cashback    cashbackamountband      varchar\r\ndim_cashback    averagemonthlycashbackamountband        varchar\r\ndim_cashback    cashbackcountband       varchar\r\ndim_cashback    averagemonthlycashbackcountband varchar\r\ndim_classification      classificationkey       bigint\r\ndim_classification      isfinancialactiveclient varchar\r\ndim_classification      isfuneralcoverclient    varchar\r\ndim_classification      istermloanclient        varchar\r\ndim_classification      istermloanclientuptodate        varchar\r\ndim_classification      istermloanclientlessthan90daysinarrears varchar\r\ndim_classification      isaccessfacilityclient  varchar\r\ndim_classification      isaccessfacilityclientuptodate  varchar\r\ndim_classification      isaccessfacilityclientlessthan90daysinarrears   varchar\r\ndim_classification      iscreditcardclient      varchar\r\ndim_classification      iscreditcardclientuptodate      varchar\r\ndim_classification      iscreditcardclientlessthan90daysinarrears       varchar\r\ndim_classification      isappuser       varchar\r\ndim_classification      isussduser      varchar\r\ndim_classification      isdigitaluser   varchar\r\ndim_classification      isdebitorderuser        varchar\r\ndim_classification      isdebitorderdisputeuser varchar\r\ndim_classification      iscardsubscriptionuser  varchar\r\ndim_classification      isscheduledpaymentuser  varchar\r\ndim_classification      isrecurringpaymentuser  varchar\r\ndim_classification      issavingsplanclient     varchar\r\ndim_classification      ishomeloanclient        varchar\r\ndim_classification      islifecoverclient       varchar\r\ndim_classification      isflexiblesavingsplanclient     varchar\r\ndim_classification      isfixedtermsavingsplanclient    varchar\r\ndim_classification      isfixeddepositsavingsplanclient varchar\r\ndim_classification      islivebettersavingsplanclient   varchar\r\ndim_classification      isstableinflows varchar\r\ndim_classification      isproductscore  varchar\r\ndim_classification      isfullybanked   varchar\r\ndim_classification_star starclassificationkey   bigint\r\ndim_classification_star clientstarstatuskey     bigint\r\ndim_classification_star clientstarstatus        varchar\r\ndim_classification_star isstartermloanclient    varchar\r\ndim_classification_star isstarcreditcardclient  varchar\r\ndim_classification_star isstaraccessfacilityclient      varchar\r\ndim_classification_star isstarfuneralcoverclient        varchar\r\ndim_classification_star isstarsavingsplanclient varchar\r\ndim_classification_star isstardigitaluser       varchar\r\ndim_classification_star isstarstableinflows     varchar\r\ndim_classification_star isstarrecurringpaymentuser      varchar\r\ndim_classification_star isstarlowcashuser       varchar\r\ndim_classification_star isstarhomeloanclient    varchar\r\ndim_classification_star isstarlifecoverclient   varchar\r\ndim_cnp cnpkey  bigint\r\ndim_cnp cnpamountband   varchar\r\ndim_cnp averagemonthlycnpamountband     varchar\r\ndim_cnp cnpcountband    varchar\r\ndim_cnp averagemonthlycnpcountband      varchar\r\ndim_demographics        demographicskey bigint\r\ndim_demographics        languagename    varchar\r\ndim_demographics        nationalitycountryname  varchar\r\ndim_demographics        clienttypename  varchar\r\ndim_demographics        gendername      varchar\r\ndim_demographics        productcount    int\r\ndim_demographics        agefirstbandname        varchar\r\ndim_demographics        agesecondbandname       varchar\r\ndim_demographics        agethirdbandname        varchar\r\ndim_demographics        agefirstbandnamesort    bigint\r\ndim_demographics        agesecondbandnamesort   bigint\r\ndim_demographics        agethirdbandnamesort    bigint\r\ndim_do_dispute  debitorderdisputekey    bigint\r\ndim_do_dispute  debitorderdisputeamountband     varchar\r\ndim_do_dispute  debitorderdisputecountband      varchar\r\ndim_do_outflows debitorderoutflowskey   bigint\r\ndim_do_outflows debitorderamountband    varchar\r\ndim_do_outflows averagemonthlydebitorderamountband      varchar\r\ndim_do_outflows debitordernetamountband varchar\r\ndim_do_outflows debitordercountband     varchar\r\ndim_do_outflows averagemonthlydebitordercountband       varchar\r\ndim_ib_outflows iboutflowskey   bigint\r\ndim_ib_outflows internetbankingoutflowsamountband       varchar\r\ndim_ib_outflows averagemonthlyinternetbankingoutflowsamountband varchar\r\ndim_ib_outflows internetbankingoutflowscountband        varchar\r\ndim_ib_outflows averagemonthlyinternetbankingoutflowcountband   varchar\r\ndim_inflows     inflowskey      bigint\r\ndim_inflows     inflowsamountband       varchar\r\ndim_inflows     averagemonthlyinflowsamountband varchar\r\ndim_other_outflows      otheroutflowskey        bigint\r\ndim_other_outflows      totalcashoutflowsamountband     varchar\r\ndim_other_outflows      totaldigitalchanneloutflowamountband    varchar\r\ndim_other_outflows      totalcardchanneloutflowsamountband      varchar\r\ndim_other_outflows      totalelectronicoutflowsamountband       varchar\r\ndim_other_outflows      cashtoelectronicratioband       varchar\r\ndim_pos_trns    poskey  bigint\r\ndim_pos_trns    posamountband   varchar\r\ndim_pos_trns    averagemonthlyposamountband     varchar\r\ndim_pos_trns    poscountband    varchar\r\ndim_pos_trns    averagemonthlyposcountband      varchar\r\ndim_regional    regionalkey     bigint\r\ndim_regional    branchname      varchar\r\ndim_regional    regionalmanagername     varchar\r\ndim_regional    businessmanagername     varchar\r\ndim_regional    operationsmanagername   varchar\r\ndim_regional    branchmanagername       varchar\r\ndim_regional    provincecode    varchar\r\ndim_regional    provincename    varchar\r\ndim_regional    countrycode     varchar\r\ndim_sst_outflows        sstoutflowskey  bigint\r\ndim_sst_outflows        sstoutflowsamountband   varchar\r\ndim_sst_outflows        averagemonthlysstoutflowsamountband     varchar\r\ndim_sst_outflows        sstoutflowscountband    varchar\r\ndim_sst_outflows        averagemonthlysstoutflowcountband       varchar\r\ndim_tenure      tenurekey       bigint\r\ndim_tenure      tenure  bigint\r\ndim_tenure      tenureband      varchar\r\ndim_ussd_outflows       ussdoutflowskey bigint\r\ndim_ussd_outflows       ussdoutflowsamountband  varchar\r\ndim_ussd_outflows       averagemonthlyussdoutflowsamountband    varchar\r\ndim_ussd_outflows       ussdoutflowscountband   varchar\r\ndim_ussd_outflows       averagemonthlyussdoutflowscountband     varchar\r\nfact_client_profile     monthenddate    date\r\nfact_client_profile     monthlysnapshotdatekey  bigint\r\nfact_client_profile     cifnumber       int\r\nfact_client_profile     isactiveclient  int\r\nfact_client_profile     clientamountbandname    varchar\r\nfact_client_profile     inflowsamount   decimal\r\nfact_client_profile     averagemonthlyinflowsamount     decimal\r\nfact_client_profile     atmwithdrawalamount     decimal\r\nfact_client_profile     atmwithdrawalcount      int\r\nfact_client_profile     averagemonthlyatmwithdrawalamount       decimal\r\nfact_client_profile     averagemonthlyatmwithdrawalcount        decimal\r\nfact_client_profile     cashbackamount  decimal\r\nfact_client_profile     cashbackcount   int\r\nfact_client_profile     averagemonthlycashbackamount    decimal\r\nfact_client_profile     averagemonthlycashbackcount     decimal\r\nfact_client_profile     appoutflowsamount       decimal\r\nfact_client_profile     appoutflowscount        int\r\nfact_client_profile     averagemonthlyappoutflowsamount decimal\r\nfact_client_profile     averagemonthlyappoutflowscount  decimal\r\nfact_client_profile     ussdoutflowsamount      decimal\r\nfact_client_profile     ussdoutflowscount       int\r\nfact_client_profile     averagemonthlyussdoutflowsamount        decimal\r\nfact_client_profile     averagemonthlyussdoutflowscount decimal\r\nfact_client_profile     internetbankingoutflowsamount   decimal\r\nfact_client_profile     internetbankingoutflowscount    int\r\nfact_client_profile     averagemonthlyinternetbankingoutflowsamount     decimal\r\nfact_client_profile     averagemonthlyinternetbankingoutflowcount       decimal\r\nfact_client_profile     debitorderamount        decimal\r\nfact_client_profile     debitordercount int\r\nfact_client_profile     averagemonthlydebitorderamount  decimal\r\nfact_client_profile     averagemonthlydebitordercount   decimal\r\nfact_client_profile     debitorderdisputeamount decimal\r\nfact_client_profile     debitorderdisputecount  int\r\nfact_client_profile     debitordernetamount     decimal\r\nfact_client_profile     posamount       decimal\r\nfact_client_profile     poscount        int\r\nfact_client_profile     averagemonthlyposamount decimal\r\nfact_client_profile     averagemonthlyposcount  decimal\r\nfact_client_profile     cnpamount       decimal\r\nfact_client_profile     cnpcount        int\r\nfact_client_profile     averagemonthlycnpamount decimal\r\nfact_client_profile     averagemonthlycnpcount  decimal\r\nfact_client_profile     sstoutflowsamount       decimal\r\nfact_client_profile     sstoutflowscount        int\r\nfact_client_profile     averagemonthlysstoutflowsamount decimal\r\nfact_client_profile     averagemonthlysstoutflowcount   decimal\r\nfact_client_profile     totalcashoutflowsamount int\r\nfact_client_profile     totaldigitalchanneloutflowamount        decimal\r\nfact_client_profile     totalcardchanneloutflowsamount  decimal\r\nfact_client_profile     totalelectronicoutflowsamount   decimal\r\nfact_client_profile     cashtoelectronicratio   decimal\r\nfact_client_profile     stableinflowscount      int\r\nfact_client_profile     averagemonthlyclientamountbandkey       bigint\r\nfact_client_profile     behaviourscore  int\r\nfact_client_profile     isfullybanked   int\r\nfact_client_profile     productbehaviourscore   int\r\nfact_client_profile     appoutflowskey  bigint\r\nfact_client_profile     atmwithdrawalskey       bigint\r\nfact_client_profile     cashbackkey     bigint\r\nfact_client_profile     cnpkey  bigint\r\nfact_client_profile     debitorderdisputekey    bigint\r\nfact_client_profile     debitorderoutflowskey   bigint\r\nfact_client_profile     iboutflowskey   bigint\r\nfact_client_profile     inflowskey      bigint\r\nfact_client_profile     otheroutflowskey        bigint\r\nfact_client_profile     poskey  bigint\r\nfact_client_profile     sstoutflowskey  bigint\r\nfact_client_profile     tenurekey       bigint\r\nfact_client_profile     ussdoutflowskey bigint\r\nfact_client_profile     classificationkey       bigint\r\nfact_client_profile     starclassificationkey   bigint\r\nfact_client_profile     regionalkey     bigint\r\nfact_client_profile     demographicskey bigint\r\nfact_client_profile     clientpersona   varchar\r\nfact_client_profile     clientpersonadetail     varchar\r\nfact_client_profile     modifiedts      datetime2\r\nfact_client_profile     clientmonthkey  bigint\r\nfact_client_profile     clientpersonadetail     varchar\r\nfact_client_profile     clientpersonadetail     varchar\r\nfact_client_profile     modifiedts      datetime2\r\nfact_client_profile     clientmonthkey  bigint\r\nfact_client_profile     clientpersonadetail     varchar\r\nfact_client_profile     modifiedts      datetime2\r\nfact_client_profile     clientmonthkey  bigint\r\nfact_client_profile     clientbankingdefinitionkey      bigint\r\nkeymeasures     Value   varchar")]
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