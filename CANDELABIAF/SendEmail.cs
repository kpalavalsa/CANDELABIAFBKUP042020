using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using System.Net.Mail;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Configuration;
using System;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace CANDELABIAF
{
    public static class SendEmail
    {
        private static string _invocationId;
        private static TraceWriter _logger;

        [FunctionName("SendEmail")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequestMessage req, ExecutionContext exCtx, TraceWriter log)
        {
            _logger = log;
            _logger.Info($"Function-({_invocationId}): Triggered a Function which processess email");


            // parse query parameter
            string RunId = req.GetQueryNameValuePairs()
                .FirstOrDefault(q => string.Compare(q.Key, "RunId", true) == 0)
                .Value;

            if (RunId == null)
            {
                // Get request body
                dynamic data = await req.Content.ReadAsAsync<object>();
                RunId = data?.RunId;
            }

            string ProcessName = req.GetQueryNameValuePairs()
                .FirstOrDefault(q => string.Compare(q.Key, "ProcessName", true) == 0)
                .Value;

            if (ProcessName == null)
            {
                // Get request body
                dynamic data = await req.Content.ReadAsAsync<object>();
                ProcessName = data?.ProcessName;
            }

            if (ProcessName is null)
            {
                ProcessName = "NULL";
            }

            var str = "Server = tcp:sc-az-datacontrol-srv1.database.windows.net,1433; Initial Catalog = CandelaKPI-DEV; Persist Security Info = False; User ID = Dev; Password =fba4bUyzBV7QvXEq; MultipleActiveResultSets = False; Encrypt = True; TrustServerCertificate = False; Connection Timeout = 30;";
            try
            {

                using (SqlConnection conn = new SqlConnection(str))
                {
                    conn.Open();
                    //SecretRequest secretRequest = await req.Content.ReadAsAsync<SecretRequest>();

                    //if (string.IsNullOrEmpty(secretRequest.Secret))
                    //    return req.CreateResponse(HttpStatusCode.BadRequest, "Request does not contain a valid Secret.");


                    /* Commented for time being
                    AzureServiceTokenProvider serviceTokenProvider = new AzureServiceTokenProvider();

                    var keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(serviceTokenProvider.KeyVaultTokenCallback));
                    //var secretUri = SecretUri(secretRequest.Secret);

                    string secretUri = "https://CandelaBIKV.vault.azure.net/Secrets/VamshiEmailAccount";
                    SecretBundle secretValue;
                    try
                    {
                        secretValue = await keyVaultClient.GetSecretAsync(secretUri).ConfigureAwait(false);
                        //log.Info($"Function-({_invocationId}): Success in fetching the secret value from KeyVault ");
                        Logger("Success in fetching the secret value from KeyVault", "INFO", null, ProcessName, RunId, conn);
                    }
                    catch (Exception kex)
                    {
                        Logger("Error in getting the Secret value from KeyVault for the secret: VisionBIServiceAccount", "ERROR", kex.Message.Replace("'", ""), ProcessName, RunId, conn);
                        //log.Error($"Function-({_invocationId}): Error in getting the Secret value from KeyVault for the secret: VisionBIServiceAccount ", kex);
                        return req.CreateResponse(HttpStatusCode.ExpectationFailed, $"{kex}");
                    }
                    */

                    string fromEmail = "vamshi@snp.com";//"vamshi.krishna@candelamedical.com";// "vamshi @snp.com";
                    //string fromEmail =  "vamshi@snp.com";
                    string toEmail = "data@candelamedical.com";
                    int smtpPort = 587;
                    bool smtpEnableSsl = true;
                    string smtpHost = "smtp.office365.com"; // your smtp host
                    string smtpUser = "vamshi@snp.com";//"vamshi.krishna@candelamedical.com";// "Vamshi @snp.com"; // your smtp user Configuration["Values:SMTPUser"]; //
                    //string smtpUser =  "Vamshi@snp.com"; // your smtp user Configuration["Values:SMTPUser"]; //
                    string smtpPass = "M@hathi5";//secretValue.Value;// ""; // your smtp password
                    //string smtpPass =  ""; // your smtp password
                    string subject = "DataLoad Process";
                    string message = "";
                    //int emailstobesend;
                    string Error = "";

                    MailMessage mail = new MailMessage(fromEmail, toEmail);
                    SmtpClient client = new SmtpClient();
                    client.Port = smtpPort;
                    client.EnableSsl = smtpEnableSsl;
                    client.DeliveryMethod = SmtpDeliveryMethod.Network;
                    client.UseDefaultCredentials = false;
                    client.Host = smtpHost;
                    client.Credentials = new System.Net.NetworkCredential(smtpUser, smtpPass);
                    mail.Priority = MailPriority.High;
                    mail.IsBodyHtml = true;
                    mail.CC.Add("vamshi@snp.com");
                    //mail.Bcc.Add("vamshi@snp.com");

                    Logger("Generating Email Body is started", "INFO", null, ProcessName, RunId, conn);

                    string sql = "SELECT [ProcessName],[Summary],[Status],[ErrorInfo],[LoggedTime],[RecordsInSource],[RecordsCopiedToDest],[SkippedRecordsToMove],[CopyDuration(Seconds)] as Duration,LogsPath FROM BI.LG_ProcessLogs where RunId = '" + RunId + "' Order by ID";

                    subject = "Data Load is completed and below are the details - " + RunId;

                    message = "";
                    if (conn.State == ConnectionState.Closed)
                        conn.Open();
                    using (var command = new SqlCommand(sql, conn))
                    {
                        message = message + "<table border=1 ><tr><td style='background:#8EAADB;'><b>PROCESS NAME </b></td><td style='background:#8EAADB;'><b>SUMMARY</b></td><td style='background:#8EAADB;'><b>ERROR INFO</b></td><td style='background:#8EAADB;'><b>PROCESSTIME</b></td><td style='background:#8EAADB;'><b>NO.OF RECORDS IN SOURCE</b></td><td style='background:#8EAADB;'><b>NO.OF RECORDS COPIED TO DESTINATION</b></td><td style='background:#8EAADB;'><b>NO.OF RECORDS SKIPPED</b></td><td style='background:#8EAADB;'><b>DURATION(SECONDS)</b></td><td style='background:#8EAADB;'><b>LOGS PATH</b></td><td style='background:#8EAADB;'><b>STATUS</b></td></tr>";

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                message = message + "<tr><td>" + reader["ProcessName"] + "</td><td>" + reader["Summary"] + "</td><td>" + reader["ErrorInfo"] + "</td><td>" + reader["LoggedTime"].ToString() + "</td><td>" + reader["RecordsInSource"].ToString() + "</td><td>" + reader["RecordsCopiedToDest"].ToString() + "</td><td>" + reader["SkippedRecordsToMove"].ToString() + "</td><td>" + reader["Duration"] + "</td><td>" + reader["LogsPath"] + "</td>";//</tr>"

                                if (reader["Status"].Equals("ERROR"))
                                {
                                    message = message + "<td><b><span style='color:red'>" + reader["Status"] + "</span></b></td></tr>";
                                    Error = "1";
                                }
                                else if (reader["Status"].Equals("INFO"))
                                {
                                    message = message + "<td><b><span style='color:Green'>" + reader["Status"] + "</span></b></td></tr>";
                                }
                                else
                                {
                                    message = message + "<td><b><span style='color:Yellow'>" + reader["Status"] + "</span></td></b></tr> ";
                                }
                            }
                        }
                        message = message + "</table>";
                    }
                    message = message + "</body></html>";

                    if (Error == "1")
                    {
                        subject = "Errors in : " + subject;
                        message = "<html><body> <b><span style='color:red'>Noticed Errors in the data loading please check the below details:</span></b><br><br>" + message;
                    }
                    else
                    {
                        subject = subject;
                        message = "<html><body> " + message;
                    }

                    mail.Subject = subject;

                    mail.Body = message;

                    try
                    {
                        client.Send(mail);
                        Logger("Process Email is sent", "INFO", null, ProcessName, RunId, conn);
                        //log.Verbose("Email sent.");
                    }
                    catch (Exception ex)
                    {
                        Logger("Failed To Send an Email", "ERROR", ex.Message.Replace("'", ""), ProcessName, RunId, conn);
                        return req.CreateResponse(HttpStatusCode.ExpectationFailed, ex);
                        //log.Verbose("Error sent.");
                    }


                }
            }
            catch (Exception ex)
            {
                _logger.Error($"Function-({_invocationId}): Failed to process email for the ADF Run Id: {RunId}", ex);
                return req.CreateResponse(HttpStatusCode.ExpectationFailed, ex);
            }

            return req.CreateResponse(HttpStatusCode.OK);
        }

        private static void Logger(string Summary, string Status, string ErrorInfo, string ProcessName, string RunId, SqlConnection conn)
        {
            if (ErrorInfo is null)
                _logger.Info($"Function-({_invocationId}): {Summary}, For ADF Rund Id - {RunId}");
            else
                _logger.Error($"Function-({_invocationId}): {Summary} with Error: {ErrorInfo} , For ADF Rund Id - {RunId}");

            string Query = "Insert Into BI.LG_ProcessLogs(ProcessName,Summary,Status,ErrorInfo,RunId) values('" + ProcessName + "','" + Summary + "','" + Status + "','" + ErrorInfo + "','" + RunId + "')";
            PutData(Query, conn);
        }
        private static void PutData(string SqlQuery, SqlConnection conn)
        {
            if (conn.State == ConnectionState.Closed)
                conn.Open();

            SqlCommand command = new SqlCommand(SqlQuery, conn);
            command.CommandTimeout = 600;

            command.ExecuteNonQuery();

            conn.Close();

        }
    }
}
