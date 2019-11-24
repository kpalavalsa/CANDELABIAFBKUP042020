using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.AnalysisServices.Tabular;
using System.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Newtonsoft.Json;

namespace CANDELABIAF
{
    public static class ProcessAAS
    {
        private static string _invocationId;
        private static TraceWriter _logger;

        [FunctionName("PROCESSAAS")]
        public static HttpResponseMessage Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequestMessage req, Microsoft.Azure.WebJobs.ExecutionContext exCtx, TraceWriter log)
        {
            _logger = log;
            _logger.Info($"Function-({exCtx.InvocationId}): Triggered a Function which process Tabular Model");
            string superSecret = System.Environment.GetEnvironmentVariable("SuperSecret");
            _logger.Info($"Connection opened to DB : {superSecret}");
            // parse query parameter
            string modelname = req.GetQueryNameValuePairs()
                .FirstOrDefault(q => string.Compare(q.Key, "modelname", true) == 0)
                .Value;

            if (modelname == null)
            {
                // Get request body
                dynamic data = req.Content.ReadAsAsync<object>().Result;
                modelname = data?.modelname;
            }

            string ProcessName = req.GetQueryNameValuePairs()
                .FirstOrDefault(q => string.Compare(q.Key, "ProcessName", true) == 0)
                .Value;

            if (ProcessName == null)
            {
                // Get request body
                dynamic data = req.Content.ReadAsAsync<object>().Result;
                ProcessName = data?.ProcessName;
            }

            string RunId = req.GetQueryNameValuePairs()
                .FirstOrDefault(q => string.Compare(q.Key, "RunId", true) == 0)
                .Value;

            if (RunId == null)
            {
                // Get request body
                dynamic data = req.Content.ReadAsAsync<object>().Result;
                RunId = data?.RunId;
            }
            if (RunId is null)
            {
                RunId = "NULL";
            }
            if (ProcessName is null)
            {
                ProcessName = "NULL";
            }
            try
            {

                var str = "Server = tcp:sc-az-datacontrol-srv1.database.windows.net,1433; Initial Catalog = CandelaKPI-Dev; Persist Security Info = False; User ID = Dev; Password =fba4bUyzBV7QvXEq; MultipleActiveResultSets = False; Encrypt = True; TrustServerCertificate = False; Connection Timeout = 30;";
                SqlConnection conn = new SqlConnection(str);
                conn.Open();
                _logger.Info($"Connection opened to DB");
                Item output = new Item();
                string output1="";
                int retrycount = 0;
                if (modelname != null)
                {
                    do
                    {
                        try
                        {
                            retrycount++;
                            _logger.Info($"Triggered the model refresh");
                            output = CallRefreshAsync(modelname).Result;
                            output1 = JsonConvert.SerializeObject(output);
                            _logger.Info($"called the model refresh with Output: {output1}");

                            Logger(modelname + " - Data Model is refreshed", "INFO", null, ProcessName, RunId, conn);
                            break;
                        }
                        catch (Exception e)
                        {
                            if (retrycount == 3)
                            {
                                Logger(modelname + " - Data Model refresh is Failed", "ERROR", e.Message.Replace("'", ""), ProcessName, RunId, conn);
                                return req.CreateResponse(HttpStatusCode.ExpectationFailed, e);
                            }
                            else
                            {
                                Logger(modelname + " - Data Model refresh is Failed at retry attempt-"+retrycount.ToString(), "WARNING", e.Message.Replace("'", ""), ProcessName, RunId, conn);
                                
                                Thread.Sleep(60000);
                            }
                        }
                    } while (true);
                }
                if (output.status == "succeeded")
                    return req.CreateResponse(HttpStatusCode.OK);
                
                else
                    return req.CreateResponse(HttpStatusCode.ExpectationFailed);
            }
            catch (Exception ex)
            {
                return req.CreateResponse(HttpStatusCode.ExpectationFailed, ex);
            }

        }

        private static async Task<Item> CallRefreshAsync(string modelname)

        {
            string output = "";
            Item JsonOutput = new Item();
            HttpClient client = new HttpClient();
            
            client.BaseAddress = new Uri($"https://aspaaseastus2.asazure.windows.net/servers/candelakpiserver/models/{modelname}/");



            // Send refresh request

            client.DefaultRequestHeaders.Accept.Clear();

            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", await UpdateToken());



            RefreshRequest refreshRequest = new RefreshRequest()

            {

                type = "full",

                maxParallelism = 10

            };



            HttpResponseMessage response = await client.PostAsJsonAsync("refreshes", refreshRequest);
            _logger.Info($"Triggered the model refresh inside call refreshasync");

            response.EnsureSuccessStatusCode();

            Uri location = response.Headers.Location;

            Console.WriteLine(response.Headers.Location);

            // Check the response



            while (true) // Will exit while loop when exit Main() method (it's running asynchronously)

            {


                // Refresh token if required

                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", await UpdateToken());

                response = await client.GetAsync(location);


                if (response.IsSuccessStatusCode)

                {

                    output = await response.Content.ReadAsStringAsync();

                    JsonOutput = JsonConvert.DeserializeObject<Item>(output);

                }


                if (JsonOutput.status.ToLower() == "succeeded" || JsonOutput.status.ToLower() == "failed")
                {

                    goto X;
                }


            }

            X:;
            return JsonOutput;
        }



        private static async Task<string> UpdateToken()

        {

            string resourceURI = "https://*.asazure.windows.net";

            string clientID = "c9a05523-4152-4fec-9649-c0368ec60163"; // Native app with necessary API permissions



            //string authority = "https://login.windows.net/common/oauth2/authorize";

            string authority = "https://login.windows.net/899f8232-fd9d-4a3d-928f-f1f931b312e9";///oauth2/authorize"; // Authority address can optionally use tenant ID in place of "common". If service principal or B2B enabled, this is a requirement.

            AuthenticationContext ac = new AuthenticationContext(authority);



            //Interactive login if not cached:

            //AuthenticationResult ar = await ac.AcquireTokenAsync(resourceURI, clientID, new Uri("urn:ietf:wg:oauth:2.0:oob"), new PlatformParameters(PromptBehavior.Auto));



            //Username/password:

            //UserPasswordCredential cred = new UserPasswordCredential("", "");

            //AuthenticationResult ar = await ac.AcquireTokenAsync(resourceURI, clientID, cred);



            //Service principal:

            //12/19/2017: Bug disallows use of service principals. At time of writing, the fix is being rolled out to production clusters. Please retry soon if not working by the time you try it.
            //b5465d86-5fe0-4f4d-93fe-2e9281ccc11a is app registration  Application ID 
            //hQ6kzzUwCw2iDp/vyPxCYW/s8RtnP/zrCBuE9bVjBYc= is app registration  Secret Key 
            ClientCredential cred = new ClientCredential("c9a05523-4152-4fec-9649-c0368ec60163", "4cD]JIRZ*/U=ZUzKn92A4NqJJxMRUo.f");

            AuthenticationResult ar = await ac.AcquireTokenAsync(resourceURI, cred);



            return ar.AccessToken;

        }

        private static void Logger(string Summary, string Status, string ErrorInfo, string ProcessName, string RunId, SqlConnection conn)
        {
            if (ErrorInfo is null)
                _logger.Info($"Function-({_invocationId}): {Summary}, For ADF Rund Id - {RunId}");
            else
                _logger.Error($"Function-({_invocationId}): {Summary}, For ADF Rund Id - {RunId}, With Error - {ErrorInfo}");

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

    class RefreshRequest

    {

        public string type { get; set; }

        public int maxParallelism { get; set; }

    }
    class Objects
    {
        public string table { get; set; }
        public string partition { get; set; }
        public string status { get; set; }
    }
    class Item
    {
        public DateTime startTime { get; set; }
        public string type { get; set; }
        public string status { get; set; }
        public string currentRefreshType { get; set; }
        public List<Objects> Objects { get; set; }
    }

}
