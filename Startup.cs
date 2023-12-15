using System;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Azure.Identity;

[assembly: FunctionsStartup(typeof(FileTransferService.Functions.ReportProcessingEvents.Startup))]

namespace FileTransferService.Functions.ReportProcessingEvents
{
    class Startup  : FunctionsStartup
    {
        public override void ConfigureAppConfiguration(IFunctionsConfigurationBuilder builder)
        {
            string appConfigurationConnString = Environment.GetEnvironmentVariable("AppConfigurationConnString");
            builder.ConfigurationBuilder.AddAzureAppConfiguration(options =>
            {
                options.Connect(appConfigurationConnString)
                    .ConfigureKeyVault(kv =>
                    {
                        kv.SetCredential(new DefaultAzureCredential(
                                new DefaultAzureCredentialOptions
                                {
                                    AuthorityHost = AzureAuthorityHosts.AzureGovernment
                                }
                            ));
                    });
            });
        }

        public override void Configure(IFunctionsHostBuilder builder)
        {
        }
    }
}
