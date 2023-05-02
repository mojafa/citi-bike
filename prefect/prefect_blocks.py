from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_dbt.cli import BigQueryTargetConfigs, DbtCliProfile, DbtCoreOperation


your_GCS_bucket_name = "citi_bike_datalake_citi-bike-385512"  # (1) insert your GCS bucket name
gcs_credentials_block_name = "citibike"

credentials_block = GcpCredentials(
    service_account_info={
    "type": "service_account",
    "project_id": "citi-bike-385512",
    "private_key_id": "9463363d954e464cd57ab250534266865d70e24d",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDbwxTZzZGNyei/\nrTjG2TTE/o2M4lXH2puFbyVPJC0lS7m4/2Timlsusl63C+sm2WKV8tkJcpAhCE51\nb6JAfSodRutIuTumZOq62Df6Y79xlWkPSmX6Rr6ntnnGLNS5ENafUrADKeGiwTZr\nkNoWsYJRAW6Ag1jTgNwDCGHzEw4kMp1i1xBFKLkCnI4enSI87VqJmcAtdtYkLdQD\njFCpz7PWNw0nr1tiqErt81LFsoqlAX0Ms4kpIxiepT52MKO8JMLfI8f3ZuQLv+y/\nKU93B7brSdaYixpBeIUN5GVZlVUItUw5Toa2pkXkKbk2yQWNYmP7/PRwtN9BhEM/\nHsDG0AwlAgMBAAECggEADeVX/C2zdnO98+DQJkK1w38VFEN7iReZneZZRZfUAve2\n1qvPhUSdX2aH9qsaQ2qbgT/J2X8U/gG4dFSKveksMI27cu3Dh9H5xbbGU17bTCNW\ncVETAbBXfeNb79wF123pJEnmN0z+p3vwJP0TzTuMMfMVOasUgsszHj35c5VQ35j1\nr5bvYyla+tFGD+kchLMddw1sYmYWdK6n23OcqqG388GZvHktwDISATOqHxEqjW9x\nBzPcFW8tMXfT1KbfS7GtZt2Ji9Lk3cpprua716xjm8M4YKUd7SjPyMJF5FkpWtqx\n2FSf3On3KA09XoCFdXGk/hiYJZOrZK9VObSsER9aAQKBgQD65Eh3jR7nc3rZVSP7\n8P8kyaHfuEcn5LzpwN/36tN/65Ux9Q+X8ITDW7eJmqCZy89+YdBRcL4IR6YneaJR\noCKh7/IyAoPQ5JnDrarck2aAxb/A2mLwEk4TYLNFR0R+MDfI6xnaYKtWjORKqmXr\nLMA8iGomsenOEr5DB/dkj6mWPQKBgQDgPIq0wl7v8Tdlo/bmL0n/v0uuDzoBmRI9\npsiCXjhWLEQcjWDckMpvpabS4jflEuT+QdA8FpZShiFbZ6HYYCYjXzKMe4gZCoeN\nT70LNCvhoKtvczEmFwcMws9a96/egUJpa3VnDTf3H6X2OX2LrUqHWAv+SmsheD35\nugB0kWEUCQKBgQCvbO8+qG3zZ8t+Kwn+H4RHNLT6uN8IBegRicsQjiFUUGUBiVhd\n5M7vyjGLBZNF9jwfIkWGrE4ze+WxtFrKuC1/DUdLsHZ6mVzqdYQCtw30/FAXiJul\nQSdWZUb0KeC6Wvymf7yT9QPYgKsfigW5apD/wmJ2q+/PJ0vhkrBooAr0AQKBgBgJ\nRVEE+Mo9kBOcFM4tyX/ZkJIy3aPoNZVYOGwJD37lNdPdr8FU3+5B0nUOfLFYaiV7\nBog6X5iu+gpjPG0GOXBXNwLqBvewMkGKh5gY2o9P+rByp5UOqNnMVA/LNxXhwy2r\nsCUAvLwHr3GKThPX2oJRhM+YSI2I5xPKxVu5ba+JAoGAM2XGK9IPzlG7QIqe/CeS\njVVS+lJRYEwx5TDw+ZdYILKk1Ccx6S6vzoqaA7AH3lGaZxIy742N1CBR2ohhN71D\n6m3GGjHlhzosUkAVsbR0PM9XwvaTznf+uewroeA6JPq438upio+d0MWoNvi+gekB\nt2kEqwxfkpZg5ICI93FMJZc=\n-----END PRIVATE KEY-----\n",
    "client_email": "terraform-iam@citi-bike-385512.iam.gserviceaccount.com",
    "client_id": "102648729695913965013",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/terraform-iam%40citi-bike-385512.iam.gserviceaccount.com"
}
)


credentials_block.save(f"{gcs_credentials_block_name}", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("citibike"),
    bucket="citi_bike_datalake_citi-bike-385512",
)

bucket_block.save(f"{gcs_credentials_block_name}-bucket", overwrite=True)


credentials = GcpCredentials.load(gcs_credentials_block_name)
target_configs = BigQueryTargetConfigs(
    schema="citi_bike_dbt",
    credentials=credentials,
)
target_configs.save("citi-bike-dbt-target-config", overwrite=True)

dbt_cli_profile = DbtCliProfile(
    name="citi-bike-dbt-cli-profile",
    target="dev",
    target_configs=target_configs,
)
dbt_cli_profile.save("citi-bike-dbt-cli-profile", overwrite=True)
