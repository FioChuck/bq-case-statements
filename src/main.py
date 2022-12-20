
import random
from google.cloud import bigquery
import pandas
from faker import Faker
import uuid
import time
# Construct a BigQuery client object.
client = bigquery.Client()
start_time = time.time()

loop = 40
for i in range(loop):
    print('Percent Complete: ' + str(100*i/loop))
    print("--- %s seconds ---" % (time.time() - start_time))

    # reset records to insert
    disputes_records = []
    archives_records = []

    for x in range(10000):
        id = str(uuid.uuid1())

        fake = Faker()  # fake proxy object
        archive_date = fake.date_between(start_date='-6M', end_date='-3M')
        dispute_date = fake.date_between(start_date='-3M', end_date='now')

        # initializing name list
        name_list = [
            "Ria",
            "Gary",
            "Miah",
            "Gabriela",
            "Musa",
            "Jaime",
            "Solomon",
            "Chelsea",
            "Connor",
            "Alessia"
        ]

        random_name = random.choice(name_list)

        disputes_records.insert(0, {
            "id": id,
            "name": random_name,
            "dispute_date": dispute_date
        })

        archives_records.insert(0, {
            "id": id,
            "archive_date": archive_date
        })

    disputes_df = pandas.DataFrame(
        disputes_records,
        columns=[
            "id",
            "name",
            "dispute_date"
        ])

    archives_df = pandas.DataFrame(
        archives_records,
        columns=[
            "id",
            "archive_date"
        ]
    )

    ######################
    #### Write to BQ #####
    ######################

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "archive_date", bigquery.enums.SqlTypeNames.DATE),
        ],
        write_disposition="WRITE_APPEND",
    )

    table_id = "cf-data-analytics.case_statement.archives"
    job = client.load_table_from_dataframe(
        archives_df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    ######################
    #### Write to BQ #####
    ######################

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "dispute_date", bigquery.enums.SqlTypeNames.DATE),
        ],
        write_disposition="WRITE_APPEND",
    )

    table_id = "cf-data-analytics.case_statement.disputes"
    job = client.load_table_from_dataframe(
        disputes_df, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.
