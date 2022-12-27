
import random
from google.cloud import bigquery
import pandas
from faker import Faker
import uuid
import time

start_time = time.time()


class Record:

    def __init__(self):
        self.fake = Faker()
        self.name_list = [
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

    def fake_values(self):
        self.name = random.choice(self.name_list)
        self.id = str(uuid.uuid1())
        self.archive_date = self.fake.date_between(
            start_date='-6M', end_date='-3M')
        self.dispute_date = self.fake.date_between(
            start_date='-3M', end_date='now')

        # return name, id, archive_date, dispute_date


class Record_list:

    def __init__(self):
        self.disputes_records = []
        self.archives_records = []

    def fake_array(self, fake):

        self.disputes_records.insert(0, {
            "id": id,
            "name": fake.name,
            "dispute_date": fake.dispute_date
        })

        self.archives_records.insert(0, {
            "id": id,
            "archive_date": fake.archive_date
        })

        self.disputes_df = pandas.DataFrame(
            self.disputes_records,
            columns=[
                "id",
                "name",
                "dispute_date"
            ])

        self.archives_df = pandas.DataFrame(
            self.archives_records,
            columns=[
                "id",
                "archive_date"
            ]
        )


fake_record = Record()  # instantiate object
fake_record.fake_values()  # set fake values

fake_records = Record_list()
fake_records.fake_array(fake_record)


def main(request):
    client = bigquery.Client()

    print('Creating Records')

    # reset records to insert
    disputes_records = []
    archives_records = []

    for x in range(5000):
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
    print('Done Creating Records')
    print("--- %s seconds ---" % (time.time() - start_time))

    ######################
    #### Write to BQ #####
    ######################
    print('Writing Archives to BigQuery')

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

    print('Done Writing Archives to BigQuery')
    print("--- %s seconds ---" % (time.time() - start_time))

    ######################
    #### Write to BQ #####
    ######################
    print('Writing Disputes to BigQuery')

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "name", bigquery.enums.SqlTypeNames.STRING),
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

    print('Done Writing Disputes to BigQuery')
    print("--- %s seconds ---" % (time.time() - start_time))  # done

    return 'finish'
