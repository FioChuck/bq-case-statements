
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
        # set name to random value from list
        self.name = random.choice(self.name_list)

        self.id = str(uuid.uuid1())  # set id to uuid value
        self.archive_date = self.fake.date_between(
            start_date='-6M', end_date='-3M')
        self.dispute_date = self.fake.date_between(
            start_date='-3M', end_date='now')


class Record_list:

    def __init__(self):
        self.disputes_records = []
        self.archives_records = []

    def fake_list(self, fake):

        self.disputes_records.insert(0, {
            "id": fake.id,
            "name": fake.name,
            "dispute_date": fake.dispute_date
        })

        self.archives_records.insert(0, {
            "id": fake.id,
            "archive_date": fake.archive_date
        })

    def fake_df(self):
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


def bq_write(df, table, schema):

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",  # append values into bigquey table
    )

    job = client.load_table_from_dataframe(
        df, table, job_config=job_config
    )  # Make an API request.

    job.result()  # Wait for the job to complete.


######### CALCULATIONS ###################

archive_schema = [
    bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("archive_date", bigquery.enums.SqlTypeNames.DATE)
]

disputes_schema = [
    bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("name", bigquery.enums.SqlTypeNames.STRING),
    bigquery.SchemaField("dispute_date", bigquery.enums.SqlTypeNames.DATE),
]


fake_record = Record()  # instantiate Record object
fake_record_array = Record_list()  # instatiate Record_list object

for x in range(1000000):
    fake_record.fake_values()  # set fake values
    fake_record_array.fake_list(fake_record)  # insert values into list

fake_record_array.fake_df()  # insert list into pandas dataframe

print('Done Creating Dataframes')
print("--- %s seconds ---" % (time.time() - start_time))

# 100,000 in 10.0 seconds

# bq_write(fake_record_array.archives_df,
#          "cf-data-analytics.case_statement.archives", archive_schema)
