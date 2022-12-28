# TL;DR

A simple Python GCP Cloud Function to insert mock transaction data into BigQuery.

The mock data and BigQuery statements _(below)_ are used to highlight a SQL anti-pattern and recommend alternatives.

The two SQL queries shown below achieve the same result; however, one technique is more optimized. This example was created to document the importance of columnar transformations versus multiple left joins.

# Optimized Query

```sql
SELECT
  t0.id,
  t0.name,
  t0.dispute_date,
  t1.archive_date,
  CASE
    WHEN DATE_DIFF(t1.archive_date, t0.dispute_date,month) = -1 THEN t1.archive_date
END
  AS archive_date1,
  CASE
    WHEN DATE_DIFF(t1.archive_date, t0.dispute_date,month) = -2 THEN t1.archive_date
END
  AS archive_date2,
  CASE
    WHEN DATE_DIFF(t1.archive_date, t0.dispute_date,month) = -3 THEN t1.archive_date
END
  AS archive_date3,
  CASE
    WHEN DATE_DIFF(t1.archive_date, t0.dispute_date,month) = -4 THEN t1.archive_date
END
  AS archive_date4,
  CASE
    WHEN DATE_DIFF(t1.archive_date, t0.dispute_date,month) = -5 THEN t1.archive_date
END
  AS archive_date5,
  CASE
    WHEN DATE_DIFF(t1.archive_date, t0.dispute_date,month) = -6 THEN t1.archive_date
END
  AS archive_date6
FROM
  case_statement.disputes AS t0
LEFT JOIN
  `cf-data-analytics.case_statement.archives` AS t1
ON
  t0.id = t1.id
```

# Inefficient Left Join

```sql
SELECT
  t0.*,
  t1.archive_date AS archive_date1,
  t2.archive_date AS archive_date2,
  t3.archive_date AS archive_date3,
  t4.archive_date AS archive_date4,
  t5.archive_date AS archive_date5,
  t6.archive_date AS archive_date6
FROM
  case_statement.disputes AS t0
LEFT JOIN
  `cf-data-analytics.case_statement.archives` AS t1
ON
  t0.id = t1.id
  AND DATE_DIFF(t1.archive_date, t0.dispute_date,month) = -1
LEFT JOIN
  `cf-data-analytics.case_statement.archives` AS t2
ON
  t0.id = t2.id
  AND DATE_DIFF(t2.archive_date, t0.dispute_date,month) = -2
LEFT JOIN
  `cf-data-analytics.case_statement.archives` AS t3
ON
  t0.id = t3.id
  AND DATE_DIFF(t3.archive_date, t0.dispute_date,month) = -3
LEFT JOIN
  `cf-data-analytics.case_statement.archives` AS t4
ON
  t0.id = t4.id
  AND DATE_DIFF(t4.archive_date, t0.dispute_date,month) = -4
  LEFT JOIN
  `cf-data-analytics.case_statement.archives` AS t5
ON
  t0.id = t5.id
  AND DATE_DIFF(t5.archive_date, t0.dispute_date,month) = -5
  LEFT JOIN
  `cf-data-analytics.case_statement.archives` AS t6
ON
  t0.id = t6.id
  AND DATE_DIFF(t6.archive_date, t0.dispute_date,month) = -6
```

# Setup

This project includes a yaml file for deployment to Google Cloud using Github Actions maintained here: https://github.com/google-github-actions/deploy-cloud-functions. The Github Action Workflow requires several _"Action Secrets"_ used to set environment variables during deployment. Set the following secrets in the repository before deployment.

| Action Secret  | Value                                                          |
| -------------- | -------------------------------------------------------------- |
| GCP_PROJECT_ID | GCP Project ID where Function will be deployed                 |
| GCP_SA_KEY     | Service Account Key used to authenticate GitHub to GCP Project |

Two tables are also required in BigQuery to run this example.

## `archives` table

```json
[
  {
    "name": "id",
    "type": "STRING"
  },
  {
    "name": "archive_date",
    "type": "DATE"
  }
]
```

## `disputes` table

```json
[
  {
    "name": "id",
    "type": "STRING"
  },
  {
    "name": "name",
    "type": "STRING"
  },
  {
    "name": "dispute_date",
    "type": "DATE"
  }
]
```
