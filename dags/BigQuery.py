from google.cloud import bigquery

def head_table():
    try:
        client = bigquery.Client()

        table_id = 'ck-huynhhuuquoc.hoatuoi.hoatuoi_table'

        query = f"""
            SELECT *
            FROM `{table_id}`
            LIMIT 5
        """

        query_job = client.query(query)

        results = query_job.result()

        for row in results:
            print(row)

    except Exception as e:
        print(f"Error: {e}")

def count_row():
    try:
        client = bigquery.Client()

        table_id = 'ck-huynhhuuquoc.hoatuoi.hoatuoi_table'

        query = f"""
            SELECT COUNT(*) as total_rows
            FROM `{table_id}`
        """

        query_job = client.query(query)

        results = query_job.result()

        for row in results:
            print(f"Total rows in {table_id}: {row.total_rows}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    head_table()
    count_row()
