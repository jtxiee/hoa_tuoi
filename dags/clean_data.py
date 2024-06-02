import pandas as pd
import os


def clean_data():
    # directory = 'C:\\Users\\ACER\\Desktop\\hoatuoi\\data\\crawl_data\\hoatuoi_job.csv'
    # directory = '/opt/airflow/data/crawl_data/hoatuoi_job.csv'
    # df = pd.read_csv(directory)

    # directory = 'C:\\Users\\ACER\\Desktop\\hoatuoi\\data\\crawl_data\\hoatuoi_job.json'
    directory = '/opt/airflow/data/crawl_data/hoatuoi_job.json'
    df = pd.read_json(directory)

    # In ra 5 dong dau cua du lieu
    # print(df.head())

    #Xoa ky hieu thua
    df = df.replace({'\n': ''}, regex=True)
    df = df.replace({'\t': ''}, regex=True)
    df = df.replace({'\r': ''}, regex=True)
    print(df)

   # Check missing data
    missing_data = df.isnull()
    print('data bi thiu:',missing_data)

    total_missing_count = missing_data.sum()
    print('data bi thiu:',total_missing_count)

    # Kiểm tra xem có giá trị null trong cột 'mota' không
    if df['mota'].isnull().any():
        # Thay thế các giá trị null trong cột 'mota' bằng giá trị mới 'khong co'
        df['mota'].fillna('khong co', inplace=True)
        print("Đã thực hiện thay thế giá trị null trong cột 'mota' thành 'khong co'")
    else:
        print("Không có giá trị null trong cột 'mota'")

    # Thay thế mọi giá trị rỗng trong cột 'mota' bằng giá trị mặc định 'khong co'
    df['mota'] = df['mota'].replace('', 'khong co')

    # In ra cột 'mota' sau khi thực hiện thay thế
    print(df['mota'])






    # Fill missing data which salary is missing we will fill it with 0
    # df['salary'] = df['salary'].fillna("0")
    # missing_data = df.isnull()
    # print(missing_data)

    # Save data
    now = pd.Timestamp.now()
    year = now.year
    month = now.month
    day = now.day

    # data_dir = f'C:\\Users\\ACER\\Desktop\\hoatuoi\\data\\clean_data\\{year}\\{month}\\{day}'
    data_dir = f'/opt/airflow/data/clean_data/{year}/{month}/{day}'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir, exist_ok=True)

    # Save data to csv
    df.to_csv(f'{data_dir}/hoatuoi_cleaned.csv', index=False)

    # Save data to json
    df.to_json(f'{data_dir}/hoatuoi_cleaned.json', orient='records')


clean_data()
