import base64
import os
from datetime import datetime
import pandas as pd

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition

def send_email():
    now = pd.Timestamp.now()
    year = now.year
    month = now.month
    day = now.day
    file_path = f'/opt/airflow/data/clean_data/{year}/{month}/{day}/hoatuoi_cleaned.csv'
    # file_path = 'C:\\Users\\ACER\\Desktop\\hoatuoi\\data\\clean_data\\2024\\5\\12\\hoatuoi_cleaned.csv'
    with open(file_path, 'rb') as f:
        data = f.read()
        f.close()

    encoded_file = base64.b64encode(data).decode()

    message = Mail(
        from_email='20067161.quoc@student.iuh.edu.vn',  # Khong thay doi
        to_emails='quoc7246329@gmail.com',
        subject='Big Data - 20067161 - Nhom 6',
        html_content='Chao Co,<br>Em gui file data da duoc clean. Em xin cam on!')

    attached_file = Attachment(
        FileContent(encoded_file),
        FileName('data_sau_cleaned.csv'), 
        FileType('text/csv'),
        Disposition('attachment')
    )
    message.attachment = attached_file

    try:
        sg = SendGridAPIClient("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")
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
        print(datetime.now())
    except Exception as e:
        print(str(e))

# Use the function
send_email()