from faker import Faker
import csv
import uuid
from datetime import datetime, timedelta

fake = Faker()

# Fields: id, created_at, event_type, country, device_type
def create_row():
    id = uuid.uuid4()
    two_years_ago = datetime.now() - timedelta(days=730)
    created_at = fake.date_time_between(start_date=two_years_ago, end_date='now').isoformat()
    event_type = fake.random_element(elements=('click', 'view', 'purchase'))
    country = fake.country_code(representation='alpha-2')
    device_type = fake.random_element(elements=('mobile', 'desktop', 'tablet'))
    return [str(id), created_at, event_type, country, device_type]

with open('data.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    for _ in range(1000 * 3000):
        writer.writerow(create_row())