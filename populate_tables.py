from sqlalchemy import create_engine, MetaData, select
from faker import Faker 
import sys 
import random 
import datetime 

# Set up connections between sqlalchemy and postgres dbapi
# Instantiate metadata object
engine = create_engine("postgresql://postgres:postgres@localhost:5432/fakedata")
metadata = MetaData()

# Instantiate faker object
faker = Faker()

# Reflect metadata/schema from existing postgres database to bring in existing tables 
with engine.connect() as conn: 
    metadata.reflect(conn)

customers = metadata.tables["customers"]
products = metadata.tables["products"]
stores = metadata.tables["stores"]
transactions = metadata.tables["transactions"]

# product list
product_list = ["hat", "cap", "shirt", "sweater", "sweatshirt", "shorts", 
    "jeans", "sneakers", "boots", "coat", "accessories"]


class GenerateData:
    """
    generate a specific number of records to a target table in the 
    postgres database
    """

    def __init__(self):
        """
        initialize command line arguments
        """
        self.table = sys.argv[1]
        self.num_records = int(sys.argv[2])

    
    def create_data(self):
        """
        using faker library, generate data and execute DML 
        """
        
        if self.table not in metadata.tables.keys():
            return print(f"{self.table} does not exist")

        if self.table == "customers":
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    insert_stmt = customers.insert().values(
                        first_name = faker.first_name(),
                        last_name = faker.last_name(),
                        email = faker.email(),
                        address = faker.address(),
                        dob = faker.date_of_birth(minimum_age=16, maximum_age=60)
                    )
                    conn.execute(insert_stmt)

        if self.table == "products":
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    insert_stmt = products.insert().values(
                        name = random.choice(product_list),
                        price = faker.random_int(1,100000) / 100.0
                    )
                    conn.execute(insert_stmt)

        if self.table == "stores":
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    insert_stmt = stores.insert().values(
                        address = faker.address()
                    )
                    conn.execute(insert_stmt)

        if self.table == "transactions":
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    date_obj = datetime.datetime.now() - datetime.timedelta(days=random.randint(0,30))

                    insert_stmt = transactions.insert().values(
                        transaction_date=date_obj.strftime("%Y/%m/%d"),
                        customer_id=random.choice(conn.execute(select([customers.c.customer_id])).fetchall())[0],
                        product_id=random.choice(conn.execute(select([products.c.product_id])).fetchall())[0],
                        store_id=random.choice(conn.execute(select([stores.c.store_id])).fetchall())[0]
                    )
                    conn.execute(insert_stmt)


if __name__ == "__main__":
    generate_data = GenerateData()
    generate_data.create_data()
