import pymysql
import json
from decimal import Decimal

# Set the database credentials
host = 'aws-simplified.c94quwu6kdey.me-south-1.rds.amazonaws.com'
port = 3306
user = 'admin'
password = 'Asd5350183'
database = 'superstore'

# Connect to the database
connection = pymysql.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    database=database
)

# Create a cursor object
cursor = connection.cursor()

# Execute a SQL query
query="""
    select customerID, sum(sales) sum_sales
    from orders
    group by 1
    order by 2 desc
    limit 10;
    """


cursor.execute(query)


# Fetch the results
results = cursor.fetchall()


# save data in a dic    
Dictionary = dict((x, y) for x, y in results)
new_dict = {"customerID": Dictionary}



# export dic into json file
with open('result.json', 'w') as fp:
    json.dump(new_dict, fp)

# Close the cursor and connection
cursor.close()
connection.close()

