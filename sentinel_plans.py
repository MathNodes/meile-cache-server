import requests
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# 1. Connect to MySQL server
try:
    connection = mysql.connector.connect(
        host='localhost',
        user='your_username',          # Replace with your MySQL username
        password='your_password',      # Replace with your MySQL password
        database='your_database_name'  # Replace with your MySQL database name
    )
    if connection.is_connected():
        cursor = connection.cursor()

        # 2. Fetch JSON data
        url = "https://lcd.sentinel.co/sentinel/plans?pagination.limit=5000"
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTP errors
        data = response.json()

        # 3. Insert data into Plans table
        insert_query = """
        INSERT INTO Plans (
            id, provider_address, duration, gigabytes, denom, amount, status, status_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        for plan in data['plans']:
            status_at_str = plan.get('status_at', '')
            # Truncate the fractional seconds to 6 digits and remove 'T' and 'Z'
            status_at_str = status_at_str.replace('T', ' ').rstrip('Z')  # Replace T with space and remove 'Z'
            status_at_str = status_at_str[:23]  # Keep only 6 digits in fractional seconds
            
            # Now parse the ISO format string
            dt = datetime.fromisoformat(status_at_str)
            
            # Convert to MySQL format
            status_at = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
            
            # Extract the first price entry (since prices is an array)
            price = plan['prices'][0] if plan['prices'] else {'denom': None, 'amount': None}
            
            # Convert gigabytes and amount to integers
            gigabytes = int(plan['gigabytes']) if plan['gigabytes'] else None
            amount = int(price['amount']) if price['amount'] else None

            # Prepare values for insertion
            record = (
                plan['id'],
                plan['provider_address'],
                plan['duration'],
                gigabytes,
                price['denom'],
                amount,
                plan['status'],
                plan['status_at']  # MySQL will automatically handle fractional seconds
            )

            cursor.execute(insert_query, record)

        # Commit changes
        connection.commit()
        print(f"Inserted {cursor.rowcount} rows into the Plans table")

except Error as e:
    print(f"MySQL Error: {e}")
except requests.exceptions.RequestException as e:
    print(f"HTTP Request Error: {e}")
finally:
    # Close connection
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection closed")