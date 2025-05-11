import mysql.connector
import requests
import json
from datetime import datetime

# Database configuration - Modify these values accordingly
db_config = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_database'
}

# Connect to the MySQL database
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Step 1: Fetch all 'id' values from the 'Plans' table
    cursor.execute("SELECT id FROM Plans")
    ids = cursor.fetchall()

    # Step 2: Loop through each id and fetch data from the API
    for (plan_id,) in ids:
        url = f"https://lcd.sentinel.co/sentinel/plans/{plan_id}"
        print(f"Fetching data for plan ID: {plan_id}")

        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise exception for HTTP errors

            # Step 3: Parse the JSON response
            data = response.json()
            plan = data.get('plan')

            if plan:
                # Step 4: Prepare data for insertion
                status_at_str = plan.get('status_at', '')
                # Truncate the fractional seconds to 6 digits and remove 'T' and 'Z'
                status_at_str = status_at_str.replace('T', ' ').rstrip('Z')  # Replace T with space and remove 'Z'
                status_at_str = status_at_str[:23]  # Keep only 6 digits in fractional seconds
                
                # Now parse the ISO format string
                dt = datetime.fromisoformat(status_at_str)
                
                # Convert to MySQL format
                status_at = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                price = plan['prices'][0] if plan['prices'] else {'denom': None, 'amount': None}
                amount = int(price['amount']) if price['amount'] else None
                insert_query = """
                    INSERT INTO plan_info (
                        id,
                        provider_address,
                        duration,
                        gigabytes,
                        denom,
                        amount,
                        status,
                        status_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s,%s)
                    
                """

                values = (
                    plan['id'],
                    plan['provider_address'],
                    plan['duration'],
                    plan['gigabytes'],
                    price['denom'],
                    amount,
                    plan['status'],
                    status_at
                )

                # Step 5: Insert or update the plan data in the plan_info table
                cursor.execute(insert_query, values)

        except requests.exceptions.RequestException as req_err:
            print(f"HTTP request failed for ID {plan_id}: {req_err}")
        except KeyError as key_err:
            print(f"Missing expected key in JSON for ID {plan_id}: {key_err}")
        except Exception as e:
            print(f"Error processing ID {plan_id}: {e}")
            conn.rollback()

    # Commit all changes to the database
    conn.commit()

except mysql.connector.Error as db_err:
    print(f"Database connection error: {db_err}")

finally:
    # Ensure resources are released
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        print("Database connection closed.")