import mysql.connector
import os
import re
from dotenv import load_dotenv
from google import genai
from google.genai import types

# Load environment variables from .env file
load_dotenv()

# Read variables from .env
API_KEY = os.getenv("Gemini_api_key")
SQL_HOST = os.getenv("SQL_host")
SQL_USER = os.getenv("SQL_user")
SQL_PASSWORD = os.getenv("SQL_password")

# Initialize Gemini API
client = genai.Client(api_key=API_KEY)

# Maintain conversation history
conversation_history = []

def clean_sql_query(sql_query):
    """Remove markdown formatting (```sql ... ```) from the query."""
    sql_query = re.sub(r"```sql\s*", "", sql_query, flags=re.IGNORECASE)  # Remove opening ```sql
    sql_query = re.sub(r"```", "", sql_query)  # Remove closing ```
    return sql_query.strip()  # Trim whitespace

def get_sql_query(user_prompt, database):
    """Use Gemini to convert natural language into an SQL query with conversation history."""
    system_instruction = f"""
    You are an AI SQL assistant that converts natural language into valid MySQL queries for the '{database}' database.
    Maintain conversation history so that each query is generated in the context of past queries.
    Do not provide explanations, only return the SQL query.
    """

    # Add user input to conversation history
    conversation_history.append(f"User: {user_prompt}")

    # Keep only the last 5 exchanges for context
    context = "\n".join(conversation_history[-5:])

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=[context],  # Pass conversation history
        config=types.GenerateContentConfig(
            system_instruction=system_instruction,
            max_output_tokens=100,
            temperature=0.2
        )
    )

    sql_query = response.text.strip()
    sql_query = clean_sql_query(sql_query)

    # Store Gemini's response in conversation history
    conversation_history.append(f"AI: {sql_query}")

    return sql_query

# Establish MySQL connection
conn = mysql.connector.connect(
    host=SQL_HOST,
    user=SQL_USER,
    password=SQL_PASSWORD
)
cursor = conn.cursor()

# List all databases
cursor.execute("SHOW DATABASES")
databases = [db[0] for db in cursor.fetchall()]

# Display databases to the user, including an option to create a new one
print("\nAvailable Databases:")
print("0. Create a new database")
for idx, db in enumerate(databases, 1):
    print(f"{idx}. {db}")

# Ask the user to select a database or create a new one
while True:
    try:
        choice = int(input("\nSelect a database (enter number, or 0 to create a new one): "))
        
        if choice == 0:
            new_db_name = input("Enter the name for the new database: ").strip()
            
            # Check if database already exists
            if new_db_name in databases:
                print(f"⚠️ Database '{new_db_name}' already exists. Please select it from the list.")
            else:
                cursor.execute(f"CREATE DATABASE {new_db_name}")
                print(f"✅ Database '{new_db_name}' created successfully!")
                selected_db = new_db_name
                break  # Exit loop since new database is created
            
        elif 1 <= choice <= len(databases):
            selected_db = databases[choice - 1]
            break
        
        else:
            print("Invalid choice, please select a valid number.")
    
    except ValueError:
        print("Please enter a valid number.")

# Use the selected database
cursor.execute(f"USE {selected_db}")

# List all tables in the selected database
cursor.execute("SHOW TABLES")
tables = [tbl[0] for tbl in cursor.fetchall()]

print(f"\nTables in '{selected_db}':")
if tables:
    for tbl in tables:
        print(f"- {tbl}")
else:
    print("No tables found in this database.")

# Initialize conversation with table context
conversation_history.append(f"User selected database: {selected_db}")
if tables:
    conversation_history.append(f"Tables in database: {', '.join(tables)}")

# Continuous Query Loop
while True:
    user_prompt = input("\nEnter your query in natural language (or type 'exit' to quit): ").strip()

    if user_prompt.lower() == "exit":
        print("Exiting the program. Goodbye!")
        break

    # Convert to SQL using Gemini with history
    sql_query = get_sql_query(user_prompt, selected_db)
    print(f"\nGenerated SQL Query:\n{sql_query}\n")

    # Check if the query is a DELETE operation and ask for confirmation
    if sql_query.lower().startswith("delete"):
        confirmation = input("⚠️ Are you sure you want to delete? (yes/no): ").strip().lower()
        if confirmation != "yes":
            print("❌ Deletion canceled.")
            continue  # Skip execution if user says "no"

    try:
        cursor.execute(sql_query)

        # Fetch results only for SELECT, SHOW, DESCRIBE queries
        if sql_query.lower().startswith(("select", "show", "describe")):
            results = cursor.fetchall()
            if results:
                for row in results:
                    print(row)
            else:
                print("No results found.")
        else:
            conn.commit()
            print("✅ Query executed successfully.")

        # Ensure the result set is fully read to avoid "Unread result found" error
        while cursor.nextset():
            pass

    except mysql.connector.Error as err:
        print(f"Error: {err}")

        # Ensure any unread results are cleared before executing the next query
        try:
            cursor.fetchall()
        except mysql.connector.Error:
            pass

# Close the connection
cursor.close()
conn.close()
