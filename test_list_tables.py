"""
Test script: List all tables in the 'Su' dataset in 'Power_BI_Learn' workspace
Uses XMLA connector with username/password auth
"""
import sys
import os

# Preload ADOMD
sys.path.append(r"C:\Program Files\Microsoft.NET\ADOMD.NET\160")
import clr
clr.AddReference(r"C:\Program Files\Microsoft.NET\ADOMD.NET\160\Microsoft.AnalysisServices.AdomdClient.dll")

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from powerbi_xmla_connector import PowerBIXmlaConnector

# Credentials from .env
username = os.getenv("PBI_USERNAME")
password = os.getenv("PBI_PASSWORD")

print(f"Username: {username}")
print(f"Workspace: Power_BI_Learn")
print(f"Dataset: Su")
print("=" * 50)

# Create connector
connector = PowerBIXmlaConnector(username=username, password=password)

# Connect
print("\nConnecting...")
success = connector.connect("Power_BI_Learn", "Su")

if success:
    print("✅ Connected successfully!\n")
    
    # List tables
    print("Discovering tables...")
    tables = connector.discover_tables()
    
    if tables:
        print(f"\n📊 Tables in 'Su' dataset ({len(tables)}):")
        print("-" * 40)
        for i, table in enumerate(tables, 1):
            print(f"  {i}. {table['name']}")
            if table.get('description') and table['description'] != 'No description available':
                print(f"     Description: {table['description']}")
            print(f"     Type: {table.get('type', 'TABLE')}")
    else:
        print("❌ No tables found")
    
    # Close
    connector.close()
else:
    print("❌ Connection failed!")
