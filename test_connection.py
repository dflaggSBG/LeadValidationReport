#!/usr/bin/env python3
"""
Quick test script to verify Salesforce connection with new configuration
"""
import os
from dotenv import load_dotenv
from lead_validation_etl import LeadValidationETL

def main():
    # Load environment variables
    load_dotenv()
    
    print("🔧 Testing Salesforce Connection...")
    print(f"Token URL: {os.getenv('SF_TOKEN_URL')}")
    print(f"Instance URL: {os.getenv('SF_INSTANCE_URL')}")
    print(f"Username: {os.getenv('SF_USERNAME')}")
    print()
    
    # Test authentication
    etl = LeadValidationETL()
    
    if etl.authenticate_salesforce():
        print("✅ SUCCESS: Salesforce authentication working!")
        print("🎯 Ready to run full ETL pipeline!")
        return True
    else:
        print("❌ FAILED: Check your credentials in .env file")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
