#!/usr/bin/env python3
"""
Run script for the Daily Fake Leads Report.

This script launches a focused daily monitoring dashboard that shows
fake leads per lead source for the current day.

Usage:
    python run_daily_fake_leads_report.py
    
    Or with custom port:
    python run_daily_fake_leads_report.py --port 8503
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime

def main():
    """Run the daily fake leads report."""
    dashboard_path = Path(__file__).parent / "src" / "dashboard" / "daily_fake_leads_report.py"
    
    if not dashboard_path.exists():
        print(f"❌ Report file not found: {dashboard_path}")
        return 1
    
    # Parse command line arguments for port
    port = "8503"
    if len(sys.argv) > 1:
        if sys.argv[1] == "--port" and len(sys.argv) > 2:
            port = sys.argv[2]
        elif sys.argv[1].startswith("--port="):
            port = sys.argv[1].split("=")[1]
    
    current_date = datetime.now().strftime("%A, %B %d, %Y")
    
    print("🚨 Starting Fake Leads Report Dashboard...")
    print(f"📅 Current Date: {current_date}")
    print(f"📊 Dashboard will be available at: http://localhost:{port}")
    print("\n📋 This flexible report features:")
    print("   • 📅 Date range selection (single day or multi-day periods)")
    print("   • 📊 Fake & high risk leads summary with last refresh timestamp")
    print("   • 📊 Leads by source table (data-dense view)")
    print("   • 🔍 Individual lead analysis with validation details")
    print("   • ⏰ Hourly pattern analysis by source")
    print("   • 🔄 ETL pipeline refresh capability")
    print("   • 📄 PDF & HTML export options\n")
    
    try:
        # Run streamlit with the daily report
        cmd = [
            sys.executable, "-m", "streamlit", "run", str(dashboard_path),
            "--server.port", port,
            "--server.headless", "false", 
            "--browser.gatherUsageStats", "false"
        ]
        
        subprocess.run(cmd, check=True)
        
    except KeyboardInterrupt:
        print("\n👋 Daily report stopped by user.")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running report: {e}")
        return 1
    except FileNotFoundError:
        print("❌ Streamlit not found. Make sure you've installed the requirements:")
        print("   pip install -r requirements.txt")
        return 1

if __name__ == "__main__":
    sys.exit(main())
