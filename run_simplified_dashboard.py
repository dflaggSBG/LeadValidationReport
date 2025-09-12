#!/usr/bin/env python3
"""
Run script for the Simplified Lead Validation Dashboard.

This script launches the streamlined version of the lead validation dashboard
focused on Data Quality Scores and Fraud Scores as requested.

Usage:
    python run_simplified_dashboard.py
    
    Or with custom port:
    python run_simplified_dashboard.py --port 8502
"""

import sys
import subprocess
from pathlib import Path

def main():
    """Run the simplified dashboard."""
    dashboard_path = Path(__file__).parent / "src" / "dashboard" / "simplified_dashboard.py"
    
    if not dashboard_path.exists():
        print(f"❌ Dashboard file not found: {dashboard_path}")
        return 1
    
    # Parse command line arguments for port
    port = "8501"
    if len(sys.argv) > 1:
        if sys.argv[1] == "--port" and len(sys.argv) > 2:
            port = sys.argv[2]
        elif sys.argv[1].startswith("--port="):
            port = sys.argv[1].split("=")[1]
    
    print("🚀 Starting Simplified Lead Validation Dashboard...")
    print(f"📊 Dashboard will be available at: http://localhost:{port}")
    print("\n📋 This dashboard shows:")
    print("   • Overall Validation Results (Total, Avg/Median Quality & Fraud Scores)")
    print("   • Validation Results by Lead Source (with fake leads analysis)")
    print("   • Trend Reports (Quality & Fraud scores over time)")
    print("   • Trend Reports by Lead Source\n")
    
    try:
        # Run streamlit with the simplified dashboard
        cmd = [
            sys.executable, "-m", "streamlit", "run", str(dashboard_path),
            "--server.port", port,
            "--server.headless", "false",
            "--browser.gatherUsageStats", "false"
        ]
        
        subprocess.run(cmd, check=True)
        
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user.")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running dashboard: {e}")
        return 1
    except FileNotFoundError:
        print("❌ Streamlit not found. Make sure you've installed the requirements:")
        print("   pip install -r requirements.txt")
        return 1

if __name__ == "__main__":
    sys.exit(main())
