# Lead Validation Reporting System

A comprehensive Python analytics system for validating lead quality, compliance, and completeness. Features Salesforce integration, automated validation rules, and interactive reporting dashboards.

## ⚡ Quick Start (5 minutes)

**Get the dashboard running immediately:**

```bash
# 1. Clone and navigate to the project
cd LeadValidationReport

# 2. Setup virtual environment
python -m venv venv
venv\Scripts\activate    # Windows
# source venv/bin/activate  # macOS/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the dashboard
streamlit run src/dashboard/validation_dashboard.py
```

The dashboard will automatically open at **http://localhost:8501** 🎉

> **Note**: For first-time usage, you may need sample data. Run `python lead_validation_etl.py` if the dashboard shows no data.

## 🚀 Detailed Setup

### Prerequisites

- Python 3.8+ (recommended: Python 3.10-3.12)
- Salesforce API access (for lead data extraction)
- DuckDB (automatically managed)
- pip (Python package manager)
- Virtual environment support (venv module)
- Internet connection for package installation

### Installation

1. **Navigate to the project directory:**
   ```bash
   cd LeadValidationReport
   ```

2. **Create and activate a virtual environment (strongly recommended):**
   ```bash
   # Windows
   python -m venv venv
   venv\Scripts\activate
   
   # You should see (venv) prefix in your terminal prompt after activation

   # macOS/Linux
   python -m venv venv
   source venv/bin/activate
   ```
   
   > **⚠️ Important**: Always activate your virtual environment before running the application. If you see `streamlit: command not found` or similar errors, check that your virtual environment is activated.

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure your environment:**
   ```bash
   # Copy the environment template
   copy env.example .env
   # Edit .env with your specific settings
   ```

### Running the System

#### 1. Extract and Validate Leads (First Time Setup)
```bash
# Extract leads and run validation analysis
python lead_validation_etl.py

# Force refresh all data
python lead_validation_etl.py --force-refresh

# Run validation only (skip extraction)
python lead_validation_etl.py --validation-only
```

#### 2. Run Lead Validation Dashboard

**🎯 Simplified Dashboard (Recommended):**
```bash
# Ensure virtual environment is activated first
venv\Scripts\activate  # Windows
# source venv/bin/activate  # macOS/Linux

# Run the simplified dashboard (focused reporting)
python run_simplified_dashboard.py

# Or with custom port
python run_simplified_dashboard.py --port 8502
```

**🚨 Daily Fake Leads Report (New!):**
```bash
# Run the daily monitoring report (today's fake leads by source)
python run_daily_fake_leads_report.py

# Or with custom port
python run_daily_fake_leads_report.py --port 8503
```

**📊 Full Dashboard (Advanced):**
```bash
# From project root directory
streamlit run src/dashboard/validation_dashboard.py

# Or specify port if 8501 is in use
streamlit run src/dashboard/validation_dashboard.py --server.port 8550
```

**Dashboard Access:**
- Local URL: http://localhost:8501
- Network URL: http://[your-ip]:8501
- The dashboard will automatically open in your default web browser

**Simplified Dashboard Features:**
- ✅ Overall Validation Results (Total, Avg/Median Data Quality & Fraud Scores)
- 🎯 Validation Results by Lead Source (with fake leads analysis) 
- 📈 Trend Reports (Quality & Fraud scores over time)
- 📊 Trend Reports by Lead Source
- 🚨 Detailed fake leads analysis with validation point results
- 📅 Lead creation date segmentation across all sections

**Daily Fake Leads Report Features:**
- 🚨 Real-time monitoring of today's fake leads by source
- ⚠️ Risk level alerts (Critical/High/Medium/Low) per source
- 📊 Immediate action recommendations (Pause/Investigate/Monitor)
- 🔍 Detailed analysis of each fake lead detected today
- ⏰ Hourly fraud pattern analysis
- 📈 Source performance ranking for current day
- 🎯 Focused daily operations dashboard

#### 3. Generate Lead Validation Reports
```bash
# Generate comprehensive validation report
python generate_reports.py --report-type comprehensive

# Generate compliance report only
python generate_reports.py --report-type compliance
```

## 📁 Project Structure

```
LeadValidationReport/
├── src/
│   ├── dashboard/                    # Streamlit Dashboard
│   │   ├── validation_dashboard.py  # Main dashboard entry point  
│   │   ├── simplified_dashboard.py  # 🎯 Simplified focused dashboard
│   │   ├── daily_fake_leads_report.py # 🚨 Daily fake leads monitoring
│   │   ├── components.py            # UI components & charts
│   │   ├── data_loader.py           # Data loading from DuckDB
│   │   └── styles.py                # CSS styling
│   ├── validation/                  # Lead validation logic
│   │   ├── rules.py                 # Validation rules engine
│   │   └── fraud_detection.py       # 🚨 Fraud detection & scoring
│   └── utils/                       # Utility functions
│       └── validation_parser.py     # Validation parsing utilities
├── sql/
│   └── views/                       # Analytics SQL views
│       ├── lead_source_quality_summary.sql
│       ├── lead_validation_overview.sql
│       ├── validation_by_source.sql
│       ├── validation_metrics.sql
│       ├── validation_summary.sql
│       ├── validation_trends.sql
│       ├── worst_lead_sources.sql
│       ├── simplified_overall_results.sql      # 📊 Simplified overall metrics
│       ├── simplified_results_by_source.sql    # 🎯 Source-focused results
│       ├── fake_leads_detail.sql               # 🚨 Detailed fake leads analysis
│       ├── simplified_trends_overall.sql       # 📈 Overall trends reporting
│       ├── simplified_trends_by_source.sql     # 📊 Source trends reporting
│       └── daily_fake_leads_by_source.sql      # 🚨 Daily fake leads monitoring
├── config/
│   └── settings.py                  # Configuration settings
├── data/
│   └── leads.duckdb                 # Lead validation database
├── reports/                         # Generated validation reports
├── logs/                            # Application logs
├── config/
│   └── settings.py                  # Configuration settings
├── lead_validation_etl.py           # Main ETL pipeline
├── generate_reports.py              # Report generation script
├── run_simplified_dashboard.py      # 🎯 Run simplified dashboard
├── run_daily_fake_leads_report.py   # 🚨 Run daily fake leads report
├── reset_database.py                # Database reset utility
├── test_connection.py               # Connection testing utility
├── setup.py                         # Project setup script
├── requirements.txt                 # Python dependencies
├── env.example                      # Environment template
└── README.md                        # This file
```

## 🎯 Key Features

### 🎯 Simplified Reporting (New!)

**Streamlined dashboard focused on the metrics that matter most:**

- **Overall Validation Results**
  - Total Validations count
  - Average & Median Data Quality Score (0-1 scale)
  - Average & Median Fraud Score (0-1 scale)  
  - System health status and alerts

- **Validation Results by Lead Source**
  - Lead source performance comparison
  - Data Quality and Fraud scores by source  
  - Fake leads identification and analysis
  - Detailed validation results for each suspicious lead
  - Risk level assessment per source

- **Trend Analysis**
  - Overall Data Quality Score trends over time
  - Overall Fraud Score trends over time
  - Individual validation point trends (Email, Phone, Name, Company, Completeness)
  - Source-specific trend analysis and comparisons

- **Detailed Fake Leads Analysis**
  - Complete list of high-risk leads with fraud scores ≥ 0.6
  - Validation point results for each suspicious lead
  - Recommended actions (Reject, Quarantine, Flag, Monitor)
  - Source-specific fraud patterns

### 📊 Lead Quality Analytics (Advanced Dashboard)
- **Completeness Scoring**: Measure data completeness across lead fields
- **Quality Metrics**: Identify missing, invalid, or inconsistent data
- **Validation Rules**: Custom business rules for lead quality assessment
- **Trend Analysis**: Track lead quality improvements over time

### ✅ Compliance Monitoring
- **TCPA Compliance**: Verify consent and opt-in status
- **Data Privacy**: Check for PII handling compliance
- **Required Fields**: Ensure mandatory fields are populated
- **Format Validation**: Validate phone numbers, emails, addresses

### 🔄 Automated Data Pipeline
- **Salesforce Integration**: Extract leads and related data
- **Incremental Processing**: Only process new/changed leads
- **Validation Engine**: Run comprehensive validation rules
- **Backup Management**: Automatic data backups

### 📈 Interactive Dashboard
- **Executive Summary**: Key validation metrics and trends
- **Detailed Reports**: Drill-down capabilities for specific issues
- **Compliance Dashboard**: Monitor compliance status across all leads
- **Quality Heatmaps**: Visual representation of data quality

## 🔧 Configuration

### Environment Variables

Create a `.env` file with the following variables:

```bash
# Salesforce API Configuration
SF_CLIENT_ID=your_salesforce_client_id
SF_CLIENT_SECRET=your_salesforce_client_secret  
SF_USERNAME=your_salesforce_username
SF_PASSWORD=your_salesforce_password
SF_SECURITY_TOKEN=your_salesforce_security_token

# Database Configuration
DUCKDB_PATH=./data/leads.duckdb

# Dashboard Configuration
DASHBOARD_HOST=localhost
DASHBOARD_PORT=8501
DASHBOARD_TITLE=Lead Validation Dashboard

# Validation Configuration
VALIDATION_RULES_STRICT=true
COMPLIANCE_CHECKS_ENABLED=true
DATA_RETENTION_DAYS=90
```

## 📊 Validation Metrics

### Core Quality Metrics
- **Lead Completeness Score**: Percentage of required fields populated
- **Data Quality Index**: Overall data quality assessment
- **Compliance Score**: Compliance with business rules and regulations
- **Enrichment Coverage**: Percentage of leads with enriched data

### Business Rules
- Phone number format validation
- Email address validation
- Required field completeness
- TCPA consent verification
- Duplicate detection and scoring
- Geographic validation

## 🚀 Advanced Features

### Custom Validation Rules
- Define business-specific validation logic
- Configure scoring weights for different validation criteria  
- Set up automated alerts for compliance violations
- Create custom reports for specific validation needs

### Data Enrichment
- Integrate with external data providers
- Append missing demographic and firmographic data
- Validate and standardize address information
- Phone number validation and formatting

## 🔧 Troubleshooting

### Common Issues and Solutions

#### `streamlit: command not found` or `streamlit is not recognized`
**Cause**: Virtual environment is not activated or Streamlit is not installed.

**Solutions**:
1. Activate your virtual environment:
   ```bash
   # Windows
   venv\Scripts\activate
   
   # macOS/Linux
   source venv/bin/activate
   ```
2. Verify Streamlit is installed:
   ```bash
   pip list | grep streamlit
   ```
3. If not installed, reinstall dependencies:
   ```bash
   pip install -r requirements.txt
   ```

#### `ImportError` or `ModuleNotFoundError`
**Cause**: Missing dependencies or incorrect Python environment.

**Solutions**:
1. Ensure virtual environment is activated
2. Reinstall all dependencies:
   ```bash
   pip install -r requirements.txt --upgrade
   ```
3. Check Python version compatibility:
   ```bash
   python --version  # Should be 3.8+
   ```

#### Dashboard fails to load or shows errors
**Cause**: Database not initialized or configuration issues.

**Solutions**:
1. Run the ETL pipeline first:
   ```bash
   python lead_validation_etl.py
   ```
2. Check if database file exists:
   ```bash
   ls data/leads.duckdb  # Should exist after ETL run
   ```
3. Reset database if corrupted:
   ```bash
   python reset_database.py
   ```

#### Port already in use (8501)
**Cause**: Another Streamlit app or service is using the default port.

**Solutions**:
1. Use a different port:
   ```bash
   streamlit run src/dashboard/validation_dashboard.py --server.port 8502
   ```
2. Kill existing process:
   ```bash
   # Windows
   netstat -ano | findstr :8501
   taskkill /PID <process_id> /F
   
   # macOS/Linux
   lsof -ti:8501 | xargs kill -9
   ```

#### Slow performance or memory issues
**Solutions**:
1. Limit data processing in dashboard settings
2. Ensure sufficient system memory (recommend 4GB+)
3. Close other resource-intensive applications

### Getting Help

- **Check logs**: Look in the `logs/` directory for detailed error messages
- **Test connection**: Run `python test_connection.py` to verify database connectivity
- **Verify setup**: Run `python setup.py check` to validate installation

## 📋 Version Information

### Current Version: 1.0.0

### Recent Updates:
- ✅ Enhanced setup instructions with virtual environment troubleshooting
- ✅ Added comprehensive troubleshooting section
- ✅ Updated project structure documentation
- ✅ Improved dashboard access information
- ✅ Added quick start guide for immediate usage
- ✅ Updated prerequisites with version recommendations

### System Requirements:
- **Python**: 3.8+ (tested with 3.10-3.12)
- **Memory**: 4GB RAM minimum, 8GB recommended
- **Storage**: 500MB for application + data storage space
- **OS**: Windows 10+, macOS 10.14+, Linux (Ubuntu 18.04+)

---

**🎯 Ensure Lead Quality and Compliance with Automated Validation! 📊✅**
