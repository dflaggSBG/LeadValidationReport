"""Daily Fake Leads by Source Report - Focused monitoring dashboard."""
import streamlit as st
import sys
import subprocess
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import re

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.dashboard.data_loader import get_database_connection

# Page configuration
st.set_page_config(
    page_title="Daily Fake Leads Report",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS - Compact and data-dense
st.markdown("""
<style>
    .main-header {
        font-size: 1.8rem;
        font-weight: 600;
        text-align: center;
        margin-bottom: 1rem;
        color: #d32f2f;
        padding: 0.5rem;
        border-bottom: 2px solid #d32f2f;
    }
    .section-header {
        font-size: 1.3rem;
        font-weight: 600;
        margin: 1rem 0 0.5rem 0;
        color: #1976d2;
        border-bottom: 1px solid #1976d2;
        padding-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.1rem;
        font-weight: 500;
        margin: 0.8rem 0 0.4rem 0;
        color: #424242;
    }
    .alert-critical {
        background-color: #ffebee;
        border-left: 3px solid #f44336;
        padding: 0.5rem 0.8rem;
        margin: 0.5rem 0;
        border-radius: 3px;
        font-size: 0.9rem;
    }
    .alert-warning {
        background-color: #fff8e1;
        border-left: 3px solid #ff9800;
        padding: 0.5rem 0.8rem;
        margin: 0.5rem 0;
        border-radius: 3px;
        font-size: 0.9rem;
    }
    .alert-success {
        background-color: #e8f5e8;
        border-left: 3px solid #4caf50;
        padding: 0.5rem 0.8rem;
        margin: 0.5rem 0;
        border-radius: 3px;
        font-size: 0.9rem;
    }
    .fake-lead-card {
        background-color: #ffebee;
        border: 1px solid #f44336;
        padding: 0.6rem;
        margin: 0.3rem 0;
        border-radius: 4px;
        font-size: 0.85rem;
        line-height: 1.3;
    }
    .source-card {
        background: #fafafa;
        padding: 0.6rem;
        border-radius: 4px;
        border: 1px solid #e0e0e0;
        margin: 0.3rem 0;
        font-size: 0.85rem;
    }
    .metric-container {
        background: white;
        border: 1px solid #e0e0e0;
        border-radius: 4px;
        padding: 0.5rem;
    }
    /* Override Streamlit default font sizes */
    .stMarkdown p {
        font-size: 0.9rem;
        margin-bottom: 0.5rem;
    }
    .stMarkdown h3 {
        font-size: 1.2rem;
        margin-bottom: 0.5rem;
    }
    .stMarkdown h2 {
        font-size: 1.4rem;
        margin-bottom: 0.6rem;
    }
    /* Make dataframes more compact */
    .dataframe {
        font-size: 0.85rem;
    }
    /* Compact expanders */
    .streamlit-expanderHeader {
        font-size: 0.95rem;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_daily_fake_leads(start_date=None, end_date=None):
    """Load fake leads by source for the specified date range."""
    try:
        conn = get_database_connection()
        
        # Build date filter using simpler approach  
        if start_date and end_date:
            if start_date == end_date:
                # Single day
                where_clause = f"AND created_date >= '{start_date} 00:00:00' AND created_date <= '{start_date} 23:59:59'"
            else:
                # Date range
                where_clause = f"AND created_date >= '{start_date} 00:00:00' AND created_date <= '{end_date} 23:59:59'"
        else:
            # Default to today
            where_clause = "AND DATE_TRUNC('day', created_date) = CURRENT_DATE"
        
        # Build query with string concatenation to avoid f-string issues
        query = """
            SELECT 
                COALESCE(lead_source, 'Unknown') as lead_source,
                COUNT(*) as total_leads_today,
                COUNTIF(COALESCE(api_fake_lead, false) = true) as fake_leads_count,
                COUNTIF(COALESCE(api_fraud_score, 0) >= 8) as critical_fraud_count,
                ROUND((COUNTIF(COALESCE(api_fake_lead, false) = true)::DOUBLE / COUNT(*)) * 100, 2) as fake_leads_percentage,
                ROUND((COUNTIF(COALESCE(api_fraud_score, 0) >= 8)::DOUBLE / COUNT(*)) * 100, 2) as critical_fraud_percentage,
                AVG(COALESCE(api_quality_score, quality_score)) as avg_quality_score,
                AVG(COALESCE(api_fraud_score, 0)) as avg_fraud_score,
                CASE 
                    WHEN (COUNTIF(COALESCE(api_fake_lead, false) = true)::DOUBLE / COUNT(*)) * 100 >= 50 THEN 'CRITICAL'
                    WHEN (COUNTIF(COALESCE(api_fake_lead, false) = true)::DOUBLE / COUNT(*)) * 100 >= 20 THEN 'HIGH'
                    WHEN (COUNTIF(COALESCE(api_fake_lead, false) = true)::DOUBLE / COUNT(*)) * 100 >= 10 THEN 'MEDIUM'
                    WHEN COUNTIF(COALESCE(api_fake_lead, false) = true) > 0 THEN 'LOW'
                    ELSE 'CLEAN'
                END as daily_risk_level,
                RANK() OVER (ORDER BY (COUNTIF(COALESCE(api_fake_lead, false) = true)::DOUBLE / COUNT(*)) * 100 DESC) as worst_source_rank,
                CASE WHEN COUNTIF(COALESCE(api_fake_lead, false) = true) >= 3 THEN true ELSE false END as alert_volume,
                CASE WHEN (COUNTIF(COALESCE(api_fake_lead, false) = true)::DOUBLE / COUNT(*)) * 100 >= 25 THEN true ELSE false END as alert_percentage,
                MIN(created_date) as earliest_lead_today,
                MAX(created_date) as latest_lead_today,
                CURRENT_DATE as report_date
            FROM parsed_validations
            WHERE parse_error IS NULL
        """ + where_clause + """
            GROUP BY COALESCE(lead_source, 'Unknown')
            ORDER BY fake_leads_percentage DESC, fake_leads_count DESC
        """
        result = pd.read_sql(query, conn)
        conn.close()
        return result
            
    except Exception as e:
        st.error(f"Error loading daily fake leads: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_daily_fake_leads_detail(start_date=None, end_date=None):
    """Load detailed list of fake and high risk leads for the specified date range."""
    try:
        conn = get_database_connection()
        
        # Build date filter using simpler approach
        if start_date and end_date:
            if start_date == end_date:
                # Single day
                where_clause = f"AND created_date >= '{start_date} 00:00:00' AND created_date <= '{start_date} 23:59:59'"
            else:
                # Date range
                where_clause = f"AND created_date >= '{start_date} 00:00:00' AND created_date <= '{end_date} 23:59:59'"
        else:
            # Default to today
            where_clause = "AND DATE_TRUNC('day', created_date) = CURRENT_DATE"
        
        # Build query with string concatenation to avoid f-string issues
        query = """
        SELECT 
            task_id as lead_id,
            lead_source,
            COALESCE(api_first_name, '') as first_name,
            COALESCE(api_last_name, '') as last_name,
            COALESCE(api_email, lead_email) as email,
            COALESCE(api_phone, '') as phone,
            COALESCE(api_company, lead_company) as company,
            COALESCE(api_fraud_score, 0) as fraud_score,
            COALESCE(api_fraud_factors, 'No specific factors') as fraud_factors,
            COALESCE(api_quality_factors, 'No specific factors') as quality_factors,
            COALESCE(api_recommendation, 'review') as recommendation,
            COALESCE(api_fake_lead, false) as is_fake,
            created_date,
            parsed_at,
            
            -- Categorize the lead type
            CASE 
                WHEN COALESCE(api_fake_lead, false) = true THEN 'FAKE'
                WHEN COALESCE(api_fraud_score, 0) >= 8 THEN 'HIGH_RISK'
                ELSE 'OTHER'
            END as lead_type
            
        FROM parsed_validations
        WHERE parse_error IS NULL
        """ + where_clause + """
        AND (
            COALESCE(api_fake_lead, false) = true 
            OR COALESCE(api_fraud_score, 0) >= 8
        )
        ORDER BY 
            CASE WHEN COALESCE(api_fake_lead, false) = true THEN 1 ELSE 2 END,
            COALESCE(api_fraud_score, 0) DESC, 
            lead_source, 
            created_date DESC
        """
        result = pd.read_sql(query, conn)
        conn.close()
        return result
    except Exception as e:
        st.error(f"Error loading daily fake leads detail: {e}")
        return pd.DataFrame()


def show_daily_summary():
    """Show summary statistics for selected date range."""
    # Get date range from session state
    start_date = st.session_state.get('report_start_date', datetime.now().date())
    end_date = st.session_state.get('report_end_date', datetime.now().date())
    
    # Dynamic section title
    if start_date == end_date:
        section_title = f"📊 Fake Leads Summary - {start_date.strftime('%B %d, %Y')}"
    else:
        section_title = f"📊 Fake Leads Summary - {start_date.strftime('%b %d')} to {end_date.strftime('%b %d, %Y')}"
    
    st.markdown(f'<div class="section-header">{section_title}</div>', unsafe_allow_html=True)
    
    data = load_daily_fake_leads(start_date, end_date)
    
    if data.empty:
        date_desc = "today" if start_date == end_date == datetime.now().date() else "for the selected date range"
        st.success(f"🎉 **No fake leads detected {date_desc}!** All sources are clean.")
        return
    
    # Overall daily metrics
    total_leads_today = data['total_leads_today'].sum()
    total_fake_leads = data['fake_leads_count'].sum()
    total_fake_percentage = (total_fake_leads / total_leads_today * 100) if total_leads_today > 0 else 0
    
    # Get unique count of problematic leads (fake + high risk, no double counting)
    problematic_leads_detail = load_daily_fake_leads_detail(start_date, end_date)
    total_problematic_leads = len(problematic_leads_detail) if not problematic_leads_detail.empty else 0
    fake_count_unique = len(problematic_leads_detail[problematic_leads_detail['lead_type'] == 'FAKE']) if not problematic_leads_detail.empty else 0
    high_risk_count_unique = len(problematic_leads_detail[problematic_leads_detail['lead_type'] == 'HIGH_RISK']) if not problematic_leads_detail.empty else 0
    total_problematic_percentage = (total_problematic_leads / total_leads_today * 100) if total_leads_today > 0 else 0
    
    # Key metrics cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        date_desc = "Today" if start_date == end_date == datetime.now().date() else "Selected Period"
        st.metric(
            label=f"📊 Leads Validated ({date_desc})",
            value=f"{total_leads_today:,}",
            help=f"Leads created in selected date range that went through validation"
        )
    
    with col2:
        st.metric(
            label="🚨 Fake Leads", 
            value=f"{fake_count_unique}",
            help="Number of fake leads detected in selected period"
        )
    
    with col3:
        st.metric(
            label="⚠️ Total Problematic Leads",
            value=f"{total_problematic_leads}",
            help=f"Total unique problematic leads: {fake_count_unique} fake + {high_risk_count_unique} high risk (no double counting)"
        )


def show_fake_leads_by_source_table():
    """Show a clean table of fake leads count by lead source."""
    st.markdown('<div class="section-header">📊 Fake Leads by Lead Source</div>', unsafe_allow_html=True)
    
    # Get date range from session state
    start_date = st.session_state.get('report_start_date', datetime.now().date())
    end_date = st.session_state.get('report_end_date', datetime.now().date())
    
    data = load_daily_fake_leads(start_date, end_date)
    
    if data.empty:
        st.info("No data available for today.")
        return
    
    # Create focused table with explicit column ordering
    # Order: Lead Source | Total Leads | Fake Leads | High Risk | Fake % | High Risk %
    if 'critical_fraud_count' in data.columns and 'critical_fraud_percentage' in data.columns:
        table_columns = [
            'lead_source', 
            'total_leads_today', 
            'fake_leads_count', 
            'critical_fraud_count',
            'fake_leads_percentage',
            'critical_fraud_percentage'
        ]
    else:
        table_columns = [
            'lead_source', 
            'total_leads_today', 
            'fake_leads_count', 
            'fake_leads_percentage'
        ]
    
    table_data = data[table_columns].copy()
    
    # Sort by fake leads count (highest first), then by high risk, then by percentage
    if 'critical_fraud_count' in table_data.columns:
        table_data = table_data.sort_values(['fake_leads_count', 'critical_fraud_count', 'fake_leads_percentage'], ascending=[False, False, False])
    else:
        table_data = table_data.sort_values(['fake_leads_count', 'fake_leads_percentage'], ascending=[False, False])
    
    # Format the data
    table_data['fake_leads_percentage'] = table_data['fake_leads_percentage'].round(1)
    if 'critical_fraud_percentage' in table_data.columns:
        table_data['critical_fraud_percentage'] = table_data['critical_fraud_percentage'].round(1)
    
    # Rename columns for clean display (ensuring correct order)
    if 'critical_fraud_count' in table_data.columns and 'critical_fraud_percentage' in table_data.columns:
        table_data.columns = ['Lead Source', 'Total Leads', 'Fake Leads', 'High Risk', 'Fake %', 'High Risk %']
    else:
        table_data.columns = ['Lead Source', 'Total Leads', 'Fake Leads', 'Fake %']
    
    # Display the table (no special styling needed)
    st.dataframe(
        table_data,
        use_container_width=True,
        hide_index=True,
        height=300
    )
    
    # Quick summary stats below table
    total_sources = len(table_data)
    sources_with_fakes = len(table_data[table_data['Fake Leads'] > 0])
    
    if sources_with_fakes > 0:
        st.info(f"📊 **{sources_with_fakes} out of {total_sources} sources** sent fake leads today")
    else:
        st.success(f"✅ **All {total_sources} sources are clean** today")
    
    # Add direct download functionality here
    col_export = st.columns([1, 1, 1])[1]  # Center the button
    with col_export:
        # Get date range and load problematic leads data directly
        start_date = st.session_state.get('report_start_date', datetime.now().date())
        end_date = st.session_state.get('report_end_date', datetime.now().date())
        
        # Load the problematic leads data for export
        export_leads = load_daily_fake_leads_detail(start_date, end_date)
        
        if not export_leads.empty:
            # Generate the export data with parsed factors
            def parse_factors(factors_text):
                """Parse fraud/quality factors into individual metrics."""
                import re
                factors = {}
                if pd.isna(factors_text) or factors_text in ['No specific factors', 'No factors']:
                    return factors
                
                text = str(factors_text)
                
                # Common fraud factor patterns
                if 'Known fake lead from database' in text:
                    factors['Known Fake Lead'] = 'TRUE'
                if 'ML fraud model' in text:
                    ml_match = re.search(r'ML fraud model.*?score[:\s]*(\d+)', text)
                    if ml_match:
                        factors['ML Fraud Score'] = ml_match.group(1)
                    risk_match = re.search(r'ML fraud model[:\s]*(MEDIUM|HIGH|LOW|NORMAL) RISK', text)
                    if risk_match:
                        factors['ML Risk Level'] = risk_match.group(1)
                
                # Email factors
                if 'Email not deliverable' in text:
                    factors['Email Deliverable'] = 'NO'
                if 'Email deliverable' in text and 'not' not in text:
                    factors['Email Deliverable'] = 'YES'
                if 'Valid email format' in text:
                    factors['Email Format Valid'] = 'YES'
                if 'Invalid email' in text:
                    factors['Email Format Valid'] = 'NO'
                
                # Phone factors  
                if 'Valid phone number' in text:
                    factors['Phone Valid'] = 'YES'
                if 'Invalid phone' in text:
                    factors['Phone Valid'] = 'NO'
                
                # Geographic factors
                if 'ZIP/state consistent' in text:
                    factors['ZIP State Consistent'] = 'YES'
                if 'Valid postal code' in text:
                    factors['Postal Code Valid'] = 'YES'
                if 'Valid state' in text:
                    factors['State Valid'] = 'YES'
                
                # Data completeness
                completeness_match = re.search(r'Data completeness \((\d+)/(\d+)\)', text)
                if completeness_match:
                    factors['Data Completeness'] = f"{completeness_match.group(1)}/{completeness_match.group(2)}"
                
                return factors
            
            # Create export table
            export_table = export_leads.copy()
            export_table['name'] = export_table['first_name'] + ' ' + export_table['last_name']
            export_table['phone_display'] = export_table['phone'].apply(lambda x: x if x else 'Missing')
            export_table['company_display'] = export_table['company'].apply(lambda x: x if x else 'Missing')
            
            # Parse factors for each lead
            all_factors = []
            for _, row in export_table.iterrows():
                fraud_parsed = parse_factors(row['fraud_factors'])
                quality_parsed = parse_factors(row['quality_factors'])
                combined_factors = {**fraud_parsed, **quality_parsed}
                all_factors.append(combined_factors)
            
            # Get all unique factor names
            all_factor_names = set()
            for factors in all_factors:
                all_factor_names.update(factors.keys())
            
            # Create final export table
            final_export = export_table[[
                'lead_id', 'lead_type', 'name', 'email', 'phone_display', 
                'company_display', 'lead_source', 'fraud_score', 'recommendation'
            ]].copy()
            
            # Add factor columns
            for factor_name in sorted(all_factor_names):
                final_export[factor_name] = ''
            
            # Populate factor columns
            for i, factors in enumerate(all_factors):
                for factor_name, factor_value in factors.items():
                    final_export.loc[i, factor_name] = factor_value
            
            # Add original text columns
            final_export['Original Fraud Factors'] = export_table['fraud_factors']
            final_export['Original Quality Issues'] = export_table['quality_factors']
            
            # Rename columns
            base_columns = [
                'Lead ID', 'Type', 'Name', 'Email', 'Phone', 
                'Company', 'Source', 'Fraud Score', 'Action'
            ]
            final_export.columns = base_columns + list(sorted(all_factor_names)) + ['Original Fraud Factors', 'Original Quality Issues']
            
            csv = final_export.to_csv(index=False)
            
            st.download_button(
                label="📥 Download All Data",
                data=csv,
                file_name=f"problematic_leads_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                help="Download complete problematic leads data with parsed factors",
                type="secondary",
                use_container_width=True
            )
        else:
            st.info("📊 No problematic leads found for selected date range to export")


def get_last_refresh_time():
    """Get the timestamp of the last ETL run."""
    try:
        conn = get_database_connection()
        query = """
        SELECT MAX(parsed_at) as last_refresh
        FROM parsed_validations
        WHERE parse_error IS NULL
        """
        result = conn.execute(query).fetchone()
        conn.close()
        
        if result and result[0]:
            last_refresh = pd.to_datetime(result[0])
            return last_refresh.strftime("%I:%M %p on %B %d, %Y")
        else:
            return "Unknown"
    except:
        return "Unknown"

def run_etl_pipeline():
    """Run the ETL pipeline to refresh data."""
    try:
        with st.spinner("🔄 Running ETL pipeline to refresh data..."):
            # Run the ETL process
            result = subprocess.run(
                [sys.executable, "lead_validation_etl.py", "--validation-only"],
                cwd=project_root,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                # Force update the refresh timestamp even if no new data was processed
                try:
                    conn = get_database_connection()
                    conn.execute("""
                        UPDATE parsed_validations 
                        SET parsed_at = CURRENT_TIMESTAMP 
                        WHERE task_id IN (
                            SELECT task_id FROM parsed_validations 
                            WHERE parse_error IS NULL 
                            ORDER BY parsed_at DESC 
                            LIMIT 1
                        )
                    """)
                    conn.close()
                except Exception as e:
                    st.warning(f"Could not update timestamp: {e}")
                
                st.success("✅ ETL pipeline completed successfully! Data refreshed.")
                st.cache_data.clear()
                return True
            else:
                st.error(f"❌ ETL pipeline failed: {result.stderr}")
                return False
                
    except subprocess.TimeoutExpired:
        st.error("⏰ ETL pipeline timed out after 5 minutes")
        return False
    except Exception as e:
        st.error(f"❌ Error running ETL pipeline: {e}")
        return False

def main():
    """Main fake leads report with date range selection."""
    current_date = datetime.now().strftime("%A, %B %d, %Y")
    last_refresh = get_last_refresh_time()
    
    # Date range selector
    col_date1, col_date2, col_refresh = st.columns([1, 1, 1])
    
    with col_date1:
        start_date = st.date_input(
            "Start Date",
            value=datetime.now().date(),
            help="Select the start date for the report"
        )
    
    with col_date2:
        end_date = st.date_input(
            "End Date", 
            value=datetime.now().date(),
            help="Select the end date for the report"
        )
    
    with col_refresh:
        st.markdown("<br>", unsafe_allow_html=True)  # Align with date inputs
        if st.button("🔄 Refresh Data", help="Run ETL pipeline to sync latest data from Salesforce"):
            if run_etl_pipeline():
                st.rerun()
            else:
                st.error("Failed to refresh data. Please check ETL configuration.")
    
    # Determine report title based on date range
    if start_date == end_date:
        if start_date == datetime.now().date():
            report_title = "🚨 Daily Fake Leads Report (Today)"
            report_subtitle = current_date
        else:
            report_title = "🚨 Fake Leads Report"
            report_subtitle = start_date.strftime("%A, %B %d, %Y")
    else:
        report_title = "🚨 Fake Leads Report"
        report_subtitle = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
    
    st.markdown(f'''
    <div class="main-header">
        {report_title}<br>
        <small>{report_subtitle}</small><br>
        <small style="color: #666; font-size: 0.8rem;">Last Updated: {last_refresh}</small>
    </div>
    ''', unsafe_allow_html=True)
    
    # Store date range in session state for functions to use
    st.session_state['report_start_date'] = start_date
    st.session_state['report_end_date'] = end_date
    
    # Main report sections
    show_daily_summary()
    st.markdown("---")
    
    show_fake_leads_by_source_table()
    


if __name__ == "__main__":
    main()
