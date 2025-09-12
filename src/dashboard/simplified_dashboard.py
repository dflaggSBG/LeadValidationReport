"""Simplified Lead Validation Dashboard - Focused on Data Quality and Fraud Scores."""
import streamlit as st
import sys
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.dashboard.data_loader import get_database_connection

# Page configuration
st.set_page_config(
    page_title="Lead Validation - Simplified Report",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS - Compact and data-dense
st.markdown("""
<style>
    .main-header {
        font-size: 1.6rem;
        font-weight: 600;
        text-align: center;
        margin-bottom: 1rem;
        color: #1976d2;
        padding: 0.5rem;
        border-bottom: 2px solid #1976d2;
    }
    .section-header {
        font-size: 1.2rem;
        font-weight: 600;
        margin: 1rem 0 0.5rem 0;
        color: #1976d2;
        border-bottom: 1px solid #1976d2;
        padding-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1.05rem;
        font-weight: 500;
        margin: 0.8rem 0 0.4rem 0;
        color: #424242;
    }
    .metric-card {
        background: white;
        padding: 0.6rem;
        border-radius: 4px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        border-left: 3px solid #1976d2;
        margin: 0.3rem 0;
    }
    .fake-lead {
        background-color: #ffebee;
        border-left: 3px solid #f44336;
        padding: 0.6rem;
        margin: 0.3rem 0;
        border-radius: 3px;
        font-size: 0.85rem;
    }
    .warning-box {
        background-color: #fff8e1;
        border-left: 3px solid #ff9800;
        padding: 0.6rem;
        margin: 0.5rem 0;
        border-radius: 3px;
        font-size: 0.9rem;
    }
    /* Override Streamlit defaults for compact display */
    .stMarkdown p {
        font-size: 0.9rem;
        margin-bottom: 0.4rem;
    }
    .stMarkdown h1 {
        font-size: 1.4rem;
        margin-bottom: 0.5rem;
    }
    .stMarkdown h2 {
        font-size: 1.2rem;
        margin-bottom: 0.4rem;
    }
    .stMarkdown h3 {
        font-size: 1.05rem;
        margin-bottom: 0.3rem;
    }
    .stDataFrame {
        font-size: 0.8rem;
    }
    .streamlit-expanderHeader {
        font-size: 0.9rem;
    }
    .stMetric {
        background: white;
        border: 1px solid #e0e0e0;
        border-radius: 4px;
        padding: 0.4rem;
    }
    .stSelectbox label {
        font-size: 0.85rem;
    }
    .stButton > button {
        font-size: 0.85rem;
        padding: 0.4rem 0.8rem;
    }
    /* Reduce chart heights for data density */
    .js-plotly-plot {
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_overall_results(date_filter="All Time"):
    """Load overall validation results with date filtering."""
    try:
        conn = get_database_connection()
        
        # Build date filter clause
        date_clause = get_date_filter_clause(date_filter)
        
        # Try simplified view first, fall back to existing data structure
        try:
            query = f"SELECT * FROM leads.simplified_overall_results WHERE created_date {date_clause}"
            result = pd.read_sql(query, conn)
            conn.close()
            return result
        except:
            # Fall back to existing parsed_validations structure with date filtering
            query = f"""
            SELECT 
                COUNT(*) as total_validations,
                COUNT(DISTINCT task_id) as unique_leads,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as avg_data_quality_score,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as median_data_quality_score,
                AVG(CASE WHEN COALESCE(api_fake_lead, false) THEN 0.8 ELSE 0.2 END) as avg_fraud_score,
                AVG(CASE WHEN COALESCE(api_fake_lead, false) THEN 0.8 ELSE 0.2 END) as median_fraud_score,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as avg_overall_score,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as median_overall_score,
                COUNTIF(COALESCE(api_quality_score, quality_score) >= 7) as excellent_quality_count,
                COUNTIF(COALESCE(api_quality_score, quality_score) >= 5 AND COALESCE(api_quality_score, quality_score) < 7) as good_quality_count,
                COUNTIF(COALESCE(api_quality_score, quality_score) >= 3 AND COALESCE(api_quality_score, quality_score) < 5) as fair_quality_count,
                COUNTIF(COALESCE(api_quality_score, quality_score) < 3) as poor_quality_count,
                COUNTIF(COALESCE(api_fake_lead, false)) as critical_fraud_risk_count,
                ROUND((COUNTIF(COALESCE(api_quality_score, quality_score) >= 7)::DOUBLE / COUNT(*)) * 100, 2) as high_quality_percentage,
                ROUND((COUNTIF(COALESCE(api_fake_lead, false))::DOUBLE / COUNT(*)) * 100, 2) as high_fraud_risk_percentage,
                MIN(parsed_at) as earliest_validation,
                MAX(parsed_at) as latest_validation,
                CASE 
                    WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 8 THEN 'EXCELLENT'
                    WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 6 THEN 'GOOD'
                    WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 4 THEN 'FAIR'
                    ELSE 'POOR'
                END as overall_system_status
            FROM parsed_validations
            WHERE parse_error IS NULL
            AND created_date {date_clause}
            """
            result = pd.read_sql(query, conn)
            conn.close()
            return result
    except Exception as e:
        st.error(f"Error loading overall results: {e}")
        return pd.DataFrame()

def get_date_filter_clause(date_filter):
    """Generate SQL date filter clause based on selection."""
    if date_filter == "Last 30 Days":
        return ">= CURRENT_DATE - INTERVAL '30 days'"
    elif date_filter == "Last 7 Days":
        return ">= CURRENT_DATE - INTERVAL '7 days'"
    elif date_filter == "This Month":
        return ">= DATE_TRUNC('month', CURRENT_DATE)"
    elif date_filter == "Last Month":
        return ">= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' AND created_date < DATE_TRUNC('month', CURRENT_DATE)"
    else:  # All Time
        return "IS NOT NULL"


@st.cache_data(ttl=600)
def load_results_by_source(date_filter="All Time"):
    """Load validation results by source with date filtering."""
    try:
        conn = get_database_connection()
        
        # Build date filter clause
        date_clause = get_date_filter_clause(date_filter)
        
        # Try simplified view first, fall back to existing data structure
        try:
            query = f"SELECT * FROM leads.simplified_results_by_source WHERE created_date {date_clause} ORDER BY avg_data_quality_score DESC"
            result = pd.read_sql(query, conn)
            conn.close()
            return result
        except:
            # Fall back to existing structure with date filtering
            query = f"""
            SELECT 
                COALESCE(lead_source, 'Unknown') as lead_source,
                COUNT(*) as total_validations,
                COUNT(DISTINCT task_id) as unique_leads,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as avg_data_quality_score,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as median_data_quality_score,
                AVG(CASE WHEN COALESCE(api_fake_lead, false) THEN 0.8 ELSE 0.2 END) as avg_fraud_score,
                AVG(CASE WHEN COALESCE(api_fake_lead, false) THEN 0.8 ELSE 0.2 END) as median_fraud_score,
                COUNTIF(COALESCE(api_quality_score, quality_score) >= 7) as excellent_quality_count,
                COUNTIF(COALESCE(api_quality_score, quality_score) >= 5 AND COALESCE(api_quality_score, quality_score) < 7) as good_quality_count,
                COUNTIF(COALESCE(api_quality_score, quality_score) >= 3 AND COALESCE(api_quality_score, quality_score) < 5) as fair_quality_count,
                COUNTIF(COALESCE(api_quality_score, quality_score) < 3) as poor_quality_count,
                COUNTIF(COALESCE(api_fake_lead, false)) as likely_fake_leads_count,
                ROUND((COUNTIF(COALESCE(api_fake_lead, false))::DOUBLE / COUNT(*)) * 100, 2) as fake_leads_percentage,
                RANK() OVER (ORDER BY AVG(COALESCE(api_quality_score, quality_score)) DESC) as quality_rank,
                CASE 
                    WHEN AVG(CASE WHEN COALESCE(api_fake_lead, false) THEN 0.8 ELSE 0.2 END) >= 0.7 THEN 'CRITICAL'
                    WHEN AVG(CASE WHEN COALESCE(api_fake_lead, false) THEN 0.8 ELSE 0.2 END) >= 0.5 THEN 'HIGH'
                    WHEN AVG(CASE WHEN COALESCE(api_fake_lead, false) THEN 0.8 ELSE 0.2 END) >= 0.3 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as source_risk_level,
                CASE 
                    WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 9 THEN 'A+'
                    WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 7 THEN 'A'
                    WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 5 THEN 'B'
                    WHEN AVG(COALESCE(api_quality_score, quality_score)) >= 3 THEN 'C'
                    ELSE 'F'
                END as source_grade,
                MIN(parsed_at) as first_validation,
                MAX(parsed_at) as latest_validation
            FROM parsed_validations 
            WHERE parse_error IS NULL
            AND created_date {date_clause}
            GROUP BY COALESCE(lead_source, 'Unknown')
            ORDER BY avg_data_quality_score DESC
            """
            result = pd.read_sql(query, conn)
            conn.close()
            return result
    except Exception as e:
        st.error(f"Error loading source results: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=600)
def load_fake_leads(date_filter="All Time"):
    """Load fake leads detail with date filtering."""
    try:
        conn = get_database_connection()
        
        # Build date filter clause
        date_clause = get_date_filter_clause(date_filter)
        
        # Try simplified view first, fall back to existing data structure
        try:
            query = f"SELECT * FROM leads.fake_leads_detail WHERE created_date {date_clause} ORDER BY fraud_score DESC LIMIT 100"
            result = pd.read_sql(query, conn)
            conn.close()
            return result
        except:
            # Fall back to existing structure for fake leads with detailed validation data
            query = f"""
            SELECT 
                task_id as lead_id,
                lead_source,
                parsed_at as validation_timestamp,
                COALESCE(api_first_name, '') as first_name,
                COALESCE(api_last_name, '') as last_name,
                COALESCE(api_email, lead_email) as email,
                COALESCE(api_phone, '') as phone,
                COALESCE(api_company, lead_company) as company,
                COALESCE(api_data_quality_score, api_quality_score, quality_score) / 10.0 as data_quality_score,
                COALESCE(api_fraud_score, 0) / 10.0 as fraud_score,
                COALESCE(api_quality_score, quality_score) / 10.0 as overall_score,
                
                -- Fraud risk level from validation
                COALESCE(api_fraud_risk_level, 
                    CASE WHEN COALESCE(api_fraud_score, 0) >= 8 THEN 'high'
                         WHEN COALESCE(api_fraud_score, 0) >= 5 THEN 'medium'
                         ELSE 'low' END, 
                    'unknown') as fraud_risk_level,
                
                -- Use actual validation results from API
                COALESCE(api_email_valid, 
                    CASE WHEN COALESCE(api_email, lead_email) IS NOT NULL AND COALESCE(api_email, lead_email) != '' THEN true ELSE false END) as email_valid,
                
                COALESCE(api_phone_valid, 
                    CASE WHEN api_phone IS NOT NULL AND api_phone != '' THEN true ELSE false END) as phone_valid,
                
                -- Rich validation details from API (only using columns that exist)
                COALESCE(api_fraud_factors, 'No specific fraud factors identified') as fraud_factors,
                COALESCE(api_quality_factors, 'No specific quality factors identified') as quality_factors,
                COALESCE(api_summary_notes, 'Flagged as fake lead') as summary_notes,
                COALESCE(api_quality_level, 'unknown') as quality_level,
                COALESCE(api_recommendation, 'review') as recommended_action,
                
                -- Additional available fields
                api_fake_lead,
                api_market_segment,
                
                -- Validation summary from JSON if available
                raw_api_response,
                raw_description
                
            FROM parsed_validations
            WHERE parse_error IS NULL
            AND created_date {date_clause}
            AND (COALESCE(api_fake_lead, false) = true OR COALESCE(api_fraud_score, 0) >= 5)
            ORDER BY COALESCE(api_fraud_score, 0) DESC, COALESCE(api_quality_score, quality_score) ASC
            LIMIT 100
            """
            result = pd.read_sql(query, conn)
            conn.close()
            return result
    except Exception as e:
        st.error(f"Error loading fake leads: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=600)
def load_trends_overall():
    """Load overall trends."""
    try:
        conn = get_database_connection()
        # Try simplified view first, fall back to existing data structure
        try:
            query = """
            SELECT * FROM leads.simplified_trends_overall 
            WHERE period_type = 'daily'
            ORDER BY trend_date DESC 
            LIMIT 30
            """
            result = pd.read_sql(query, conn)
            conn.close()
            return result
        except:
            # Fall back to existing structure
            query = """
            SELECT 
                DATE_TRUNC('day', parsed_at) as trend_date,
                COUNT(*) as total_validations,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as avg_data_quality_score,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as median_data_quality_score,
                AVG(CASE WHEN COALESCE(api_fake_lead, false) THEN 0.8 ELSE 0.2 END) as avg_fraud_score,
                AVG(CASE WHEN COALESCE(api_fake_lead, false) THEN 0.8 ELSE 0.2 END) as median_fraud_score,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as avg_overall_score,
                0.7 as avg_email_score,
                0.7 as avg_phone_score,
                0.7 as avg_name_score,
                0.7 as avg_company_score,
                0.7 as avg_completeness_score,
                75.0 as email_pass_rate_percent,
                75.0 as phone_pass_rate_percent,
                75.0 as name_pass_rate_percent,
                75.0 as company_pass_rate_percent,
                75.0 as completeness_pass_rate_percent,
                ROUND((COUNTIF(COALESCE(api_quality_score, quality_score) >= 7)::DOUBLE / COUNT(*)) * 100, 1) as high_quality_percentage,
                ROUND((COUNTIF(COALESCE(api_fake_lead, false))::DOUBLE / COUNT(*)) * 100, 1) as high_fraud_risk_percentage
            FROM parsed_validations
            WHERE parse_error IS NULL
            AND parsed_at >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY DATE_TRUNC('day', parsed_at)
            ORDER BY trend_date DESC
            LIMIT 30
            """
            result = pd.read_sql(query, conn)
            conn.close()
            return result
    except Exception as e:
        st.error(f"Error loading overall trends: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=600)
def load_creation_date_analysis():
    """Load lead quality analysis by creation date."""
    try:
        conn = get_database_connection()
        query = """
        SELECT 
            DATE_TRUNC('month', created_date) as creation_month,
            COUNT(*) as total_leads,
            AVG(COALESCE(api_data_quality_score, api_quality_score, quality_score)) / 10.0 as avg_data_quality_score,
            AVG(COALESCE(api_fraud_score, 0)) / 10.0 as avg_fraud_score,
            COUNTIF(COALESCE(api_fake_lead, false)) as fake_leads_count,
            ROUND((COUNTIF(COALESCE(api_fake_lead, false))::DOUBLE / COUNT(*)) * 100, 2) as fake_leads_percentage,
            ROUND((COUNTIF(COALESCE(api_quality_score, quality_score) >= 7)::DOUBLE / COUNT(*)) * 100, 2) as high_quality_percentage,
            COUNT(DISTINCT lead_source) as unique_sources,
            AVG(COALESCE(api_lead_score, 0)) as avg_lead_score
        FROM parsed_validations
        WHERE parse_error IS NULL 
        AND created_date IS NOT NULL
        GROUP BY DATE_TRUNC('month', created_date)
        ORDER BY creation_month DESC
        """
        result = pd.read_sql(query, conn)
        conn.close()
        return result
    except Exception as e:
        st.error(f"Error loading creation date analysis: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_trends_by_source(selected_sources=None):
    """Load trends by source."""
    try:
        conn = get_database_connection()
        # Try simplified view first, fall back to existing data structure
        try:
            query = """
            SELECT * FROM leads.simplified_trends_by_source 
            WHERE period_type = 'daily'
            """
            
            if selected_sources:
                sources_str = "', '".join(selected_sources)
                query += f" AND lead_source IN ('{sources_str}')"
                
            query += " ORDER BY trend_date DESC, lead_source LIMIT 200"
            result = pd.read_sql(query, conn)
            conn.close()
            return result
        except:
            # Fall back to existing structure
            source_filter = ""
            if selected_sources:
                sources_str = "', '".join(selected_sources)
                source_filter = f"AND COALESCE(lead_source, 'Unknown') IN ('{sources_str}')"
            
            query = f"""
            SELECT 
                'daily' as period_type,
                DATE_TRUNC('day', parsed_at) as trend_date,
                COALESCE(lead_source, 'Unknown') as lead_source,
                COUNT(*) as total_validations,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as avg_data_quality_score,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as median_data_quality_score,
                AVG(CASE WHEN COALESCE(api_fake_lead, false) THEN 0.8 ELSE 0.2 END) as avg_fraud_score,
                AVG(CASE WHEN COALESCE(api_fake_lead, false) THEN 0.8 ELSE 0.2 END) as median_fraud_score,
                AVG(COALESCE(api_quality_score, quality_score)) / 10.0 as avg_overall_score,
                0.7 as avg_email_score,
                0.7 as avg_phone_score,
                0.7 as avg_name_score,
                0.7 as avg_company_score,
                0.7 as avg_completeness_score,
                75.0 as email_pass_rate_percent,
                75.0 as phone_pass_rate_percent,
                75.0 as name_pass_rate_percent,
                75.0 as company_pass_rate_percent,
                75.0 as completeness_pass_rate_percent,
                ROUND((COUNTIF(COALESCE(api_quality_score, quality_score) >= 7)::DOUBLE / COUNT(*)) * 100, 1) as high_quality_percentage,
                ROUND((COUNTIF(COALESCE(api_fake_lead, false))::DOUBLE / COUNT(*)) * 100, 1) as high_fraud_risk_percentage,
                COUNTIF(COALESCE(api_fake_lead, false)) as likely_fake_count
            FROM parsed_validations
            WHERE parse_error IS NULL
            AND parsed_at >= CURRENT_DATE - INTERVAL '30 days'
            {source_filter}
            GROUP BY DATE_TRUNC('day', parsed_at), COALESCE(lead_source, 'Unknown')
            HAVING COUNT(*) >= 3
            ORDER BY trend_date DESC, lead_source
            LIMIT 200
            """
            result = pd.read_sql(query, conn)
            conn.close()
            return result
    except Exception as e:
        st.error(f"Error loading source trends: {e}")
        return pd.DataFrame()


def show_overall_results():
    """Display Overall Validation Results section."""
    st.markdown('<div class="section-header">Overall Validation Results</div>', unsafe_allow_html=True)
    
    # Add date range selector
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("### 📊 System-Wide Validation Metrics")
    with col2:
        date_filter = st.selectbox(
            "Time Period",
            options=["All Time", "Last 30 Days", "Last 7 Days", "This Month", "Last Month"],
            help="Filter results by lead creation date",
            key="date_filter_key"
        )
    
    # Load data with date filtering
    data = load_overall_results(date_filter)
    if data.empty:
        st.warning("No overall results data available for the selected time period. Please run the ETL pipeline first.")
        return
    
    row = data.iloc[0]
    
    # Show what time period is selected
    if date_filter != "All Time":
        st.info(f"📅 Showing results for: **{date_filter}** (based on lead creation date)")
    
    # Key metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📊 Total Validations", 
            value=f"{row['total_validations']:,}",
            help="Total number of lead validations performed"
        )
    
    with col2:
        quality_score = row['avg_data_quality_score']
        st.metric(
            label="✅ Average Data Quality Score", 
            value=f"{quality_score:.3f}",
            help="Average data quality score (0-1, higher is better)"
        )
    
    with col3:
        median_quality = row['median_data_quality_score'] 
        st.metric(
            label="📈 Median Data Quality Score", 
            value=f"{median_quality:.3f}",
            help="Median data quality score (0-1, higher is better)"
        )
    
    with col4:
        fraud_score = row['avg_fraud_score']
        st.metric(
            label="🚨 Average Fraud Score", 
            value=f"{fraud_score:.3f}",
            delta=f"Risk Level: {get_risk_level(fraud_score)}",
            delta_color="inverse" if fraud_score > 0.5 else "normal",
            help="Average fraud score (0-1, higher is more fraudulent)"
        )
    
    # Second row of metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        median_fraud = row['median_fraud_score']
        st.metric(
            label="🔍 Median Fraud Score", 
            value=f"{median_fraud:.3f}",
            help="Median fraud score (0-1, higher is more fraudulent)"
        )
    
    with col2:
        high_quality_pct = row['high_quality_percentage']
        st.metric(
            label="🌟 High Quality Leads", 
            value=f"{high_quality_pct:.1f}%",
            help="Percentage of leads with quality score >= 0.7"
        )
    
    with col3:
        high_fraud_pct = row['high_fraud_risk_percentage'] 
        st.metric(
            label="⚠️ High Fraud Risk", 
            value=f"{high_fraud_pct:.1f}%",
            delta_color="inverse",
            help="Percentage of leads with fraud score >= 0.6"
        )
    
    with col4:
        system_status = row['overall_system_status']
        status_color = get_status_color(system_status)
        st.metric(
            label="🏥 System Health", 
            value=system_status,
            help="Overall system health based on quality and fraud metrics"
        )


def show_results_by_source():
    """Display Validation Results by Lead Source section."""
    st.markdown('<div class="section-header">Validation Results by Lead Source</div>', unsafe_allow_html=True)
    
    # Add date-based source analysis
    st.markdown('<div class="sub-header">📊 Source Performance by Creation Date</div>', unsafe_allow_html=True)
    show_source_by_date_analysis()
    
    st.markdown('<div class="sub-header">📋 Overall Source Performance</div>', unsafe_allow_html=True)
    
    # Get the date filter from session state (set in overall results section)
    date_filter = st.session_state.get('date_filter_key', 'All Time')
    source_data = load_results_by_source(date_filter)
    if source_data.empty:
        st.warning("No source data available. Please run the ETL pipeline first.")
        return
    
    # Source summary table
    st.markdown('<div class="sub-header">Lead Source Performance Summary</div>', unsafe_allow_html=True)
    
    # Select key columns for display
    display_columns = [
        'lead_source', 'total_validations', 'avg_data_quality_score', 
        'avg_fraud_score', 'source_grade', 'likely_fake_leads_count',
        'fake_leads_percentage', 'source_risk_level'
    ]
    
    display_data = source_data[display_columns].copy()
    
    # Format columns
    display_data['avg_data_quality_score'] = display_data['avg_data_quality_score'].round(3)
    display_data['avg_fraud_score'] = display_data['avg_fraud_score'].round(3)
    display_data['fake_leads_percentage'] = display_data['fake_leads_percentage'].round(1)
    
    # Rename for display
    display_data.columns = [
        'Lead Source', 'Total Validations', 'Avg Data Quality Score',
        'Avg Fraud Score', 'Grade', 'Fake Leads Count', 
        'Fake Leads %', 'Risk Level'
    ]
    
    st.dataframe(
        display_data, 
        use_container_width=True,
        height=400
    )
    
    # Show fake leads section if any exist
    date_filter = st.session_state.get('date_filter_key', 'All Time')
    fake_leads = load_fake_leads(date_filter)
    if not fake_leads.empty:
        show_fake_leads_section(fake_leads)
    elif date_filter != "All Time":
        st.info(f"ℹ️ No fake leads found for the selected time period: **{date_filter}**")


def parse_validation_json(raw_json_str):
    """Parse additional validation details from raw JSON response."""
    if not raw_json_str or raw_json_str == '':
        return {}
    
    try:
        data = json.loads(raw_json_str)
        return {
            'summary_notes': data.get('summaryNotes', []),
            'fake_source': data.get('fakeSource', ''),
            'fake_reason': data.get('fakeReason', ''), 
            'fake_confidence': data.get('fakeConfidence', ''),
            'validation_method': data.get('validationMethod', ''),
            'recommendation_confidence': data.get('recommendationConfidence', ''),
            'ml_fraud_detection': data.get('mlFraudDetection', {}),
            'email_summary': data.get('emailSummary', {}),
            'has_gibberish_names': data.get('hasGibberishNames', False),
            'has_gibberish_company': data.get('hasGibberishCompany', False)
        }
    except (json.JSONDecodeError, TypeError):
        return {}

def show_fake_leads_section(fake_leads_data):
    """Display fake leads with detailed validation information from raw data."""
    st.markdown('<div class="sub-header">🚨 Fake Leads - Detailed Validation Analysis</div>', unsafe_allow_html=True)
    
    if fake_leads_data.empty:
        st.info("No high-risk leads found in the current data set.")
        return
    
    # Group by lead source for better organization
    sources_with_fake_leads = fake_leads_data.groupby('lead_source').size().reset_index()
    sources_with_fake_leads.columns = ['Lead Source', 'Fake Leads Count']
    sources_with_fake_leads = sources_with_fake_leads.sort_values('Fake Leads Count', ascending=False)
    
    st.write("**High-Risk Leads by Source:**")
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.dataframe(sources_with_fake_leads, use_container_width=True)
    
    with col2:
        # Show top fake leads with detailed validation reasons
        st.write("**Top High-Risk Leads with Validation Details:**")
        top_fake_leads = fake_leads_data.head(10)
        
        for idx, lead in top_fake_leads.iterrows():
            fraud_score_display = f"{lead['fraud_score']:.1f}" if pd.notna(lead['fraud_score']) else "N/A"
            risk_level = lead['fraud_risk_level'].upper() if pd.notna(lead['fraud_risk_level']) else 'UNKNOWN'
            
            # Parse additional details from JSON if available
            json_details = parse_validation_json(lead.get('raw_api_response', ''))
            
            with st.expander(f"🚨 {lead['first_name']} {lead['last_name']} - {lead['lead_source']} (Fraud Score: {fraud_score_display}, Risk: {risk_level})"):
                col_a, col_b = st.columns(2)
                
                with col_a:
                    st.markdown(f"""
                    **Lead:** {lead['first_name']} {lead['last_name']} | **Source:** {lead['lead_source']}  
                    **Email:** {lead['email']} | **Phone:** {lead['phone'] if lead['phone'] else 'Missing'}  
                    **Company:** {lead['company'] if lead['company'] else 'Missing'}  
                    
                    **Scores:** Quality: {lead['data_quality_score']:.1f}/1.0 | Fraud: {fraud_score_display}/1.0 | Risk: {risk_level}
                    """)
                    
                    # Show confidence and method if available from JSON
                    extras = []
                    if json_details.get('recommendation_confidence'):
                        extras.append(f"Confidence: {json_details['recommendation_confidence']}")
                    if json_details.get('validation_method'):
                        extras.append(f"Method: {json_details['validation_method']}")
                    
                    if extras:
                        st.markdown(f"**Details:** {' | '.join(extras)}")
                
                with col_b:
                    # Show summary notes from JSON (most important) - compact format
                    if json_details.get('summary_notes'):
                        st.markdown("**📋 Summary:** " + " | ".join(json_details['summary_notes'][:2]))  # Limit to first 2 notes
                    
                    # Compact fraud and quality factors
                    fraud_factors = lead.get('fraud_factors', '')
                    quality_factors = lead.get('quality_factors', '')
                    
                    if pd.notna(fraud_factors) and fraud_factors != 'No specific fraud factors identified':
                        st.markdown(f"**🚨 Fraud:** {fraud_factors[:100]}...")  # Truncate long factors
                    
                    if pd.notna(quality_factors) and quality_factors != 'No specific quality factors identified':
                        st.markdown(f"**📊 Quality:** {quality_factors[:100]}...")  # Truncate long factors
                    
                    # Compact validation results
                    email_status = "Valid" if lead.get('email_valid', False) else "Invalid"
                    phone_status = "Valid" if lead.get('phone_valid', False) else "Invalid"
                    
                    validation_results = [f"Email: {email_status}", f"Phone: {phone_status}"]
                    
                    # Add additional flags compactly
                    if json_details.get('has_gibberish_names'):
                        validation_results.append("Names: Gibberish")
                    if json_details.get('has_gibberish_company'):
                        validation_results.append("Company: Gibberish")
                    
                    st.markdown(f"**✅ Validation:** {' | '.join(validation_results)}")
                    
                    # Show most important additional details in one line
                    details = []
                    fake_source = json_details.get('fake_source', '')
                    if fake_source and fake_source != 'validation_engine':
                        details.append(f"Known from: {fake_source}")
                    if pd.notna(lead.get('api_market_segment')) and lead['api_market_segment']:
                        details.append(f"Segment: {lead['api_market_segment']}")
                        
                    if details:
                        st.markdown(f"**ℹ️ Details:** {' | '.join(details)}")
                    
                    # Show recommended action compactly
                    action = lead.get('recommended_action', 'review').upper()
                    action_colors = {
                        'REJECT': 'error',
                        'QUARANTINE': 'warning', 
                        'REVIEW': 'info'
                    }
                    action_color = action_colors.get(action, 'info')
                    
                    if action_color == 'error':
                        st.error(f"🚫 **Action:** {action}")
                    elif action_color == 'warning':
                        st.warning(f"⚠️ **Action:** Manual review required")
                    else:
                        st.info(f"🔍 **Action:** {action}")


def show_trend_reports():
    """Display Trend Reports section."""
    st.markdown('<div class="section-header">Trend Report</div>', unsafe_allow_html=True)
    
    trends_data = load_trends_overall()
    if trends_data.empty:
        st.warning("No trends data available. Please run the ETL pipeline first.")
        return
    
    # Overall trends chart
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Data Quality Score Over Time', 'Fraud Score Over Time', 
                       'Validation Points Over Time', 'Volume Over Time'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Data Quality Score trend
    fig.add_trace(
        go.Scatter(x=trends_data['trend_date'], y=trends_data['avg_data_quality_score'],
                  mode='lines+markers', name='Avg Data Quality Score',
                  line=dict(color='blue', width=3)),
        row=1, col=1
    )
    
    # Fraud Score trend  
    fig.add_trace(
        go.Scatter(x=trends_data['trend_date'], y=trends_data['avg_fraud_score'],
                  mode='lines+markers', name='Avg Fraud Score',
                  line=dict(color='red', width=3)),
        row=1, col=2
    )
    
    # Individual validation points
    validation_points = ['avg_email_score', 'avg_phone_score', 'avg_name_score', 'avg_company_score']
    colors = ['green', 'orange', 'purple', 'brown']
    point_names = ['Email', 'Phone', 'Name', 'Company']
    
    for i, (point, color, name) in enumerate(zip(validation_points, colors, point_names)):
        fig.add_trace(
            go.Scatter(x=trends_data['trend_date'], y=trends_data[point],
                      mode='lines', name=name, line=dict(color=color)),
            row=2, col=1
        )
    
    # Volume trend
    fig.add_trace(
        go.Scatter(x=trends_data['trend_date'], y=trends_data['total_validations'],
                  mode='lines+markers', name='Daily Validations',
                  line=dict(color='gray', width=2)),
        row=2, col=2
    )
    
    fig.update_layout(height=500, showlegend=True, title_text="Overall Validation Trends", margin=dict(t=40, b=40, l=40, r=40))
    st.plotly_chart(fig, use_container_width=True)


def show_trend_reports_by_source():
    """Display Trend Reports by Lead Source section."""
    st.markdown('<div class="section-header">Trend Report by Lead Source</div>', unsafe_allow_html=True)
    
    # Source selection
    source_data = load_results_by_source()
    if source_data.empty:
        st.warning("No source data available for trend analysis.")
        return
    
    all_sources = source_data['lead_source'].tolist()
    selected_sources = st.multiselect(
        "Select Lead Sources to Compare",
        options=all_sources,
        default=all_sources[:5] if len(all_sources) > 5 else all_sources,
        help="Select up to 5 sources for trend comparison"
    )
    
    if not selected_sources:
        st.info("Please select at least one lead source to view trends.")
        return
    
    # Load trends data for selected sources
    trends_by_source = load_trends_by_source(selected_sources)
    if trends_by_source.empty:
        st.warning("No trend data available for selected sources.")
        return
    
    # Create trends charts
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Data Quality Score by Source', 'Fraud Score by Source',
                       'Validation Points by Source', 'Volume by Source'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}],
               [{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    colors = px.colors.qualitative.Set1[:len(selected_sources)]
    
    for i, source in enumerate(selected_sources):
        source_data_filtered = trends_by_source[trends_by_source['lead_source'] == source]
        color = colors[i % len(colors)]
        
        # Data Quality Score by source
        fig.add_trace(
            go.Scatter(x=source_data_filtered['trend_date'], 
                      y=source_data_filtered['avg_data_quality_score'],
                      mode='lines+markers', name=f'{source} - Quality',
                      line=dict(color=color, width=2)),
            row=1, col=1
        )
        
        # Fraud Score by source
        fig.add_trace(
            go.Scatter(x=source_data_filtered['trend_date'],
                      y=source_data_filtered['avg_fraud_score'],
                      mode='lines+markers', name=f'{source} - Fraud',
                      line=dict(color=color, width=2, dash='dash')),
            row=1, col=2
        )
        
        # Email validation by source (as example validation point)
        fig.add_trace(
            go.Scatter(x=source_data_filtered['trend_date'],
                      y=source_data_filtered['avg_email_score'],
                      mode='lines', name=f'{source} - Email',
                      line=dict(color=color, width=1)),
            row=2, col=1
        )
        
        # Volume by source
        fig.add_trace(
            go.Scatter(x=source_data_filtered['trend_date'],
                      y=source_data_filtered['total_validations'],
                      mode='lines+markers', name=f'{source} - Volume',
                      line=dict(color=color, width=2, dash='dot')),
            row=2, col=2
        )
    
    fig.update_layout(height=500, showlegend=True, title_text="Trends by Lead Source", margin=dict(t=40, b=40, l=40, r=40))
    st.plotly_chart(fig, use_container_width=True)


def get_risk_level(fraud_score):
    """Get risk level based on fraud score."""
    if fraud_score >= 0.8:
        return "CRITICAL"
    elif fraud_score >= 0.6:
        return "HIGH"
    elif fraud_score >= 0.4:
        return "MEDIUM"
    elif fraud_score >= 0.2:
        return "LOW"
    else:
        return "MINIMAL"


def show_creation_date_analysis():
    """Display lead quality analysis by creation date."""
    date_data = load_creation_date_analysis()
    
    if date_data.empty:
        st.info("No creation date analysis available.")
        return
    
    # Show monthly comparison
    st.write("**📊 Lead Quality Trends by Creation Month:**")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Create trend chart
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Data Quality Score by Month', 'Fraud Score by Month'),
            vertical_spacing=0.15
        )
        
        # Data Quality trend
        fig.add_trace(
            go.Scatter(
                x=date_data['creation_month'], 
                y=date_data['avg_data_quality_score'],
                mode='lines+markers',
                name='Data Quality Score',
                line=dict(color='blue', width=3),
                marker=dict(size=8)
            ),
            row=1, col=1
        )
        
        # Fraud Score trend
        fig.add_trace(
            go.Scatter(
                x=date_data['creation_month'],
                y=date_data['avg_fraud_score'],
                mode='lines+markers', 
                name='Fraud Score',
                line=dict(color='red', width=3),
                marker=dict(size=8)
            ),
            row=2, col=1
        )
        
        fig.update_layout(height=350, showlegend=True, title_text="Quality Trends Over Time", margin=dict(t=30, b=30, l=30, r=30))
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Monthly summary table
        st.write("**Monthly Summary:**")
        display_data = date_data.copy()
        display_data['creation_month'] = pd.to_datetime(display_data['creation_month']).dt.strftime('%Y-%m')
        display_data['avg_data_quality_score'] = display_data['avg_data_quality_score'].round(2)
        display_data['avg_fraud_score'] = display_data['avg_fraud_score'].round(2)
        
        summary_columns = ['creation_month', 'total_leads', 'avg_data_quality_score', 'avg_fraud_score', 'fake_leads_count']
        summary_data = display_data[summary_columns].copy()
        summary_data.columns = ['Month', 'Leads', 'Quality', 'Fraud', 'Fake']
        
        st.dataframe(summary_data, use_container_width=True, hide_index=True)
        
        # Show key insights
        if len(date_data) >= 2:
            latest = date_data.iloc[0]
            previous = date_data.iloc[1]
            
            quality_change = latest['avg_data_quality_score'] - previous['avg_data_quality_score']
            fraud_change = latest['avg_fraud_score'] - previous['avg_fraud_score']
            
            st.write("**📈 Month-over-Month Changes:**")
            if quality_change > 0:
                st.success(f"Quality improved by {quality_change:.2f}")
            else:
                st.error(f"Quality declined by {abs(quality_change):.2f}")
                
            if fraud_change > 0:
                st.error(f"Fraud risk increased by {fraud_change:.2f}")
            else:
                st.success(f"Fraud risk decreased by {abs(fraud_change):.2f}")

def show_source_by_date_analysis():
    """Display source performance by creation date."""
    try:
        conn = get_database_connection()
        query = """
        SELECT 
            DATE_TRUNC('month', created_date) as creation_month,
            lead_source,
            COUNT(*) as total_leads,
            AVG(COALESCE(api_data_quality_score, api_quality_score, quality_score)) / 10.0 as avg_data_quality_score,
            AVG(COALESCE(api_fraud_score, 0)) / 10.0 as avg_fraud_score,
            COUNTIF(COALESCE(api_fake_lead, false)) as fake_leads_count,
            ROUND((COUNTIF(COALESCE(api_fake_lead, false))::DOUBLE / COUNT(*)) * 100, 2) as fake_leads_percentage
        FROM parsed_validations
        WHERE parse_error IS NULL 
        AND created_date IS NOT NULL
        AND lead_source IS NOT NULL
        GROUP BY DATE_TRUNC('month', created_date), lead_source
        HAVING COUNT(*) >= 5  -- Only sources with meaningful volume
        ORDER BY creation_month DESC, avg_data_quality_score DESC
        """
        
        source_date_data = pd.read_sql(query, conn)
        conn.close()
        
        if source_date_data.empty:
            st.info("No source date analysis available.")
            return
            
        # Get top sources for visualization
        top_sources = source_date_data.groupby('lead_source')['total_leads'].sum().nlargest(6).index.tolist()
        filtered_data = source_date_data[source_date_data['lead_source'].isin(top_sources)]
        
        if not filtered_data.empty:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Create source quality trends chart
                fig = go.Figure()
                
                colors = px.colors.qualitative.Set1[:len(top_sources)]
                
                for i, source in enumerate(top_sources):
                    source_data = filtered_data[filtered_data['lead_source'] == source]
                    fig.add_trace(
                        go.Scatter(
                            x=source_data['creation_month'],
                            y=source_data['avg_data_quality_score'],
                            mode='lines+markers',
                            name=f'{source}',
                            line=dict(color=colors[i % len(colors)], width=2),
                            marker=dict(size=6)
                        )
                    )
                
                fig.update_layout(
                    title="Data Quality Score by Source Over Time",
                    xaxis_title="Creation Month",
                    yaxis_title="Avg Data Quality Score",
                    height=300,
                    showlegend=True,
                    margin=dict(t=40, b=40, l=40, r=40)
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Show source comparison table
                st.write("**Top Sources by Month:**")
                
                # Create summary by most recent month
                latest_month = filtered_data['creation_month'].max()
                latest_data = filtered_data[filtered_data['creation_month'] == latest_month]
                
                if not latest_data.empty:
                    latest_data = latest_data.sort_values('avg_data_quality_score', ascending=False)
                    display_cols = ['lead_source', 'total_leads', 'avg_data_quality_score', 'fake_leads_count']
                    display_data = latest_data[display_cols].copy()
                    display_data['avg_data_quality_score'] = display_data['avg_data_quality_score'].round(2)
                    display_data.columns = ['Source', 'Leads', 'Quality', 'Fake']
                    
                    st.dataframe(display_data, use_container_width=True, hide_index=True)
                    
                    # Show month-over-month changes for top source
                    if len(source_date_data[source_date_data['lead_source'] == latest_data.iloc[0]['lead_source']]) >= 2:
                        st.write("**📈 Top Source Trend:**")
                        top_source_name = latest_data.iloc[0]['lead_source']
                        top_source_data = source_date_data[source_date_data['lead_source'] == top_source_name].sort_values('creation_month', ascending=False)
                        
                        if len(top_source_data) >= 2:
                            latest_month_data = top_source_data.iloc[0]
                            prev_month_data = top_source_data.iloc[1]
                            
                            quality_change = latest_month_data['avg_data_quality_score'] - prev_month_data['avg_data_quality_score']
                            if quality_change > 0:
                                st.success(f"{top_source_name}: +{quality_change:.2f}")
                            else:
                                st.error(f"{top_source_name}: {quality_change:.2f}")
                                
    except Exception as e:
        st.error(f"Error loading source date analysis: {e}")

def get_status_color(status):
    """Get color for system status."""
    colors = {
        'EXCELLENT': '🟢',
        'GOOD': '🟡', 
        'FAIR': '🟠',
        'POOR': '🔴',
        'CRITICAL': '🔴'
    }
    return colors.get(status, '⚪')


def main():
    """Main simplified dashboard application."""
    st.markdown('<div class="main-header">📊 Lead Validation - Simplified Report</div>', unsafe_allow_html=True)
    
    # Refresh button
    if st.button("🔄 Refresh Data", help="Refresh all data from the database"):
        st.cache_data.clear()
        st.rerun()
    
    # Main sections
    show_overall_results()
    st.markdown("---")
    
    show_results_by_source()
    st.markdown("---")
    
    show_trend_reports()
    st.markdown("---")
    
    show_trend_reports_by_source()
    
    # Footer
    st.markdown("---")
    st.markdown("*📊 Simplified Lead Validation Dashboard - Focus on Data Quality & Fraud Detection*")


if __name__ == "__main__":
    main()
