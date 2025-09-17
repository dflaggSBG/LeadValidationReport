"""Daily Fake Leads by Source Report - Focused monitoring dashboard."""
import streamlit as st
import sys
import subprocess
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

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
def load_daily_fake_leads():
    """Load today's fake leads by source."""
    try:
        conn = get_database_connection()
        
        # Try to use the dedicated view first
        try:
            query = "SELECT * FROM leads.daily_fake_leads_by_source"
            result = pd.read_sql(query, conn)
            conn.close()
            return result
        except:
            # Fall back to direct query
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
            AND DATE_TRUNC('day', created_date) = CURRENT_DATE
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
def load_daily_fake_leads_detail():
    """Load detailed list of today's fake and high risk leads."""
    try:
        conn = get_database_connection()
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
        AND DATE_TRUNC('day', created_date) = CURRENT_DATE
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
    """Show daily summary statistics."""
    st.markdown('<div class="section-header">📊 Today\'s Fake Leads Summary</div>', unsafe_allow_html=True)
    
    data = load_daily_fake_leads()
    
    if data.empty:
        st.success("🎉 **No fake leads detected today!** All sources are clean.")
        return
    
    # Overall daily metrics
    total_leads_today = data['total_leads_today'].sum()
    total_fake_leads = data['fake_leads_count'].sum()
    total_fake_percentage = (total_fake_leads / total_leads_today * 100) if total_leads_today > 0 else 0
    total_high_risk = data['critical_fraud_count'].sum() if 'critical_fraud_count' in data.columns else 0
    total_high_risk_percentage = (total_high_risk / total_leads_today * 100) if total_leads_today > 0 else 0
    
    # Key metrics cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="📊 Leads Validated Today",
            value=f"{total_leads_today:,}",
            help="Leads created today that went through validation (may differ from current SF status)"
        )
    
    with col2:
        st.metric(
            label="🚨 Fake Leads Today", 
            value=f"{total_fake_leads}",
            delta=f"{total_fake_percentage:.1f}% of today's leads",
            delta_color="inverse" if total_fake_leads > 0 else "normal",
            help="Number and percentage of fake leads detected today"
        )
    
    with col3:
        st.metric(
            label="⚠️ High Risk Leads",
            value=f"{total_high_risk}",
            delta=f"{total_high_risk_percentage:.1f}% of today's leads",
            delta_color="inverse" if total_high_risk > 0 else "normal",
            help="Leads with critical fraud scores (8+/10) requiring attention"
        )


def show_fake_leads_by_source_table():
    """Show a clean table of fake leads count by lead source."""
    st.markdown('<div class="section-header">📊 Fake Leads by Lead Source</div>', unsafe_allow_html=True)
    
    data = load_daily_fake_leads()
    
    if data.empty:
        st.info("No data available for today.")
        return
    
    # Create focused table
    table_columns = ['lead_source', 'total_leads_today', 'fake_leads_count', 'fake_leads_percentage', 'daily_risk_level']
    if 'critical_fraud_count' in data.columns:
        table_columns.insert(3, 'critical_fraud_count')
    
    table_data = data[table_columns].copy()
    
    # Sort by fake leads count (highest first), then by high risk, then by percentage
    if 'critical_fraud_count' in table_data.columns:
        table_data = table_data.sort_values(['fake_leads_count', 'critical_fraud_count', 'fake_leads_percentage'], ascending=[False, False, False])
    else:
        table_data = table_data.sort_values(['fake_leads_count', 'fake_leads_percentage'], ascending=[False, False])
    
    # Format the data
    table_data['fake_leads_percentage'] = table_data['fake_leads_percentage'].round(1)
    
    # Rename columns for clean display
    if 'critical_fraud_count' in table_data.columns:
        table_data.columns = ['Lead Source', 'Total Leads', 'Fake Leads', 'High Risk', 'Fake %', 'Risk Level']
    else:
        table_data.columns = ['Lead Source', 'Total Leads', 'Fake Leads', 'Fake %', 'Risk Level']
    
    # Color code the risk levels
    def style_risk_level(val):
        colors = {
            'CRITICAL': 'background-color: #ffcdd2; color: #c62828',
            'HIGH': 'background-color: #ffe0b2; color: #ef6c00', 
            'MEDIUM': 'background-color: #fff3e0; color: #f57c00',
            'LOW': 'background-color: #f3e5f5; color: #8e24aa',
            'CLEAN': 'background-color: #e8f5e8; color: #2e7d32'
        }
        return colors.get(val, '')
    
    # Display the table with formatting
    st.dataframe(
        table_data.style.map(style_risk_level, subset=['Risk Level']),
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


def show_fake_leads_detail():
    """Show detailed list of today's fake and high risk leads."""
    st.markdown('<div class="section-header">🔍 Today\'s Problematic Leads - Detailed Analysis</div>', unsafe_allow_html=True)
    
    problematic_leads = load_daily_fake_leads_detail()
    
    if problematic_leads.empty:
        st.success("🎉 No fake or high risk leads detected today!")
        return
    
    # Count by type
    fake_count = len(problematic_leads[problematic_leads['lead_type'] == 'FAKE'])
    high_risk_count = len(problematic_leads[problematic_leads['lead_type'] == 'HIGH_RISK'])
    
    st.markdown(f"**Found {len(problematic_leads)} problematic leads today:** {fake_count} fake leads + {high_risk_count} high risk leads")
    
    # Group by source for better organization
    for source in problematic_leads['lead_source'].unique():
        source_leads = problematic_leads[problematic_leads['lead_source'] == source]
        
        # Count fake vs high risk for this source
        source_fake_count = len(source_leads[source_leads['lead_type'] == 'FAKE'])
        source_high_risk_count = len(source_leads[source_leads['lead_type'] == 'HIGH_RISK'])
        
        # Create expander title based on what types of leads exist
        if source_fake_count > 0 and source_high_risk_count > 0:
            expander_title = f"🚨 {source} - {source_fake_count} fake + {source_high_risk_count} high risk leads"
        elif source_fake_count > 0:
            expander_title = f"🚨 {source} - {source_fake_count} fake leads"
        else:
            expander_title = f"⚠️ {source} - {source_high_risk_count} high risk leads"
        
        with st.expander(expander_title, expanded=True):
            for _, lead in source_leads.iterrows():
                # Different styling for fake vs high risk
                if lead['lead_type'] == 'FAKE':
                    card_class = "fake-lead-card"
                    type_indicator = "🚨 FAKE"
                else:
                    card_class = "fake-lead-card"  # Use same styling for now
                    type_indicator = "⚠️ HIGH RISK"
                
                st.markdown(f"""
                <div class="{card_class}">
                    <strong>{type_indicator}: {lead['first_name']} {lead['last_name']}</strong> (ID: {lead['lead_id']}) | Fraud: {lead['fraud_score']}/10 | Action: {lead['recommendation'].upper()}<br>
                    Email: {lead['email']} | Phone: {lead['phone'] if lead['phone'] else 'Missing'} | Company: {lead['company'] if lead['company'] else 'Missing'}<br>
                    <strong>Fraud Factors:</strong> {lead['fraud_factors']}<br>
                    <strong>Quality Issues:</strong> {lead['quality_factors']}
                </div>
                """, unsafe_allow_html=True)


def show_hourly_breakdown():
    """Show hourly breakdown of today's fake and high risk leads by source."""
    st.markdown('<div class="section-header">⏰ Hourly Problem Lead Pattern by Source (Today)</div>', unsafe_allow_html=True)
    
    try:
        conn = get_database_connection()
        
        # Get hourly data with source breakdown
        query = """
        SELECT 
            EXTRACT(hour FROM created_date) as hour_of_day,
            COALESCE(lead_source, 'Unknown') as lead_source,
            COUNT(*) as total_leads,
            COUNTIF(COALESCE(api_fake_lead, false) = true OR COALESCE(api_fraud_score, 0) >= 8) as fake_leads,
            ROUND((COUNTIF(COALESCE(api_fake_lead, false) = true)::DOUBLE / COUNT(*)) * 100, 2) as fake_percentage
        FROM parsed_validations
        WHERE parse_error IS NULL
        AND DATE_TRUNC('day', created_date) = CURRENT_DATE
        GROUP BY EXTRACT(hour FROM created_date), COALESCE(lead_source, 'Unknown')
        HAVING COUNT(*) > 0
        ORDER BY hour_of_day, lead_source
        """
        
        hourly_source_data = pd.read_sql(query, conn)
        conn.close()
        
        if hourly_source_data.empty:
            st.info("No hourly data available for today.")
            return
        
        # Create side-by-side charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Total leads by hour and source (stacked)
            fig1 = px.bar(
                hourly_source_data,
                x='hour_of_day',
                y='total_leads',
                color='lead_source',
                title="Total Leads by Hour & Source",
                labels={'hour_of_day': 'Hour', 'total_leads': 'Total Leads'}
            )
            fig1.update_layout(height=300, margin=dict(t=40, b=40, l=40, r=40))
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # Problem leads (fake + high risk) only by hour and source
            problem_only = hourly_source_data[hourly_source_data['fake_leads'] > 0]
            
            if not problem_only.empty:
                fig2 = px.bar(
                    problem_only,
                    x='hour_of_day',
                    y='fake_leads',
                    color='lead_source',
                    title="Problem Leads by Hour & Source",
                    labels={'hour_of_day': 'Hour', 'fake_leads': 'Problem Leads (Fake + High Risk)'}
                )
                fig2.update_layout(height=300, margin=dict(t=40, b=40, l=40, r=40))
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.success("✅ No problematic leads detected at any hour today")
        
        # Show hourly source breakdown table for problem leads only
        if not problem_only.empty:
            st.markdown('<div class="sub-header">📊 Problem Leads by Hour & Source</div>', unsafe_allow_html=True)
            
            # Pivot table for easier reading
            pivot_data = problem_only.pivot(index='lead_source', columns='hour_of_day', values='fake_leads').fillna(0)
            
            # Only show hours that have problem leads
            active_hours = problem_only['hour_of_day'].unique()
            pivot_data = pivot_data[sorted(active_hours)]
            
            # Add row totals
            pivot_data['Total'] = pivot_data.sum(axis=1)
            
            # Sort by total problem leads
            pivot_data = pivot_data.sort_values('Total', ascending=False)
            
            # Format as integers
            pivot_data = pivot_data.astype(int)
            
            st.dataframe(
                pivot_data,
                use_container_width=True,
                height=200
            )
        
    except Exception as e:
        st.error(f"Error loading hourly breakdown: {e}")


@st.cache_data(ttl=300)
def load_fake_leads_anomalies():
    """Load leads that were flagged as fake but may have progressed inappropriately."""
    try:
        conn = get_database_connection()
        
        # Look for leads with contradictory validation vs recommendation
        query = """
        SELECT 
            task_id,
            who_id as lead_id,
            COALESCE(api_first_name, '') as first_name,
            COALESCE(api_last_name, '') as last_name,
            COALESCE(api_email, lead_email) as email,
            COALESCE(api_company, lead_company) as company,
            lead_source,
            created_date,
            parsed_at as validation_timestamp,
            COALESCE(api_fake_lead, false) as was_flagged_fake,
            COALESCE(api_fraud_score, 0) as fraud_score,
            COALESCE(api_fraud_factors, 'No factors') as fraud_factors,
            COALESCE(api_recommendation, 'unknown') as validation_recommendation,
            
            -- Anomaly detection based on contradictions
            CASE 
                WHEN COALESCE(api_fake_lead, false) = true AND COALESCE(api_recommendation, '') = 'accept' THEN 'FAKE_BUT_ACCEPTED'
                WHEN COALESCE(api_fraud_score, 0) >= 8 AND COALESCE(api_recommendation, '') = 'accept' THEN 'HIGH_FRAUD_BUT_ACCEPTED'
                WHEN COALESCE(api_fake_lead, false) = true AND COALESCE(api_fraud_score, 0) < 5 THEN 'FAKE_FLAG_LOW_SCORE'
                WHEN COALESCE(api_recommendation, '') = 'reject' AND COALESCE(api_fraud_score, 0) < 3 THEN 'REJECT_RECOMMENDATION_LOW_SCORE'
                ELSE 'CONSISTENT'
            END as anomaly_type,
            
            -- Risk level
            CASE 
                WHEN COALESCE(api_fake_lead, false) = true AND COALESCE(api_recommendation, '') = 'accept' THEN 'CRITICAL'
                WHEN COALESCE(api_fraud_score, 0) >= 8 AND COALESCE(api_recommendation, '') = 'accept' THEN 'HIGH'
                WHEN COALESCE(api_fake_lead, false) = true THEN 'MEDIUM'
                ELSE 'LOW'
            END as anomaly_risk_level
            
        FROM parsed_validations
        WHERE parse_error IS NULL
        AND DATE_TRUNC('day', created_date) = CURRENT_DATE
        AND (
            COALESCE(api_fake_lead, false) = true 
            OR COALESCE(api_fraud_score, 0) >= 7
            OR COALESCE(api_recommendation, '') = 'reject'
        )
        """
        
        result = pd.read_sql(query, conn)
        conn.close()
        return result
        
    except Exception as e:
        st.error(f"Error loading validation anomalies: {e}")
        return pd.DataFrame()


def show_fake_leads_anomalies():
    """Display validation issues that need review."""
    st.markdown('<div class="section-header">🔍 Quality Review</div>', unsafe_allow_html=True)
    st.markdown("*Leads with high fraud scores that were recommended to accept - these need review*")
    
    anomalies = load_fake_leads_anomalies()
    
    if anomalies.empty:
        st.success("✅ No leads to review.")
        return
    
    # Filter to actual issues (high fraud but recommended to accept)
    review_needed = anomalies[
        (anomalies['fraud_score'] >= 7) & 
        (anomalies['validation_recommendation'] == 'accept')
    ]
    
    if review_needed.empty:
        st.success("✅ All high-risk leads were properly flagged for rejection.")
        return
    
    st.warning(f"⚠️ **{len(review_needed)} leads need review** - High fraud scores but recommended to accept")
    
    # Show the problematic leads in a simple table
    display_data = review_needed[['lead_id', 'first_name', 'last_name', 'company', 'lead_source', 'fraud_score', 'fraud_factors']].copy()
    display_data['name'] = display_data['first_name'] + ' ' + display_data['last_name']
    display_data = display_data[['lead_id', 'name', 'company', 'lead_source', 'fraud_score', 'fraud_factors']]
    display_data['fraud_factors'] = display_data['fraud_factors'].str[:50] + '...'  # Truncate for readability
    display_data.columns = ['Lead ID', 'Name', 'Company', 'Source', 'Fraud Score', 'Why High Risk']
    
    st.dataframe(
        display_data,
        use_container_width=True,
        hide_index=True,
        height=250
    )
    
    if len(review_needed) > 0:
        st.error("🚨 **Action Required:** These leads have high fraud scores (7+/10) but were recommended to accept. Review to ensure they're legitimate.")


def show_alerts_and_actions():
    """Show actionable alerts and recommendations."""
    st.markdown('<div class="section-header">🚨 Daily Alerts & Recommended Actions</div>', unsafe_allow_html=True)
    
    data = load_daily_fake_leads()
    
    if data.empty:
        st.success("✅ No alerts today - all sources are performing well!")
        return
    
    # Critical alerts
    critical_sources = data[data['daily_risk_level'] == 'CRITICAL']
    high_sources = data[data['daily_risk_level'] == 'HIGH'] 
    medium_sources = data[data['daily_risk_level'] == 'MEDIUM']
    
    if not critical_sources.empty:
        st.markdown('<div class="alert-critical">🚨 <strong>CRITICAL ALERTS - Immediate Action Required</strong></div>', unsafe_allow_html=True)
        for _, source in critical_sources.iterrows():
            st.error(f"🔴 **{source['lead_source']}**: {source['fake_leads_count']} fake leads ({source['fake_leads_percentage']:.1f}%) - **PAUSE THIS SOURCE**")
    
    if not high_sources.empty:
        st.markdown('<div class="alert-warning">⚠️ <strong>HIGH RISK ALERTS - Review Required</strong></div>', unsafe_allow_html=True)
        for _, source in high_sources.iterrows():
            st.warning(f"🟠 **{source['lead_source']}**: {source['fake_leads_count']} fake leads ({source['fake_leads_percentage']:.1f}%) - **INVESTIGATE IMMEDIATELY**")
    
    if not medium_sources.empty:
        st.markdown('<div class="alert-warning">🔍 <strong>MONITOR CLOSELY</strong></div>', unsafe_allow_html=True)
        for _, source in medium_sources.iterrows():
            st.info(f"🟡 **{source['lead_source']}**: {source['fake_leads_count']} fake leads ({source['fake_leads_percentage']:.1f}%) - **TRACK PATTERNS**")


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

def generate_pdf_report():
    """Generate PDF version of the daily fake leads report using fpdf2."""
    try:
        from fpdf import FPDF
        
        # Get all the data for the report
        data = load_daily_fake_leads()
        problematic_leads = load_daily_fake_leads_detail()
        
        # Calculate summary metrics
        total_leads_today = data['total_leads_today'].sum() if not data.empty else 0
        total_fake_leads = data['fake_leads_count'].sum() if not data.empty else 0
        total_fake_percentage = (total_fake_leads / total_leads_today * 100) if total_leads_today > 0 else 0
        total_high_risk = data['critical_fraud_count'].sum() if not data.empty and 'critical_fraud_count' in data.columns else 0
        total_high_risk_percentage = (total_high_risk / total_leads_today * 100) if total_leads_today > 0 else 0
        
        # Get timestamps
        current_date = datetime.now().strftime("%A, %B %d, %Y")
        last_refresh = get_last_refresh_time()
        
        # Create PDF
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font('Arial', 'B', 16)
        
        # Header (ASCII-safe)
        pdf.cell(0, 10, 'Daily Fake Leads Report', 0, 1, 'C')
        pdf.set_font('Arial', '', 12)
        pdf.cell(0, 8, current_date, 0, 1, 'C')
        pdf.cell(0, 8, f'Last Updated: {last_refresh}', 0, 1, 'C')
        pdf.ln(10)
        
        # Summary Section
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 10, "Today's Summary", 0, 1)
        pdf.set_font('Arial', '', 11)
        
        # Summary metrics
        pdf.cell(0, 8, f"Leads Validated Today: {total_leads_today:,}", 0, 1)
        pdf.cell(0, 8, f"Fake Leads Today: {total_fake_leads} ({total_fake_percentage:.1f}%)", 0, 1)
        pdf.cell(0, 8, f"High Risk Leads Today: {total_high_risk} ({total_high_risk_percentage:.1f}%)", 0, 1)
        pdf.ln(10)
        
        # Source breakdown table
        if not data.empty:
            pdf.set_font('Arial', 'B', 14)
            pdf.cell(0, 10, 'Fake Leads by Lead Source', 0, 1)
            pdf.set_font('Arial', 'B', 9)
            
            # Table headers
            col_widths = [50, 25, 25, 25, 20, 25]
            headers = ['Lead Source', 'Total', 'Fake', 'High Risk', 'Fake %', 'Risk Level']
            
            for i, header in enumerate(headers):
                pdf.cell(col_widths[i], 8, header, 1, 0, 'C')
            pdf.ln()
            
            # Table data
            pdf.set_font('Arial', '', 8)
            sorted_data = data.sort_values(['fake_leads_count', 'critical_fraud_count'], ascending=[False, False])
            
            for _, row in sorted_data.iterrows():
                critical_count = row.get('critical_fraud_count', 0)
                pdf.cell(col_widths[0], 6, str(row['lead_source'])[:25], 1, 0)
                pdf.cell(col_widths[1], 6, str(row['total_leads_today']), 1, 0, 'C')
                pdf.cell(col_widths[2], 6, str(row['fake_leads_count']), 1, 0, 'C')
                pdf.cell(col_widths[3], 6, str(critical_count), 1, 0, 'C')
                pdf.cell(col_widths[4], 6, f"{row['fake_leads_percentage']:.1f}%", 1, 0, 'C')
                pdf.cell(col_widths[5], 6, str(row['daily_risk_level']), 1, 0, 'C')
                pdf.ln()
            
            pdf.ln(10)
        
        # Detailed problematic leads
        if not problematic_leads.empty:
            pdf.set_font('Arial', 'B', 14)
            pdf.cell(0, 10, f'Problematic Leads Details ({len(problematic_leads)} leads)', 0, 1)
            pdf.set_font('Arial', '', 9)
            
            # Group by source
            for source in problematic_leads['lead_source'].unique():
                source_leads = problematic_leads[problematic_leads['lead_source'] == source]
                
                pdf.set_font('Arial', 'B', 11)
                pdf.cell(0, 8, f'{source} ({len(source_leads)} leads)', 0, 1)
                pdf.set_font('Arial', '', 9)
                
                for i, (_, lead) in enumerate(source_leads.iterrows(), 1):
                    type_indicator = "FAKE" if lead['lead_type'] == 'FAKE' else "HIGH RISK"
                    
                    # Lead info (using ASCII-safe characters)
                    pdf.cell(0, 6, f"{i}. {type_indicator}: {lead['first_name']} {lead['last_name']} (ID: {lead['lead_id']})", 0, 1)
                    pdf.cell(0, 5, f"   Email: {lead['email']}", 0, 1)
                    pdf.cell(0, 5, f"   Phone: {lead['phone'] if lead['phone'] else 'Missing'}", 0, 1)
                    pdf.cell(0, 5, f"   Company: {lead['company'] if lead['company'] else 'Missing'}", 0, 1)
                    pdf.cell(0, 5, f"   Fraud Score: {lead['fraud_score']}/10 | Action: {lead['recommendation'].upper()}", 0, 1)
                    
                    # Fraud factors (truncated for space, ASCII-safe)
                    fraud_factors = str(lead['fraud_factors'])[:80]
                    if len(str(lead['fraud_factors'])) > 80:
                        fraud_factors += '...'
                    # Remove any problematic Unicode characters
                    fraud_factors = fraud_factors.encode('ascii', 'ignore').decode('ascii')
                    pdf.cell(0, 5, f"   Fraud Factors: {fraud_factors}", 0, 1)
                    
                    pdf.ln(3)
                
                pdf.ln(5)
        
        # Footer
        pdf.ln(10)
        pdf.set_font('Arial', 'I', 8)
        pdf.cell(0, 5, 'Generated by Lead Validation System - Daily Fake Leads Monitoring Report', 0, 1, 'C')
        
        # Return PDF as bytes
        pdf_output = pdf.output(dest='S')
        # Handle different fpdf2 return types
        if isinstance(pdf_output, bytearray):
            return bytes(pdf_output)
        elif isinstance(pdf_output, bytes):
            return pdf_output
        else:
            return pdf_output.encode('latin-1')
        
    except ImportError:
        st.error("PDF generation requires fpdf2. Please install: pip install fpdf2")
        return None
    except Exception as e:
        st.error(f"Error generating PDF: {e}")
        return None

def generate_html_report():
    """Generate HTML version of the daily fake leads report."""
    try:
        # Get all the data for the report
        data = load_daily_fake_leads()
        problematic_leads = load_daily_fake_leads_detail()
        
        # Calculate summary metrics
        total_leads_today = data['total_leads_today'].sum() if not data.empty else 0
        total_fake_leads = data['fake_leads_count'].sum() if not data.empty else 0
        total_fake_percentage = (total_fake_leads / total_leads_today * 100) if total_leads_today > 0 else 0
        total_high_risk = data['critical_fraud_count'].sum() if not data.empty and 'critical_fraud_count' in data.columns else 0
        total_high_risk_percentage = (total_high_risk / total_leads_today * 100) if total_leads_today > 0 else 0
        
        # Get timestamps
        current_date = datetime.now().strftime("%A, %B %d, %Y")
        last_refresh = get_last_refresh_time()
        generation_time = datetime.now().strftime("%I:%M %p on %B %d, %Y")
        
        # Build HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Daily Fake Leads Report - {current_date}</title>
            <style>
                body {{ 
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                    margin: 0; 
                    padding: 20px; 
                    background-color: #f5f5f5; 
                    color: #333;
                }}
                .container {{ 
                    max-width: 1200px; 
                    margin: 0 auto; 
                    background: white; 
                    padding: 30px; 
                    border-radius: 8px; 
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}
                .header {{ 
                    text-align: center; 
                    margin-bottom: 30px; 
                    border-bottom: 3px solid #d32f2f; 
                    padding-bottom: 20px; 
                }}
                .title {{ 
                    font-size: 28px; 
                    color: #d32f2f; 
                    font-weight: bold; 
                    margin-bottom: 10px;
                }}
                .subtitle {{ 
                    font-size: 14px; 
                    color: #666; 
                    margin: 5px 0;
                }}
                .metrics {{ 
                    display: grid; 
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                    gap: 20px; 
                    margin: 30px 0; 
                }}
                .metric {{ 
                    text-align: center; 
                    padding: 20px; 
                    border: 1px solid #ddd; 
                    border-radius: 8px; 
                    background: #fafafa;
                }}
                .metric-value {{ 
                    font-size: 24px; 
                    font-weight: bold; 
                    color: #1976d2; 
                    display: block;
                }}
                .metric-label {{ 
                    font-size: 12px; 
                    color: #666; 
                    margin-top: 5px;
                }}
                .section {{ 
                    margin: 30px 0; 
                }}
                .section-title {{ 
                    font-size: 18px; 
                    font-weight: bold; 
                    color: #1976d2; 
                    border-bottom: 2px solid #1976d2; 
                    padding-bottom: 8px; 
                    margin-bottom: 15px;
                }}
                table {{ 
                    width: 100%; 
                    border-collapse: collapse; 
                    margin: 15px 0; 
                    font-size: 14px;
                }}
                th, td {{ 
                    border: 1px solid #ddd; 
                    padding: 12px 8px; 
                    text-align: left; 
                }}
                th {{ 
                    background-color: #f8f9fa; 
                    font-weight: bold; 
                    color: #495057;
                }}
                .risk-critical {{ background-color: #ffcdd2; }}
                .risk-high {{ background-color: #ffe0b2; }}
                .risk-medium {{ background-color: #fff3e0; }}
                .risk-low {{ background-color: #f3e5f5; }}
                .risk-clean {{ background-color: #e8f5e8; }}
                .lead-card {{ 
                    border: 1px solid #ddd; 
                    padding: 15px; 
                    margin: 15px 0; 
                    border-radius: 6px; 
                    background-color: #fafafa;
                }}
                .fake-lead {{ 
                    border-left: 5px solid #f44336; 
                    background-color: #ffebee; 
                }}
                .high-risk-lead {{ 
                    border-left: 5px solid #ff9800; 
                    background-color: #fff8e1; 
                }}
                .footer {{ 
                    text-align: center; 
                    margin-top: 40px; 
                    font-size: 12px; 
                    color: #666; 
                    border-top: 1px solid #eee; 
                    padding-top: 20px;
                }}
                @media print {{
                    body {{ background-color: white; }}
                    .container {{ box-shadow: none; }}
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <div class="title">🚨 Daily Fake Leads Report</div>
                    <div class="subtitle">{current_date}</div>
                    <div class="subtitle">Last Updated: {last_refresh}</div>
                    <div class="subtitle">Report Generated: {generation_time}</div>
                </div>
                
                <div class="section">
                    <div class="section-title">📊 Today's Summary</div>
                    <div class="metrics">
                        <div class="metric">
                            <span class="metric-value">{total_leads_today:,}</span>
                            <div class="metric-label">Leads Validated Today</div>
                        </div>
                        <div class="metric">
                            <span class="metric-value">{total_fake_leads}</span>
                            <div class="metric-label">Fake Leads ({total_fake_percentage:.1f}%)</div>
                        </div>
                        <div class="metric">
                            <span class="metric-value">{total_high_risk}</span>
                            <div class="metric-label">High Risk Leads ({total_high_risk_percentage:.1f}%)</div>
                        </div>
                    </div>
                </div>
        """
        
        # Add source breakdown table
        if not data.empty:
            html_content += f"""
                <div class="section">
                    <div class="section-title">📊 Fake Leads by Lead Source</div>
                    <table>
                        <thead>
                            <tr>
                                <th>Lead Source</th>
                                <th>Total Leads</th>
                                <th>Fake Leads</th>
                                <th>High Risk</th>
                                <th>Fake %</th>
                                <th>Risk Level</th>
                            </tr>
                        </thead>
                        <tbody>
            """
            
            # Sort data for HTML
            sorted_data = data.sort_values(['fake_leads_count', 'critical_fraud_count'], ascending=[False, False])
            
            for _, row in sorted_data.iterrows():
                risk_class = f"risk-{row['daily_risk_level'].lower()}"
                critical_count = row.get('critical_fraud_count', 0)
                html_content += f"""
                            <tr class="{risk_class}">
                                <td><strong>{row['lead_source']}</strong></td>
                                <td>{row['total_leads_today']}</td>
                                <td>{row['fake_leads_count']}</td>
                                <td>{critical_count}</td>
                                <td>{row['fake_leads_percentage']:.1f}%</td>
                                <td>{row['daily_risk_level']}</td>
                            </tr>
                """
            
            html_content += """
                        </tbody>
                    </table>
                </div>
            """
        
        # Add detailed lead analysis
        if not problematic_leads.empty:
            fake_count = len(problematic_leads[problematic_leads['lead_type'] == 'FAKE'])
            high_risk_count = len(problematic_leads[problematic_leads['lead_type'] == 'HIGH_RISK'])
            
            html_content += f"""
                <div class="section">
                    <div class="section-title">🔍 Problematic Leads Details</div>
                    <p><strong>Found {len(problematic_leads)} problematic leads today:</strong> {fake_count} fake leads + {high_risk_count} high risk leads</p>
            """
            
            # Group by source
            for source in problematic_leads['lead_source'].unique():
                source_leads = problematic_leads[problematic_leads['lead_source'] == source]
                source_fake_count = len(source_leads[source_leads['lead_type'] == 'FAKE'])
                source_high_risk_count = len(source_leads[source_leads['lead_type'] == 'HIGH_RISK'])
                
                html_content += f"<h3>{source} ({len(source_leads)} leads)</h3>"
                
                for i, (_, lead) in enumerate(source_leads.iterrows(), 1):
                    lead_class = "fake-lead" if lead['lead_type'] == 'FAKE' else "high-risk-lead"
                    type_indicator = "🚨 FAKE" if lead['lead_type'] == 'FAKE' else "⚠️ HIGH RISK"
                    
                    html_content += f"""
                    <div class="lead-card {lead_class}">
                        <h4>{type_indicator}: {lead['first_name']} {lead['last_name']} (ID: {lead['lead_id']})</h4>
                        <p><strong>Fraud Score:</strong> {lead['fraud_score']}/10 | <strong>Recommendation:</strong> {lead['recommendation'].upper()}</p>
                        <p><strong>Contact Info:</strong></p>
                        <ul>
                            <li><strong>Email:</strong> {lead['email']}</li>
                            <li><strong>Phone:</strong> {lead['phone'] if lead['phone'] else 'Missing'}</li>
                            <li><strong>Company:</strong> {lead['company'] if lead['company'] else 'Missing'}</li>
                        </ul>
                        <p><strong>Fraud Factors:</strong> {lead['fraud_factors']}</p>
                        <p><strong>Quality Issues:</strong> {lead['quality_factors']}</p>
                    </div>
                    """
            
            html_content += "</div>"
        
        # Close HTML
        html_content += f"""
                <div class="footer">
                    <p>Generated by Lead Validation System - Daily Fake Leads Monitoring Report</p>
                    <p>Report generated: {generation_time}</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html_content.encode('utf-8')
        
    except Exception as e:
        st.error(f"Error generating HTML: {e}")
        return None

def main():
    """Main daily fake leads report."""
    current_date = datetime.now().strftime("%A, %B %d, %Y")
    last_refresh = get_last_refresh_time()
    
    st.markdown(f'''
    <div class="main-header">
        🚨 Daily Fake Leads Report<br>
        <small>{current_date}</small><br>
        <small style="color: #666; font-size: 0.8rem;">Last Updated: {last_refresh}</small>
    </div>
    ''', unsafe_allow_html=True)
    
    # Inline refresh button
    if st.button("🔄 Refresh Data", help="Run ETL pipeline to sync latest data from Salesforce"):
        if run_etl_pipeline():
            st.rerun()
        else:
            st.error("Failed to refresh data. Please check ETL configuration.")
    
    # Main report sections
    show_daily_summary()
    st.markdown("---")
    
    show_fake_leads_by_source_table()
    st.markdown("---")
    
    show_fake_leads_detail()
    st.markdown("---")
    
    show_hourly_breakdown()
    
    # Footer with navigation
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📊 Go to Full Dashboard"):
            st.markdown("**Run:** `streamlit run src/dashboard/validation_dashboard.py`")
    
    with col2:
        if st.button("🎯 Go to Simplified Dashboard"):
            st.markdown("**Run:** `python run_simplified_dashboard.py`")
    
    # Report Download section
    st.markdown("---")
    st.markdown("### 📥 Export Options")
    
    col_export1, col_export2 = st.columns(2)
    
    with col_export1:
        if st.button("📄 Download PDF Report", help="Generate and download PDF version of this report", type="secondary", use_container_width=True):
            try:
                pdf_data = generate_pdf_report()
                if pdf_data:
                    st.download_button(
                        label="📥 Download PDF",
                        data=pdf_data,
                        file_name=f"daily_fake_leads_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
                else:
                    st.error("Failed to generate PDF report")
            except Exception as e:
                st.error(f"Error generating PDF: {e}")
    
    with col_export2:
        if st.button("🌐 Download HTML Report", help="Generate and download HTML version of this report", type="secondary", use_container_width=True):
            try:
                html_data = generate_html_report()
                if html_data:
                    st.download_button(
                        label="📥 Download HTML",
                        data=html_data,
                        file_name=f"daily_fake_leads_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                        mime="text/html",
                        use_container_width=True
                    )
                else:
                    st.error("Failed to generate HTML report")
            except Exception as e:
                st.error(f"Error generating HTML: {e}")
    


if __name__ == "__main__":
    main()
