"""Daily Fake Leads by Source Report - Focused monitoring dashboard."""
import streamlit as st
import sys
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
    """Load detailed list of today's fake leads."""
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
            created_date,
            parsed_at
        FROM parsed_validations
        WHERE parse_error IS NULL
        AND DATE_TRUNC('day', created_date) = CURRENT_DATE
        AND COALESCE(api_fake_lead, false) = true
        ORDER BY COALESCE(api_fraud_score, 0) DESC, lead_source, created_date DESC
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
    
    # Key metrics cards
    col1, col2 = st.columns(2)
    
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


def show_fake_leads_by_source_table():
    """Show a clean table of fake leads count by lead source."""
    st.markdown('<div class="section-header">📊 Fake Leads by Lead Source</div>', unsafe_allow_html=True)
    
    data = load_daily_fake_leads()
    
    if data.empty:
        st.info("No data available for today.")
        return
    
    # Create focused table
    table_data = data[['lead_source', 'total_leads_today', 'fake_leads_count', 'fake_leads_percentage', 'daily_risk_level']].copy()
    
    # Sort by fake leads count (highest first), then by percentage
    table_data = table_data.sort_values(['fake_leads_count', 'fake_leads_percentage'], ascending=[False, False])
    
    # Format the data
    table_data['fake_leads_percentage'] = table_data['fake_leads_percentage'].round(1)
    
    # Rename columns for clean display
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
    """Show detailed list of today's fake leads."""
    st.markdown('<div class="section-header">🔍 Today\'s Fake Leads - Detailed Analysis</div>', unsafe_allow_html=True)
    
    fake_leads = load_daily_fake_leads_detail()
    
    if fake_leads.empty:
        st.success("🎉 No fake leads detected today!")
        return
    
    st.markdown(f"**Found {len(fake_leads)} fake leads created today:**")
    
    # Group by source for better organization
    for source in fake_leads['lead_source'].unique():
        source_fakes = fake_leads[fake_leads['lead_source'] == source]
        
        with st.expander(f"🚨 {source} - {len(source_fakes)} fake leads", expanded=True):
            for _, lead in source_fakes.iterrows():
                st.markdown(f"""
                <div class="fake-lead-card">
                    <strong>{lead['first_name']} {lead['last_name']}</strong> (ID: {lead['lead_id']}) | Fraud: {lead['fraud_score']}/10 | Action: {lead['recommendation'].upper()}<br>
                    Email: {lead['email']} | Phone: {lead['phone'] if lead['phone'] else 'Missing'} | Company: {lead['company'] if lead['company'] else 'Missing'}<br>
                    <strong>Fraud Factors:</strong> {lead['fraud_factors']}<br>
                    <strong>Quality Issues:</strong> {lead['quality_factors']}
                </div>
                """, unsafe_allow_html=True)


def show_hourly_breakdown():
    """Show hourly breakdown of today's fake leads by source."""
    st.markdown('<div class="section-header">⏰ Hourly Fake Lead Pattern by Source (Today)</div>', unsafe_allow_html=True)
    
    try:
        conn = get_database_connection()
        
        # Get hourly data with source breakdown
        query = """
        SELECT 
            EXTRACT(hour FROM created_date) as hour_of_day,
            COALESCE(lead_source, 'Unknown') as lead_source,
            COUNT(*) as total_leads,
            COUNTIF(COALESCE(api_fake_lead, false) = true) as fake_leads,
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
            # Fake leads only by hour and source
            fake_only = hourly_source_data[hourly_source_data['fake_leads'] > 0]
            
            if not fake_only.empty:
                fig2 = px.bar(
                    fake_only,
                    x='hour_of_day',
                    y='fake_leads',
                    color='lead_source',
                    title="Fake Leads by Hour & Source",
                    labels={'hour_of_day': 'Hour', 'fake_leads': 'Fake Leads'}
                )
                fig2.update_layout(height=300, margin=dict(t=40, b=40, l=40, r=40))
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.success("✅ No fake leads detected at any hour today")
        
        # Show hourly source breakdown table for fake leads only
        if not fake_only.empty:
            st.markdown('<div class="sub-header">📊 Fake Leads by Hour & Source</div>', unsafe_allow_html=True)
            
            # Pivot table for easier reading
            pivot_data = fake_only.pivot(index='lead_source', columns='hour_of_day', values='fake_leads').fillna(0)
            
            # Only show hours that have fake leads
            active_hours = fake_only['hour_of_day'].unique()
            pivot_data = pivot_data[sorted(active_hours)]
            
            # Add row totals
            pivot_data['Total'] = pivot_data.sum(axis=1)
            
            # Sort by total fake leads
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
    


if __name__ == "__main__":
    main()
