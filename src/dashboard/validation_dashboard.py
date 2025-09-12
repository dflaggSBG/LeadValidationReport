"""Main Streamlit dashboard for Lead Validation Reporting."""
import streamlit as st
import sys
from pathlib import Path
import pandas as pd
import logging

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Local imports
from src.dashboard.data_loader import (
    load_validation_metrics, load_validation_summary, load_validation_by_source,
    load_validation_trends, load_recent_validations, load_problematic_leads,
    load_conversion_analysis, get_data_freshness, load_worst_lead_sources
)
from src.dashboard.components import (
    create_metric_cards, create_score_distribution_chart, create_validation_trends_chart,
    create_source_analysis_chart, create_conversion_analysis_chart, create_data_freshness_indicator,
    create_detailed_lead_table, create_score_histogram, create_worst_sources_table, 
    create_lead_source_quality_chart
)
from config.settings import DASHBOARD_TITLE

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title=DASHBOARD_TITLE,
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 2rem;
        color: #1f77b4;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        margin: 1rem 0;
        color: #2c3e50;
        border-bottom: 2px solid #3498db;
        padding-bottom: 0.5rem;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border-left: 4px solid #3498db;
    }
    .alert-success {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        border-radius: 0.25rem;
    }
    .alert-warning {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        border-radius: 0.25rem;
    }
    .alert-danger {
        background-color: #f8d7da;
        border: 1px solid #f1c6c7;
        color: #721c24;
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        border-radius: 0.25rem;
    }
</style>
""", unsafe_allow_html=True)


def main():
    """Main dashboard application."""
    st.markdown('<div class="main-header">📊 Lead Validation Dashboard</div>', unsafe_allow_html=True)
    
    # Sidebar controls
    create_sidebar()
    
    # Check data availability
    freshness_info = get_data_freshness()
    
    if freshness_info['total_leads'] == 0:
        st.error("""
        🚨 **No validation data available!**
        
        Please run the ETL pipeline first:
        ```bash
        python lead_validation_etl.py
        ```
        """)
        return
    
    # Data freshness indicator
    create_data_freshness_indicator(freshness_info)
    
    # Main dashboard tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🎯 Source Quality", "📈 Trends", "📋 Lead Details"])
    
    with tab1:
        show_overview_tab()
    
    with tab2:
        show_source_quality_tab()
    
    with tab3:
        show_trends_tab()
    
    with tab4:
        show_details_tab()


def create_sidebar():
    """Create sidebar with controls and filters."""
    st.sidebar.title("⚙️ Controls")
    
    # Data refresh button
    if st.sidebar.button("🔄 Refresh Data", type="primary"):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.markdown("---")
    
    # Filters section
    st.sidebar.subheader("📊 Filters")
    
    # Score threshold filter
    st.sidebar.slider(
        "Quality Score Threshold",
        min_value=0.0,
        max_value=1.0,
        value=0.6,
        step=0.1,
        key="score_threshold",
        help="Minimum quality score for analysis"
    )
    
    # Lead source filter
    source_data = load_validation_by_source()
    if not source_data.empty:
        all_sources = ['All'] + source_data['lead_source'].tolist()
        st.sidebar.selectbox(
            "Lead Source",
            options=all_sources,
            key="selected_source",
            help="Filter by specific lead source"
        )
    
    st.sidebar.markdown("---")
    
    # Quick actions
    st.sidebar.subheader("⚡ Quick Actions")
    
    if st.sidebar.button("📥 Export Problematic Leads"):
        problematic_leads = load_problematic_leads()
        if not problematic_leads.empty:
            csv = problematic_leads.to_csv(index=False)
            st.sidebar.download_button(
                label="Download CSV",
                data=csv,
                file_name="problematic_leads.csv",
                mime="text/csv"
            )
    
    if st.sidebar.button("📊 Generate Report"):
        st.sidebar.info("Report generation feature coming soon!")


def show_overview_tab():
    """Show overview dashboard tab."""
    st.markdown('<div class="section-header">Executive Summary</div>', unsafe_allow_html=True)
    
    # Load data
    metrics_data = load_validation_metrics()
    
    if metrics_data.empty:
        st.warning("No metrics data available")
        return
    
    metrics_row = metrics_data.iloc[0]
    
    # Key metrics cards
    create_metric_cards(metrics_row)
    
    # Two column layout for charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Score distribution chart
        st.plotly_chart(
            create_score_distribution_chart(metrics_row),
            use_container_width=True
        )
    
    with col2:
        # Score histogram
        summary_data = load_validation_summary()
        st.plotly_chart(
            create_score_histogram(summary_data),
            use_container_width=True
        )
    
    # System Health Status
    st.markdown('<div class="section-header">System Health Status</div>', unsafe_allow_html=True)
    
    # Health alerts based on overall system status
    health_status = metrics_row.get('overall_health_status', 'UNKNOWN')
    quality_percentage = metrics_row['quality_leads_percentage']
    fraud_percentage = metrics_row['fake_leads_percentage']
    
    if health_status == 'EXCELLENT':
        st.markdown('<div class="alert-success">🌟 <strong>Excellent:</strong> Lead quality system performing optimally!</div>', unsafe_allow_html=True)
    elif health_status == 'GOOD':
        st.markdown('<div class="alert-success">✅ <strong>Good:</strong> Lead quality is above target levels.</div>', unsafe_allow_html=True)
    elif health_status == 'FAIR':
        st.markdown('<div class="alert-warning">⚠️ <strong>Fair:</strong> Some lead quality issues detected - review recommended.</div>', unsafe_allow_html=True)
    elif health_status == 'POOR':
        st.markdown('<div class="alert-warning">🔶 <strong>Poor:</strong> Lead quality below acceptable levels - action needed.</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="alert-danger">🚨 <strong>Critical:</strong> Serious lead quality issues require immediate attention!</div>', unsafe_allow_html=True)
    
    # Key system alerts
    col1, col2, col3 = st.columns(3)
    with col1:
        if metrics_row.get('has_high_risk_sources', False):
            st.error("🔴 High-risk lead sources detected")
        else:
            st.success("✅ No high-risk sources")
    
    with col2:
        if metrics_row.get('high_fraud_alert', False):
            st.error(f"🚨 High fraud rate: {fraud_percentage:.1f}%")
        else:
            st.success(f"✅ Fraud rate acceptable: {fraud_percentage:.1f}%")
    
    with col3:
        if metrics_row.get('low_quality_alert', False):
            st.error(f"📉 Low quality score: {metrics_row['avg_quality_score']:.1f}")
        else:
            st.success(f"✅ Good quality score: {metrics_row['avg_quality_score']:.1f}")
    
    # Fraud analysis chart (replacing conversion analysis)
    st.markdown('<div class="section-header">Quality Score Distribution</div>', unsafe_allow_html=True)
    conversion_data = load_conversion_analysis()
    if not conversion_data.empty:
        # Update the chart title and labels to reflect fraud analysis
        st.markdown("*Note: 'Conversion Rate' in this chart represents Fraud Rate for analysis purposes*")
        st.plotly_chart(
            create_conversion_analysis_chart(conversion_data),
            use_container_width=True
        )


def show_source_quality_tab():
    """Show lead source quality analysis tab - the main focus of the dashboard."""
    st.markdown('<div class="section-header">Lead Source Quality Analysis</div>', unsafe_allow_html=True)
    
    # Load source quality data
    source_data = load_validation_by_source()
    worst_sources_data = load_worst_lead_sources()
    
    if source_data.empty:
        st.warning("No source quality data available")
        return
    
    # Executive Summary Cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_sources = len(source_data)
        st.metric(
            label="📊 Total Sources",
            value=total_sources,
            delta=None
        )
    
    with col2:
        high_quality_sources = len(source_data[source_data['quality_grade'].isin(['A+', 'A'])])
        high_quality_pct = (high_quality_sources / total_sources) * 100 if total_sources > 0 else 0
        st.metric(
            label="🌟 High Quality Sources",
            value=f"{high_quality_sources} ({high_quality_pct:.0f}%)",
            delta=None
        )
    
    with col3:
        problem_sources = len(worst_sources_data)
        st.metric(
            label="🚨 Sources Need Attention",
            value=problem_sources,
            delta=None,
            delta_color="inverse" if problem_sources > 0 else "normal"
        )
    
    with col4:
        if not source_data.empty:
            avg_quality = source_data['avg_quality_score'].mean()
            st.metric(
                label="📈 Avg Quality Score",
                value=f"{avg_quality:.1f}",
                delta=None,
                delta_color="normal" if avg_quality >= 7 else "inverse"
            )
    
    # Main visualization: Quality Matrix
    st.markdown('<div class="section-header">Lead Source Quality Matrix</div>', unsafe_allow_html=True)
    
    if len(source_data) > 0:
        quality_matrix = create_lead_source_quality_chart(source_data)
        st.plotly_chart(quality_matrix, use_container_width=True)
        
        st.markdown("""
        **How to read this chart:**
        - 🟢 **Bottom Right (Green)**: High quality, low fraud - Excellent sources
        - 🟡 **Bottom Left (Yellow)**: Low quality, low fraud - Needs improvement  
        - 🟠 **Top Right (Orange)**: High quality, high fraud - Investigate patterns
        - 🔴 **Top Left (Red)**: Low quality, high fraud - Critical action needed
        - **Bubble Size**: Represents lead volume from each source
        """)
    
    # Two column layout for detailed analysis
    col_left, col_right = st.columns([1.2, 0.8])
    
    with col_left:
        # Best and worst performing sources
        st.markdown('<div class="section-header">Source Performance Ranking</div>', unsafe_allow_html=True)
        
        # Top 10 best sources
        with st.expander("🌟 Top Performing Sources", expanded=True):
            best_sources = source_data.head(10)
            
            for idx, (_, source) in enumerate(best_sources.iterrows()):
                col_rank, col_name, col_score, col_grade, col_volume = st.columns([0.5, 3, 1, 0.5, 1])
                
                with col_rank:
                    st.write(f"#{idx+1}")
                with col_name:
                    st.write(f"**{source['lead_source']}**")
                with col_score:
                    st.write(f"{source['avg_quality_score']:.1f}")
                with col_grade:
                    grade_color = {"A+": "🟢", "A": "🟢", "B": "🟡", "C": "🟡", "D": "🔴", "F": "🔴"}
                    st.write(f"{grade_color.get(source['quality_grade'], '⚪')} {source['quality_grade']}")
                with col_volume:
                    st.write(f"{source['total_leads']:,}")
    
    with col_right:
        # Problem sources requiring attention
        create_worst_sources_table(worst_sources_data)
    
    # Detailed source analysis table
    st.markdown('<div class="section-header">Complete Source Analysis</div>', unsafe_allow_html=True)
    
    # Filters for the detailed table
    col1, col2, col3 = st.columns(3)
    
    with col1:
        grade_filter = st.selectbox(
            "Filter by Grade",
            options=["All"] + sorted(source_data['quality_grade'].unique().tolist()),
            help="Filter sources by quality grade"
        )
    
    with col2:
        min_volume = st.number_input(
            "Min Lead Volume",
            min_value=0,
            max_value=int(source_data['total_leads'].max()) if not source_data.empty else 100,
            value=0,
            help="Show only sources with at least this many leads"
        )
    
    with col3:
        sort_by = st.selectbox(
            "Sort by",
            options=["Quality Score", "Total Leads", "Quality Grade", "Fraud Rate"],
            help="Sort the table by this column"
        )
    
    # Apply filters
    filtered_data = source_data.copy()
    
    if grade_filter != "All":
        filtered_data = filtered_data[filtered_data['quality_grade'] == grade_filter]
    
    if min_volume > 0:
        filtered_data = filtered_data[filtered_data['total_leads'] >= min_volume]
    
    # Apply sorting
    sort_mapping = {
        "Quality Score": "avg_quality_score",
        "Total Leads": "total_leads", 
        "Quality Grade": "quality_rank",
        "Fraud Rate": "fake_leads_percentage"
    }
    
    if sort_by in sort_mapping:
        ascending = sort_by == "Fraud Rate"  # Fraud rate: higher is worse
        filtered_data = filtered_data.sort_values(sort_mapping[sort_by], ascending=ascending)
    
    # Display the filtered table
    if not filtered_data.empty:
        # Select key columns for display
        display_columns = [
            'lead_source', 'total_leads', 'avg_quality_score', 'quality_grade',
            'fake_leads_percentage', 'email_valid_percentage', 'phone_valid_percentage',
            'quality_leads_percentage', 'risk_level'
        ]
        
        display_columns = [col for col in display_columns if col in filtered_data.columns]
        display_data = filtered_data[display_columns].copy()
        
        # Rename columns for better display
        column_renames = {
            'lead_source': 'Lead Source',
            'total_leads': 'Total Leads',
            'avg_quality_score': 'Quality Score', 
            'quality_grade': 'Grade',
            'fake_leads_percentage': 'Fraud Rate %',
            'email_valid_percentage': 'Email Valid %',
            'phone_valid_percentage': 'Phone Valid %',
            'quality_leads_percentage': 'Quality Leads %',
            'risk_level': 'Risk Level'
        }
        
        display_data = display_data.rename(columns=column_renames)
        
        # Format numeric columns
        numeric_columns = ['Quality Score', 'Fraud Rate %', 'Email Valid %', 'Phone Valid %', 'Quality Leads %']
        for col in numeric_columns:
            if col in display_data.columns:
                display_data[col] = display_data[col].round(1)
        
        st.dataframe(
            display_data,
            use_container_width=True,
            height=400
        )
        
        # Export functionality
        if st.button("📥 Export Source Analysis"):
            csv = display_data.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"lead_source_analysis_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
    else:
        st.info("No sources match the selected filters.")


def show_trends_tab():
    """Show trends analysis tab."""
    st.markdown('<div class="section-header">Validation Trends</div>', unsafe_allow_html=True)
    
    # Load trends data
    trends_data = load_validation_trends()
    
    if trends_data.empty:
        st.warning("No trends data available")
        return
    
    # Trends chart
    st.plotly_chart(
        create_validation_trends_chart(trends_data),
        use_container_width=True
    )
    
    # Trends summary table
    st.markdown('<div class="section-header">Recent Trends Summary</div>', unsafe_allow_html=True)
    
    # Show last 7 days of data
    recent_trends = trends_data.head(7)
    
    if not recent_trends.empty:
        # Format for display
        display_trends = recent_trends.copy()
        display_trends['period_start'] = pd.to_datetime(display_trends['period_start']).dt.strftime('%Y-%m-%d')
        display_trends['avg_score'] = display_trends['avg_score'].round(3)
        
        st.dataframe(
            display_trends[['period_start', 'leads_validated', 'avg_score', 'quality_percentage', 'conversion_rate']],
            use_container_width=True
        )
    
    # Trend insights
    if len(trends_data) >= 2:
        latest = trends_data.iloc[0]
        previous = trends_data.iloc[1]
        
        score_change = latest['avg_score'] - previous['avg_score']
        volume_change = latest['leads_validated'] - previous['leads_validated']
        
        st.markdown('<div class="section-header">Trend Insights</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            if score_change > 0:
                st.success(f"📈 Quality score improved by {score_change:.3f}")
            elif score_change < 0:
                st.error(f"📉 Quality score declined by {abs(score_change):.3f}")
            else:
                st.info("➡️ Quality score remained stable")
        
        with col2:
            if volume_change > 0:
                st.info(f"📊 Lead volume increased by {volume_change:,}")
            elif volume_change < 0:
                st.info(f"📊 Lead volume decreased by {abs(volume_change):,}")
            else:
                st.info("➡️ Lead volume remained stable")




def show_details_tab():
    """Show lead details tab."""
    st.markdown('<div class="section-header">Lead Details</div>', unsafe_allow_html=True)
    
    # Filter options
    col1, col2, col3 = st.columns(3)
    
    with col1:
        detail_view = st.selectbox(
            "View",
            options=["Recent Validations", "Problematic Leads", "All Leads"],
            help="Choose which leads to display"
        )
    
    with col2:
        limit = st.number_input(
            "Number of Records",
            min_value=10,
            max_value=500,
            value=100,
            step=10,
            help="Maximum number of records to display"
        )
    
    with col3:
        if st.button("🔄 Refresh Details"):
            st.cache_data.clear()
            st.rerun()
    
    # Load and display data based on selection
    if detail_view == "Recent Validations":
        leads_data = load_recent_validations(limit=limit)
        st.markdown(f"**Showing {len(leads_data)} most recent validations**")
    
    elif detail_view == "Problematic Leads":
        score_threshold = st.session_state.get('score_threshold', 0.6)
        leads_data = load_problematic_leads(score_threshold=score_threshold)
        if not leads_data.empty:
            leads_data = leads_data.head(limit)
        st.markdown(f"**Showing leads with quality score < {score_threshold}**")
    
    else:  # All Leads
        leads_data = load_validation_summary().head(limit)
        st.markdown(f"**Showing all leads (limited to {limit} records)**")
    
    # Display the table
    if not leads_data.empty:
        create_detailed_lead_table(leads_data)
        
        # Export option
        if st.button("📥 Export to CSV"):
            csv = leads_data.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"lead_validation_export_{detail_view.lower().replace(' ', '_')}.csv",
                mime="text/csv"
            )
    else:
        st.info("No leads found matching the selected criteria.")


@st.cache_data(ttl=300)  # Cache for 5 minutes
def cached_data_loader(loader_func, *args, **kwargs):
    """Cached wrapper for data loading functions."""
    return loader_func(*args, **kwargs)


if __name__ == "__main__":
    main()
