"""Dashboard components for Lead Validation Reporting."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from typing import Dict, Any, Optional
import numpy as np


def create_metric_cards(metrics_row: pd.Series):
    """Create metric cards for the dashboard header."""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📊 Total Validations",
            value=f"{metrics_row['total_validations']:,}",
            delta=None
        )
    
    with col2:
        avg_score = metrics_row['avg_quality_score']
        st.metric(
            label="⭐ Avg Quality Score",
            value=f"{avg_score:.1f}",
            delta=None,
            delta_color="normal" if avg_score >= 7 else "inverse"
        )
    
    with col3:
        quality_percentage = metrics_row['quality_leads_percentage']
        st.metric(
            label="✅ Quality Leads",
            value=f"{quality_percentage:.1f}%",
            delta=None,
            delta_color="normal" if quality_percentage >= 70 else "inverse"
        )
    
    with col4:
        fake_percentage = metrics_row['fake_leads_percentage']
        st.metric(
            label="🚨 Fraud Rate",
            value=f"{fake_percentage:.1f}%",
            delta=None,
            delta_color="inverse" if fake_percentage > 10 else "normal"
        )


def create_score_distribution_chart(metrics_row: pd.Series) -> go.Figure:
    """Create score distribution donut chart."""
    categories = ['Excellent (9-10)', 'Good (7-8)', 'Fair (5-6)', 'Poor (3-4)', 'Invalid (0-2)']
    values = [
        metrics_row['excellent_leads'],
        metrics_row['good_leads'],
        metrics_row['fair_leads'],
        metrics_row['poor_leads'],
        metrics_row['invalid_leads']
    ]
    percentages = [
        metrics_row['excellent_percentage'],
        metrics_row['good_percentage'],
        metrics_row['fair_percentage'],
        metrics_row['poor_percentage'],
        metrics_row['invalid_percentage']
    ]
    
    colors = ['#00CC96', '#00D4AA', '#FFA15A', '#EF553B', '#B91C1C']
    
    fig = go.Figure(data=[go.Pie(
        labels=categories,
        values=values,
        hole=0.4,
        marker_colors=colors,
        textinfo='label+percent',
        hovertemplate='<b>%{label}</b><br>' +
                      'Count: %{value:,}<br>' +
                      'Percentage: %{percent}<br>' +
                      '<extra></extra>'
    )])
    
    fig.update_layout(
        title="Lead Quality Score Distribution",
        title_x=0.5,
        showlegend=True,
        height=400,
        margin=dict(t=50, b=0, l=0, r=0)
    )
    
    return fig


def create_validation_trends_chart(trends_df: pd.DataFrame) -> go.Figure:
    """Create validation trends line chart."""
    if trends_df.empty:
        return go.Figure().add_annotation(
            text="No trend data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    # Convert period_start to datetime
    trends_df['period_start'] = pd.to_datetime(trends_df['period_start'])
    trends_df = trends_df.sort_values('period_start')
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=['Average Quality Score Over Time', 'Lead Volume Over Time'],
        vertical_spacing=0.15,
        specs=[[{"secondary_y": False}], [{"secondary_y": True}]]
    )
    
    # Add average score line
    fig.add_trace(
        go.Scatter(
            x=trends_df['period_start'],
            y=trends_df['avg_score'],
            mode='lines+markers',
            name='Avg Score',
            line=dict(color='#00CC96', width=3),
            marker=dict(size=6)
        ),
        row=1, col=1
    )
    
    # Add lead volume bars
    fig.add_trace(
        go.Bar(
            x=trends_df['period_start'],
            y=trends_df['leads_validated'],
            name='Lead Volume',
            marker_color='#636EFA',
            opacity=0.7
        ),
        row=2, col=1
    )
    
    # Add quality percentage line on secondary y-axis
    fig.add_trace(
        go.Scatter(
            x=trends_df['period_start'],
            y=trends_df['quality_percentage'],
            mode='lines+markers',
            name='Quality %',
            line=dict(color='#FF6692', width=2, dash='dash'),
            marker=dict(size=4),
            yaxis='y4'
        ),
        row=2, col=1
    )
    
    # Update layout
    fig.update_layout(
        height=500,
        title_text="Lead Validation Trends",
        title_x=0.5,
        showlegend=True,
        hovermode='x unified'
    )
    
    # Update axes
    fig.update_yaxes(title_text="Quality Score", row=1, col=1, range=[0, 1])
    fig.update_yaxes(title_text="Lead Count", row=2, col=1)
    fig.update_yaxes(title_text="Quality %", secondary_y=True, row=2, col=1)
    fig.update_xaxes(title_text="Date", row=2, col=1)
    
    return fig


def create_source_analysis_chart(source_df: pd.DataFrame) -> go.Figure:
    """Create lead source analysis chart."""
    if source_df.empty:
        return go.Figure().add_annotation(
            text="No source data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    # Limit to top 15 sources and sort by quality score
    source_df = source_df.head(15).sort_values('avg_quality_score', ascending=True)  # Ascending for horizontal bar
    
    # Create color scale based on quality grade
    grade_colors = {
        'A+': '#00CC96',  # Excellent green
        'A': '#00D4AA',   # Good green
        'B': '#FFA15A',   # Fair orange
        'C': '#FFBF3F',   # Warning yellow
        'D': '#EF553B',   # Poor red
        'F': '#B91C1C'    # Critical red
    }
    
    colors = [grade_colors.get(grade, '#636EFA') for grade in source_df['quality_grade']]
    
    # Create horizontal bar chart
    fig = go.Figure()
    
    # Add quality score bars
    fig.add_trace(go.Bar(
        y=source_df['lead_source'],
        x=source_df['avg_quality_score'],
        name='Avg Quality Score',
        orientation='h',
        marker=dict(
            color=colors,
            line=dict(color='rgba(50,50,50,0.5)', width=1)
        ),
        text=[f"{score:.1f} ({grade})" for score, grade in zip(source_df['avg_quality_score'], source_df['quality_grade'])],
        textposition='inside',
        hovertemplate='<b>%{y}</b><br>' +
                      'Quality Score: %{x:.1f}<br>' +
                      'Grade: %{customdata}<br>' +
                      '<extra></extra>',
        customdata=source_df['quality_grade']
    ))
    
    fig.update_layout(
        title="Lead Sources by Quality Score (Scale: 0-10)",
        title_x=0.5,
        height=max(400, len(source_df) * 25),  # Dynamic height based on sources
        xaxis_title="Average Quality Score",
        yaxis_title="Lead Source",
        showlegend=False,
        xaxis=dict(range=[0, 10]),
        font=dict(size=10)
    )
    
    # Add vertical lines for quality thresholds
    fig.add_vline(x=7, line_dash="dash", line_color="green", 
                  annotation_text="Good Threshold (7.0)", 
                  annotation_position="top")
    fig.add_vline(x=5, line_dash="dash", line_color="orange", 
                  annotation_text="Fair Threshold (5.0)",
                  annotation_position="top")
    
    return fig


def create_conversion_analysis_chart(conversion_df: pd.DataFrame) -> go.Figure:
    """Create conversion analysis by quality score."""
    if conversion_df.empty:
        return go.Figure().add_annotation(
            text="No conversion data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    # Create grouped bar chart
    fig = go.Figure()
    
    # Add total leads bars
    fig.add_trace(go.Bar(
        x=conversion_df['score_category'],
        y=conversion_df['total_leads'],
        name='Total Leads',
        marker_color='lightblue',
        opacity=0.7
    ))
    
    # Add converted leads bars
    fig.add_trace(go.Bar(
        x=conversion_df['score_category'],
        y=conversion_df['converted_leads'],
        name='Converted Leads',
        marker_color='darkgreen',
        opacity=0.8
    ))
    
    # Add conversion rate line on secondary y-axis
    fig.add_trace(go.Scatter(
        x=conversion_df['score_category'],
        y=conversion_df['conversion_rate'],
        mode='lines+markers+text',
        name='Conversion Rate %',
        yaxis='y2',
        line=dict(color='red', width=3),
        marker=dict(size=8, color='red'),
        text=[f"{rate:.1f}%" for rate in conversion_df['conversion_rate']],
        textposition='top center'
    ))
    
    # Update layout for dual y-axis
    fig.update_layout(
        title="Conversion Analysis by Lead Quality",
        title_x=0.5,
        height=400,
        xaxis_title="Quality Score Category",
        yaxis=dict(title="Number of Leads"),
        yaxis2=dict(
            title="Conversion Rate (%)",
            overlaying='y',
            side='right',
            range=[0, max(conversion_df['conversion_rate']) * 1.2]
        ),
        legend=dict(x=0.02, y=0.98),
        barmode='group'
    )
    
    return fig


def create_data_freshness_indicator(freshness_info: Dict[str, Any]):
    """Create data freshness indicator."""
    status = freshness_info['status']
    last_validation = freshness_info.get('last_validation')
    hours_since = freshness_info.get('hours_since_validation', 0)
    
    # Choose color based on freshness
    if status == "Fresh":
        color = "🟢"
    elif status == "Recent":
        color = "🟡"
    else:
        color = "🔴"
    
    if last_validation:
        time_text = f"Last validation: {hours_since:.1f} hours ago"
    else:
        time_text = "No validation data available"
    
    st.markdown(f"""
    <div style="
        border: 1px solid #ddd;
        border-radius: 5px;
        padding: 10px;
        margin: 10px 0;
        background-color: #f8f9fa;
    ">
        <strong>{color} Data Freshness: {status}</strong><br>
        <small>{time_text}</small>
    </div>
    """, unsafe_allow_html=True)


def create_detailed_lead_table(leads_df: pd.DataFrame):
    """Create detailed lead table with formatting."""
    if leads_df.empty:
        st.info("No lead data available")
        return
    
    # Format the dataframe for display
    display_df = leads_df.copy()
    
    # Format score column with color coding
    if 'overall_score' in display_df.columns:
        display_df['overall_score'] = display_df['overall_score'].round(3)
    
    # Format timestamp columns
    timestamp_columns = ['validation_timestamp', 'created_date', 'last_modified_date']
    for col in timestamp_columns:
        if col in display_df.columns:
            display_df[col] = pd.to_datetime(display_df[col]).dt.strftime('%Y-%m-%d %H:%M')
    
    # Reorder columns for better display
    preferred_order = [
        'lead_id', 'overall_score', 'validation_status', 
        'first_name', 'last_name', 'email', 'phone', 'company',
        'lead_source', 'is_converted', 'validation_timestamp'
    ]
    
    # Keep only columns that exist
    columns_to_show = [col for col in preferred_order if col in display_df.columns]
    remaining_columns = [col for col in display_df.columns if col not in columns_to_show]
    final_columns = columns_to_show + remaining_columns
    
    # Display the table
    st.dataframe(
        display_df[final_columns],
        use_container_width=True,
        height=400
    )


def create_worst_sources_table(worst_sources_df: pd.DataFrame):
    """Create a table showing worst performing lead sources."""
    if worst_sources_df.empty:
        st.info("🎉 No problematic lead sources found! All sources are performing well.")
        return
    
    st.markdown("### 🚨 Lead Sources Requiring Attention")
    
    # Format the dataframe for better display
    display_df = worst_sources_df.copy()
    
    # Select key columns for display
    columns_to_show = [
        'lead_source', 'total_leads', 'avg_quality_score', 'quality_grade', 
        'fake_leads_percentage', 'risk_level', 'recommendation', 'remediation_priority'
    ]
    
    # Filter to columns that exist
    columns_to_show = [col for col in columns_to_show if col in display_df.columns]
    display_df = display_df[columns_to_show]
    
    # Rename columns for better display
    column_renames = {
        'lead_source': 'Lead Source',
        'total_leads': 'Total Leads', 
        'avg_quality_score': 'Quality Score',
        'quality_grade': 'Grade',
        'fake_leads_percentage': 'Fraud Rate %',
        'risk_level': 'Risk Level',
        'recommendation': 'Action Required',
        'remediation_priority': 'Priority'
    }
    
    display_df = display_df.rename(columns=column_renames)
    
    # Format numeric columns
    if 'Quality Score' in display_df.columns:
        display_df['Quality Score'] = display_df['Quality Score'].round(1)
    if 'Fraud Rate %' in display_df.columns:
        display_df['Fraud Rate %'] = display_df['Fraud Rate %'].round(1)
    
    # Style the dataframe
    def style_row(row):
        if 'Risk Level' in row:
            if row['Risk Level'] == 'HIGH_RISK':
                return ['background-color: #ffebee'] * len(row)  # Light red
            elif row['Risk Level'] == 'MEDIUM_RISK':
                return ['background-color: #fff3e0'] * len(row)  # Light orange
        return [''] * len(row)
    
    # Display the table
    st.dataframe(
        display_df.head(10),  # Show top 10 worst sources
        use_container_width=True,
        height=300
    )
    
    if len(worst_sources_df) > 10:
        st.caption(f"Showing top 10 of {len(worst_sources_df)} sources needing attention")
    
    # Add summary insights
    if not worst_sources_df.empty:
        high_risk_count = len(worst_sources_df[worst_sources_df['risk_level'] == 'HIGH_RISK'])
        medium_risk_count = len(worst_sources_df[worst_sources_df['risk_level'] == 'MEDIUM_RISK'])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                label="🔴 High Risk Sources",
                value=high_risk_count,
                delta=None
            )
        with col2:
            st.metric(
                label="🟡 Medium Risk Sources", 
                value=medium_risk_count,
                delta=None
            )
        with col3:
            avg_problem_score = worst_sources_df['problem_score'].mean() if 'problem_score' in worst_sources_df.columns else 0
            st.metric(
                label="📊 Avg Problem Score",
                value=f"{avg_problem_score:.1f}",
                delta=None
            )


def create_lead_source_quality_chart(source_df: pd.DataFrame) -> go.Figure:
    """Create a comprehensive lead source quality analysis chart."""
    if source_df.empty:
        return go.Figure().add_annotation(
            text="No source quality data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    # Create bubble chart: Quality Score vs Fraud Rate, sized by volume
    fig = go.Figure()
    
    # Define color mapping for quality grades
    grade_colors = {
        'A+': '#00CC96',
        'A': '#00D4AA', 
        'B': '#36C9DD',
        'C': '#FFA15A',
        'D': '#EF553B',
        'F': '#B91C1C'
    }
    
    # Create scatter plot
    fig.add_trace(go.Scatter(
        x=source_df['avg_quality_score'],
        y=source_df['fake_leads_percentage'],
        mode='markers+text',
        text=source_df['lead_source'],
        textposition='middle center',
        marker=dict(
            size=np.sqrt(source_df['total_leads']) * 3,  # Size by volume (square root for better scaling)
            color=[grade_colors.get(grade, '#636EFA') for grade in source_df['quality_grade']],
            opacity=0.7,
            line=dict(width=2, color='white'),
            sizemode='diameter',
            sizemin=10
        ),
        hovertemplate='<b>%{text}</b><br>' +
                      'Quality Score: %{x:.1f}<br>' +
                      'Fraud Rate: %{y:.1f}%<br>' +
                      'Total Leads: %{customdata}<br>' +
                      'Grade: %{marker.color}<br>' +
                      '<extra></extra>',
        customdata=source_df['total_leads'],
        name='Lead Sources'
    ))
    
    # Update layout
    fig.update_layout(
        title="Lead Source Quality Matrix: Quality vs Fraud Risk",
        title_x=0.5,
        xaxis_title="Average Quality Score",
        yaxis_title="Fraud Rate (%)",
        height=500,
        showlegend=False
    )
    
    # Add quadrant lines
    fig.add_vline(x=7, line_dash="dash", line_color="green", opacity=0.5)
    fig.add_hline(y=10, line_dash="dash", line_color="red", opacity=0.5)
    
    # Add quadrant annotations
    fig.add_annotation(x=8.5, y=5, text="🌟 Excellent<br>(High Quality, Low Fraud)", 
                       showarrow=False, bgcolor="rgba(0,255,0,0.1)")
    fig.add_annotation(x=5, y=5, text="⚠️ Needs Work<br>(Low Quality, Low Fraud)", 
                       showarrow=False, bgcolor="rgba(255,255,0,0.1)")
    fig.add_annotation(x=8.5, y=20, text="🤔 Investigate<br>(High Quality, High Fraud)", 
                       showarrow=False, bgcolor="rgba(255,165,0,0.1)")
    fig.add_annotation(x=5, y=20, text="🚨 Critical<br>(Low Quality, High Fraud)", 
                       showarrow=False, bgcolor="rgba(255,0,0,0.1)")
    
    # Set axis ranges
    fig.update_xaxes(range=[0, 10])
    fig.update_yaxes(range=[0, max(source_df['fake_leads_percentage'].max() * 1.1, 25)])
    
    return fig


def create_score_histogram(summary_df: pd.DataFrame) -> go.Figure:
    """Create histogram of validation scores."""
    if summary_df.empty or 'overall_score' not in summary_df.columns:
        return go.Figure().add_annotation(
            text="No score data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
    
    fig = go.Figure(data=[
        go.Histogram(
            x=summary_df['overall_score'],
            nbinsx=20,
            marker_color='skyblue',
            opacity=0.7,
            name='Score Distribution'
        )
    ])
    
    # Add vertical lines for quality thresholds
    fig.add_vline(x=0.8, line_dash="dash", line_color="green", 
                  annotation_text="Good Threshold (0.8)")
    fig.add_vline(x=0.6, line_dash="dash", line_color="orange", 
                  annotation_text="Fair Threshold (0.6)")
    
    fig.update_layout(
        title="Distribution of Lead Quality Scores",
        title_x=0.5,
        xaxis_title="Quality Score",
        yaxis_title="Number of Leads",
        height=400,
        showlegend=False
    )
    
    return fig
