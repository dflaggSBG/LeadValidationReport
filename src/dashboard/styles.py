"""CSS styles for the Lead Validation Dashboard."""

DASHBOARD_STYLES = """
<style>
    /* Main layout styles */
    .main > div {
        padding-top: 2rem;
    }
    
    /* Header styles */
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 2rem;
        color: #1f77b4;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        margin: 1.5rem 0 1rem 0;
        color: #2c3e50;
        border-bottom: 2px solid #3498db;
        padding-bottom: 0.5rem;
    }
    
    /* Metric card styles */
    .metric-card {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        border-left: 4px solid #3498db;
        margin: 0.5rem 0;
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #2c3e50;
    }
    
    .metric-label {
        font-size: 0.9rem;
        color: #7f8c8d;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Alert styles */
    .alert {
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        border-radius: 0.375rem;
        border-left: 4px solid;
    }
    
    .alert-success {
        background-color: #d4edda;
        border-color: #28a745;
        color: #155724;
    }
    
    .alert-warning {
        background-color: #fff3cd;
        border-color: #ffc107;
        color: #856404;
    }
    
    .alert-danger {
        background-color: #f8d7da;
        border-color: #dc3545;
        color: #721c24;
    }
    
    .alert-info {
        background-color: #cce7ff;
        border-color: #007bff;
        color: #004085;
    }
    
    /* Data quality indicators */
    .quality-excellent {
        color: #28a745;
        font-weight: bold;
    }
    
    .quality-good {
        color: #20c997;
        font-weight: bold;
    }
    
    .quality-fair {
        color: #ffc107;
        font-weight: bold;
    }
    
    .quality-poor {
        color: #fd7e14;
        font-weight: bold;
    }
    
    .quality-invalid {
        color: #dc3545;
        font-weight: bold;
    }
    
    /* Table styles */
    .dataframe {
        border: none !important;
    }
    
    .dataframe th {
        background-color: #f8f9fa !important;
        color: #495057 !important;
        font-weight: 600 !important;
        border-bottom: 2px solid #dee2e6 !important;
    }
    
    .dataframe td {
        border-bottom: 1px solid #dee2e6 !important;
        padding: 0.75rem !important;
    }
    
    .dataframe tr:hover {
        background-color: #f5f5f5 !important;
    }
    
    /* Button styles */
    .stButton > button {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        border: none;
        border-radius: 0.375rem;
        color: white;
        font-weight: 600;
        padding: 0.5rem 1rem;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    /* Sidebar styles */
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%);
    }
    
    /* Tab styles */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: #f8f9fa;
        border-radius: 0.375rem 0.375rem 0 0;
        color: #495057;
        font-weight: 600;
        padding: 0.75rem 1rem;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    
    /* Chart container styles */
    .plotly-chart {
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        background: white;
        padding: 1rem;
        margin: 1rem 0;
    }
    
    /* Progress bar styles */
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    }
    
    /* Selectbox and input styles */
    .stSelectbox > div > div {
        border-radius: 0.375rem;
    }
    
    .stNumberInput > div > div {
        border-radius: 0.375rem;
    }
    
    /* Data freshness indicator */
    .freshness-indicator {
        border: 1px solid #ddd;
        border-radius: 0.375rem;
        padding: 0.75rem;
        margin: 1rem 0;
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border-left: 4px solid;
    }
    
    .freshness-fresh {
        border-left-color: #28a745;
    }
    
    .freshness-recent {
        border-left-color: #ffc107;
    }
    
    .freshness-stale {
        border-left-color: #dc3545;
    }
    
    /* Loading spinner */
    .stSpinner > div {
        border-top-color: #667eea !important;
    }
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: #f1f1f1;
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: linear-gradient(45deg, #667eea, #764ba2);
        border-radius: 4px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(45deg, #5a6fd8, #6a42a0);
    }
    
    /* Responsive design */
    @media (max-width: 768px) {
        .main-header {
            font-size: 2rem;
        }
        
        .section-header {
            font-size: 1.25rem;
        }
        
        .metric-value {
            font-size: 1.5rem;
        }
    }
</style>
"""


def apply_dashboard_styles():
    """Apply custom CSS styles to the dashboard."""
    import streamlit as st
    st.markdown(DASHBOARD_STYLES, unsafe_allow_html=True)
