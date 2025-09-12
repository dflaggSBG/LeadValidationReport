#!/usr/bin/env python3
"""
Lead Validation Report Generator
Generates comprehensive PDF reports from validation data
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any
import pandas as pd
import logging
from jinja2 import Template

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.dashboard.data_loader import (
    load_validation_metrics, load_validation_summary, load_validation_by_source,
    load_validation_trends, load_problematic_leads, load_conversion_analysis
)
from config.settings import REPORTS_DIR


class LeadValidationReporter:
    """Generate lead validation reports."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.INFO)
        
    def generate_comprehensive_report(self) -> str:
        """Generate comprehensive validation report."""
        try:
            self.logger.info("📊 Generating comprehensive validation report...")
            
            # Load all data
            metrics_data = load_validation_metrics()
            summary_data = load_validation_summary()
            source_data = load_validation_by_source()
            trends_data = load_validation_trends()
            problematic_data = load_problematic_leads()
            conversion_data = load_conversion_analysis()
            
            if metrics_data.empty:
                raise Exception("No validation data available")
            
            # Generate report
            report_content = self._create_comprehensive_html(
                metrics_data.iloc[0],
                summary_data,
                source_data,
                trends_data,
                problematic_data,
                conversion_data
            )
            
            # Save report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"lead_validation_comprehensive_{timestamp}.html"
            report_path = REPORTS_DIR / report_filename
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            self.logger.info(f"✅ Report generated: {report_path}")
            return str(report_path)
            
        except Exception as e:
            self.logger.error(f"❌ Report generation failed: {e}")
            raise
    
    def generate_compliance_report(self) -> str:
        """Generate compliance-focused report."""
        try:
            self.logger.info("📋 Generating compliance report...")
            
            # Load compliance-related data
            metrics_data = load_validation_metrics()
            problematic_data = load_problematic_leads()
            
            if metrics_data.empty:
                raise Exception("No validation data available")
            
            # Generate compliance report
            report_content = self._create_compliance_html(
                metrics_data.iloc[0],
                problematic_data
            )
            
            # Save report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"lead_validation_compliance_{timestamp}.html"
            report_path = REPORTS_DIR / report_filename
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            self.logger.info(f"✅ Compliance report generated: {report_path}")
            return str(report_path)
            
        except Exception as e:
            self.logger.error(f"❌ Compliance report generation failed: {e}")
            raise
    
    def _create_comprehensive_html(self, metrics: pd.Series, summary_df: pd.DataFrame, 
                                 source_df: pd.DataFrame, trends_df: pd.DataFrame,
                                 problematic_df: pd.DataFrame, conversion_df: pd.DataFrame) -> str:
        """Create comprehensive HTML report."""
        
        template_str = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lead Validation Report - {{ report_date }}</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f8f9fa;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .header {
            text-align: center;
            margin-bottom: 40px;
            padding-bottom: 20px;
            border-bottom: 3px solid #3498db;
        }
        .header h1 {
            color: #2c3e50;
            font-size: 2.5em;
            margin: 0;
        }
        .header .subtitle {
            color: #7f8c8d;
            font-size: 1.2em;
            margin-top: 10px;
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }
        .metric-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }
        .metric-value {
            font-size: 2em;
            font-weight: bold;
            margin-bottom: 5px;
        }
        .metric-label {
            font-size: 0.9em;
            opacity: 0.9;
        }
        .section {
            margin-bottom: 40px;
        }
        .section h2 {
            color: #2c3e50;
            border-bottom: 2px solid #3498db;
            padding-bottom: 10px;
            margin-bottom: 20px;
        }
        .table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        .table th,
        .table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        .table th {
            background-color: #f8f9fa;
            font-weight: bold;
            color: #2c3e50;
        }
        .table tr:hover {
            background-color: #f5f5f5;
        }
        .alert {
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
        }
        .alert-success {
            background-color: #d4edda;
            border: 1px solid #c3e6cb;
            color: #155724;
        }
        .alert-warning {
            background-color: #fff3cd;
            border: 1px solid #ffeaa7;
            color: #856404;
        }
        .alert-danger {
            background-color: #f8d7da;
            border: 1px solid #f1c6c7;
            color: #721c24;
        }
        .quality-excellent { color: #28a745; font-weight: bold; }
        .quality-good { color: #20c997; font-weight: bold; }
        .quality-fair { color: #ffc107; font-weight: bold; }
        .quality-poor { color: #fd7e14; font-weight: bold; }
        .quality-invalid { color: #dc3545; font-weight: bold; }
        .footer {
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            color: #7f8c8d;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Lead Validation Report</h1>
            <div class="subtitle">Comprehensive Analysis Generated on {{ report_date }}</div>
        </div>

        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-value">{{ "{:,}".format(metrics.total_leads) }}</div>
                <div class="metric-label">Total Leads</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ "{:.3f}".format(metrics.avg_overall_score) }}</div>
                <div class="metric-label">Avg Quality Score</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ "{:.1f}%".format(metrics.quality_leads_percentage) }}</div>
                <div class="metric-label">Quality Leads</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ "{:.1f}%".format(metrics.conversion_rate_percentage) }}</div>
                <div class="metric-label">Conversion Rate</div>
            </div>
        </div>

        <div class="section">
            <h2>📈 Quality Distribution</h2>
            {% set quality_percentage = metrics.quality_leads_percentage %}
            {% if quality_percentage >= 80 %}
                <div class="alert alert-success">
                    ✅ <strong>Excellent:</strong> High percentage of quality leads ({{ "{:.1f}%".format(quality_percentage) }})!
                </div>
            {% elif quality_percentage >= 60 %}
                <div class="alert alert-warning">
                    ⚠️ <strong>Moderate:</strong> Consider improving lead quality processes ({{ "{:.1f}%".format(quality_percentage) }}).
                </div>
            {% else %}
                <div class="alert alert-danger">
                    🚨 <strong>Action Required:</strong> Low lead quality detected ({{ "{:.1f}%".format(quality_percentage) }})!
                </div>
            {% endif %}
            
            <table class="table">
                <thead>
                    <tr>
                        <th>Quality Category</th>
                        <th>Count</th>
                        <th>Percentage</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td><span class="quality-excellent">Excellent (0.9-1.0)</span></td>
                        <td>{{ "{:,}".format(metrics.excellent_count) }}</td>
                        <td>{{ "{:.1f}%".format(metrics.excellent_percentage) }}</td>
                    </tr>
                    <tr>
                        <td><span class="quality-good">Good (0.8-0.9)</span></td>
                        <td>{{ "{:,}".format(metrics.good_count) }}</td>
                        <td>{{ "{:.1f}%".format(metrics.good_percentage) }}</td>
                    </tr>
                    <tr>
                        <td><span class="quality-fair">Fair (0.6-0.8)</span></td>
                        <td>{{ "{:,}".format(metrics.fair_count) }}</td>
                        <td>{{ "{:.1f}%".format(metrics.fair_percentage) }}</td>
                    </tr>
                    <tr>
                        <td><span class="quality-poor">Poor (0.4-0.6)</span></td>
                        <td>{{ "{:,}".format(metrics.poor_count) }}</td>
                        <td>{{ "{:.1f}%".format(metrics.poor_percentage) }}</td>
                    </tr>
                    <tr>
                        <td><span class="quality-invalid">Invalid (0.0-0.4)</span></td>
                        <td>{{ "{:,}".format(metrics.invalid_count) }}</td>
                        <td>{{ "{:.1f}%".format(metrics.invalid_percentage) }}</td>
                    </tr>
                </tbody>
            </table>
        </div>

        {% if not source_df.empty %}
        <div class="section">
            <h2>🎯 Lead Source Performance</h2>
            <table class="table">
                <thead>
                    <tr>
                        <th>Lead Source</th>
                        <th>Total Leads</th>
                        <th>Avg Quality Score</th>
                        <th>Quality %</th>
                        <th>Conversion Rate</th>
                    </tr>
                </thead>
                <tbody>
                    {% for _, source in source_df.head(10).iterrows() %}
                    <tr>
                        <td>{{ source.lead_source }}</td>
                        <td>{{ "{:,}".format(source.total_leads) }}</td>
                        <td>{{ "{:.3f}".format(source.avg_score) }}</td>
                        <td>{{ "{:.1f}%".format(source.quality_percentage) }}</td>
                        <td>{{ "{:.1f}%".format(source.conversion_rate) }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% endif %}

        {% if not problematic_df.empty %}
        <div class="section">
            <h2>🚨 Problematic Leads (Score < 0.6)</h2>
            <p><strong>{{ "{:,}".format(problematic_df|length) }}</strong> leads require attention:</p>
            <table class="table">
                <thead>
                    <tr>
                        <th>Lead ID</th>
                        <th>Score</th>
                        <th>Name</th>
                        <th>Company</th>
                        <th>Email</th>
                        <th>Lead Source</th>
                    </tr>
                </thead>
                <tbody>
                    {% for _, lead in problematic_df.head(20).iterrows() %}
                    <tr>
                        <td>{{ lead.lead_id|truncate(15) }}</td>
                        <td>{{ "{:.3f}".format(lead.overall_score) }}</td>
                        <td>{{ (lead.first_name or "") + " " + (lead.last_name or "") }}</td>
                        <td>{{ lead.company or "N/A" }}</td>
                        <td>{{ lead.email or "N/A" }}</td>
                        <td>{{ lead.lead_source or "Unknown" }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% if problematic_df|length > 20 %}
            <p><em>... and {{ "{:,}".format(problematic_df|length - 20) }} more leads</em></p>
            {% endif %}
        </div>
        {% endif %}

        <div class="section">
            <h2>📊 Summary & Recommendations</h2>
            <ul>
                <li><strong>Data Quality Status:</strong> 
                    {% if metrics.quality_leads_percentage >= 80 %}
                        <span class="quality-excellent">Excellent</span> - Continue current processes
                    {% elif metrics.quality_leads_percentage >= 60 %}
                        <span class="quality-fair">Needs Improvement</span> - Focus on data validation processes
                    {% else %}
                        <span class="quality-invalid">Critical</span> - Immediate attention required
                    {% endif %}
                </li>
                <li><strong>Conversion Performance:</strong> 
                    {{ "{:.1f}%".format(metrics.conversion_rate_percentage) }} overall conversion rate
                </li>
                <li><strong>Data Freshness:</strong> {{ metrics.data_freshness_status }}</li>
                {% if not source_df.empty %}
                <li><strong>Best Performing Source:</strong> {{ source_df.iloc[0].lead_source }} 
                    ({{ "{:.3f}".format(source_df.iloc[0].avg_score) }} avg score)</li>
                {% endif %}
            </ul>
        </div>

        <div class="footer">
            <p>Generated by Lead Validation Reporting System<br>
            Report Date: {{ report_date }}<br>
            Data Period: {{ metrics.earliest_lead_date.strftime('%Y-%m-%d') if metrics.earliest_lead_date else 'N/A' }} 
            to {{ metrics.latest_lead_date.strftime('%Y-%m-%d') if metrics.latest_lead_date else 'N/A' }}</p>
        </div>
    </div>
</body>
</html>
        """
        
        template = Template(template_str)
        
        return template.render(
            report_date=datetime.now().strftime('%B %d, %Y at %I:%M %p'),
            metrics=metrics,
            source_df=source_df,
            problematic_df=problematic_df
        )
    
    def _create_compliance_html(self, metrics: pd.Series, problematic_df: pd.DataFrame) -> str:
        """Create compliance-focused HTML report."""
        
        template_str = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lead Compliance Report - {{ report_date }}</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; line-height: 1.6; margin: 0; padding: 20px; background-color: #f8f9fa; }
        .container { max-width: 1000px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .header { text-align: center; margin-bottom: 40px; padding-bottom: 20px; border-bottom: 3px solid #dc3545; }
        .header h1 { color: #2c3e50; font-size: 2.5em; margin: 0; }
        .compliance-status { padding: 20px; border-radius: 8px; margin-bottom: 30px; text-align: center; font-size: 1.2em; }
        .compliant { background-color: #d4edda; color: #155724; border: 2px solid #28a745; }
        .non-compliant { background-color: #f8d7da; color: #721c24; border: 2px solid #dc3545; }
        .table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
        .table th, .table td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        .table th { background-color: #f8f9fa; font-weight: bold; }
        .section { margin-bottom: 30px; }
        .section h2 { color: #2c3e50; border-bottom: 2px solid #dc3545; padding-bottom: 10px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📋 Lead Compliance Report</h1>
            <div class="subtitle">Compliance Analysis Generated on {{ report_date }}</div>
        </div>

        <div class="compliance-status {% if metrics.quality_leads_percentage >= 70 %}compliant{% else %}non-compliant{% endif %}">
            {% if metrics.quality_leads_percentage >= 70 %}
                ✅ <strong>COMPLIANT:</strong> {{ "{:.1f}%".format(metrics.quality_leads_percentage) }} of leads meet quality standards
            {% else %}
                ⚠️ <strong>NON-COMPLIANT:</strong> Only {{ "{:.1f}%".format(metrics.quality_leads_percentage) }} of leads meet quality standards
            {% endif %}
        </div>

        <div class="section">
            <h2>🎯 Compliance Metrics</h2>
            <table class="table">
                <tr><td><strong>Total Leads Processed</strong></td><td>{{ "{:,}".format(metrics.total_leads) }}</td></tr>
                <tr><td><strong>Quality Leads (Score ≥ 0.8)</strong></td><td>{{ "{:,}".format(metrics.excellent_count + metrics.good_count) }} ({{ "{:.1f}%".format(metrics.quality_leads_percentage) }})</td></tr>
                <tr><td><strong>Non-Compliant Leads (Score < 0.6)</strong></td><td>{{ "{:,}".format(metrics.poor_count + metrics.invalid_count) }} ({{ "{:.1f}%".format(metrics.poor_percentage + metrics.invalid_percentage) }})</td></tr>
                <tr><td><strong>Average Quality Score</strong></td><td>{{ "{:.3f}".format(metrics.avg_overall_score) }}</td></tr>
            </table>
        </div>

        {% if not problematic_df.empty %}
        <div class="section">
            <h2>🚨 Non-Compliant Leads Requiring Review</h2>
            <p><strong>{{ "{:,}".format(problematic_df|length) }}</strong> leads below compliance threshold:</p>
            <table class="table">
                <thead>
                    <tr><th>Lead ID</th><th>Quality Score</th><th>Status</th><th>Issues</th></tr>
                </thead>
                <tbody>
                    {% for _, lead in problematic_df.head(50).iterrows() %}
                    <tr>
                        <td>{{ lead.lead_id|truncate(15) }}</td>
                        <td>{{ "{:.3f}".format(lead.overall_score) }}</td>
                        <td>{{ lead.validation_status }}</td>
                        <td>Review Required</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% endif %}

        <div class="section">
            <h2>📊 Compliance Actions Required</h2>
            <ul>
                {% if metrics.quality_leads_percentage < 70 %}
                <li>🚨 <strong>URGENT:</strong> Implement data quality improvement processes</li>
                <li>📋 Review and update lead validation criteria</li>
                <li>🔄 Increase validation frequency</li>
                {% endif %}
                <li>📊 Regular monitoring of lead quality metrics</li>
                <li>🎯 Training for lead generation teams</li>
            </ul>
        </div>
    </div>
</body>
</html>
        """
        
        template = Template(template_str)
        
        return template.render(
            report_date=datetime.now().strftime('%B %d, %Y at %I:%M %p'),
            metrics=metrics,
            problematic_df=problematic_df
        )


def main():
    """Main entry point for report generation."""
    parser = argparse.ArgumentParser(description="Lead Validation Report Generator")
    parser.add_argument(
        "--report-type",
        choices=["comprehensive", "compliance"],
        default="comprehensive",
        help="Type of report to generate"
    )
    
    args = parser.parse_args()
    
    reporter = LeadValidationReporter()
    
    try:
        if args.report_type == "comprehensive":
            report_path = reporter.generate_comprehensive_report()
        else:
            report_path = reporter.generate_compliance_report()
        
        print(f"✅ Report generated successfully: {report_path}")
        
    except Exception as e:
        print(f"❌ Report generation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
