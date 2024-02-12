import boto3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
import time
import os
from dateutil.relativedelta import relativedelta
import pytz
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image, Spacer
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from fpdf import FPDF
import io
from io import BytesIO
from tempfile import NamedTemporaryFile
from PyPDF2 import PdfReader, PdfMerger
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, PageBreak, Table
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
import numpy as np
from collections import defaultdict
import calendar

ec2_instance_id = 'i-0ad89840c239d3159'
rds_instance_id = 'database-1'
website_distid = 'ESI8C0HXJBOKP'
assets_distid = 'E17CIOSGCFI0X5'
waf_region = 'us-east-1'
web_acl_id = 'web-acl-id'
alb_name = 'testing'
bucket_name = 'automated-athena-test'

current_date = datetime.now()

first_day_current_month = current_date.replace(day=1)

end_time = first_day_current_month - timedelta(days=1)
first_day_desired_month = end_time.replace(day=1)


def get_rds_metrics(rds_instance_id, start_time, end_time):
    cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')

    response_rds = cloudwatch_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'm2_1',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'CPUUtilization',
                        'Dimensions': [
                            {
                                'Name': 'DBInstanceIdentifier',
                                'Value': rds_instance_id
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': 'RDS_CPUUtilization'
            },
            {
                'Id': 'm2_2',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'DatabaseConnections',
                        'Dimensions': [
                            {
                                'Name': 'DBInstanceIdentifier',
                                'Value': rds_instance_id
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Count'
                },
                'Label': 'RDS_DatabaseConnections'
            },
        ],
        StartTime=start_time,
        EndTime=end_time,
        ScanBy='TimestampDescending',
    )

    return response_rds['MetricDataResults']



# EC2 Metrics
def get_ec2_metrics(ec2_instance_id, start_time, end_time):
    cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')

    response_ec2 = cloudwatch_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'm1_1',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/EC2',
                        'MetricName': 'CPUUtilization',
                        'Dimensions': [
                            {
                                'Name': 'InstanceId',
                                'Value': ec2_instance_id
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': 'CPUUtilization'
            },
        ],
        StartTime=start_time,
        EndTime=end_time,
        ScanBy='TimestampDescending',
    )

    return response_ec2['MetricDataResults']


# WAF Metrics
def get_waf_metrics():
    cloudwatch_client = boto3.client('cloudwatch', region_name=waf_region)

    response_waf = cloudwatch_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'm4_1',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/WAF',
                        'MetricName': 'BlockedRequests',
                        'Dimensions': [
                            {
                                'Name': 'WebACL',
                                'Value': web_acl_id
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Sum',
                    'Unit': 'Count'
                },
                'Label': 'BlockedRequests'
            },
            {
                'Id': 'm4_2',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/WAF',
                        'MetricName': 'AllowedRequests',
                        'Dimensions': [
                            {
                                'Name': 'WebACL',
                                'Value': web_acl_id
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Sum',
                    'Unit': 'Count'
                },
                'Label': 'AllowedRequests'
            },
            {
                'Id': 'm4_3',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/WAF',
                        'MetricName': 'CountedRequests',
                        'Dimensions': [
                            {
                                'Name': 'WebACL',
                                'Value': web_acl_id
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Sum',
                    'Unit': 'Count'
                },
                'Label': 'CountedRequests'
            },
        ],
        StartTime=first_day_desired_month,
        EndTime=end_time,
        ScanBy='TimestampDescending',
    )

    return response_waf['MetricDataResults']

# Cloudfront Metrics
def get_cloudfront_metrics():
    cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')

    response_cloudfront = cloudwatch_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'm3_1',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': 'Requests',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Sum',
                    'Unit': 'None'
                },
                'Label': 'Requests'
            },
            {
                'Id': 'm3_2',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': 'OriginLatency',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': 'OriginLatency'
            },
            {
                'Id': 'm3_3',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': 'CacheHitRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global',
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': 'CacheHitRate'
            },
            {
                'Id': 'm3_4',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': '4xxErrorRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': '4xxErrorRate'
            },
            {
                'Id': 'm3_5',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': '5xxErrorRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': '5xxErrorRate'
            },
            {
                'Id': 'm3_6',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': 'TotalErrorRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': 'TotalErrorRate'
            },
            {
                'Id': 'm3_7',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': '401ErrorRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': '401ErrorRate'
            },
            {
                'Id': 'm3_8',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': '402ErrorRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': '402ErrorRate'
            },
            {
                'Id': 'm3_9',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': '403ErrorRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': '403ErrorRate'
            },
            {
                'Id': 'm3_10',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': '404ErrorRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': '404ErrorRate'
            },
            {
                'Id': 'm3_11',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': '501ErrorRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': '501ErrorRate'
            },
            {
                'Id': 'm3_12',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': '502ErrorRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': '502ErrorRate'
            },
            {
                'Id': 'm3_13',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': '503ErrorRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': '503ErrorRate'
            },
            {
                'Id': 'm3_14',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/CloudFront',
                        'MetricName': '504ErrorRate',
                        'Dimensions': [
                            {
                                'Name': 'Region',
                                'Value': 'Global'
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': website_distid
                            },
                            {
                                'Name': 'DistributionId',
                                'Value': assets_distid
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Percent'
                },
                'Label': '504ErrorRate'
            },
        ],
        StartTime=first_day_desired_month,
        EndTime=end_time,
        ScanBy='TimestampDescending',
    )

    return response_cloudfront['MetricDataResults']

# ALB Metrics
def get_alb_metrics(alb_name, start_time, end_time):
    cloudwatch_client = boto3.client('cloudwatch', region_name='us-east-1')

    response_alb = cloudwatch_client.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'm5_1',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/ApplicationELB',
                        'MetricName': 'TargetResponseTime',
                        'Dimensions': [
                            {
                                'Name': 'LoadBalancer',
                                'Value': alb_name
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Average',
                    'Unit': 'Seconds'
                },
                'Label': 'ALB_TargetResponseTime'
            },
            {
                'Id': 'm5_2',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/ApplicationELB',
                        'MetricName': 'HealthyHostCount',
                        'Dimensions': [
                            {
                                'Name': 'LoadBalancer',
                                'Value': alb_name
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Sum',
                    'Unit': 'Count'
                },
                'Label': 'ALB_HealthyHostCount'
            },
            {
                'Id': 'm5_3',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/ApplicationELB',
                        'MetricName': 'UnHealthyHostCount',
                        'Dimensions': [
                            {
                                'Name': 'LoadBalancer',
                                'Value': alb_name
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Sum',
                    'Unit': 'Count'
                },
                'Label': 'ALB_UnHealthyHostCount'
            },
            {
                'Id': 'm5_4',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/ApplicationELB',
                        'MetricName': 'HTTPCode_Target_5XX_Count',
                        'Dimensions': [
                            {
                                'Name': 'LoadBalancer',
                                'Value': alb_name
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Sum',
                    'Unit': 'Count'
                },
                'Label': 'ALB_HTTPCode_Target_5XX_Count'
            },
            {
                'Id': 'm5_5',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/ApplicationELB',
                        'MetricName': 'HTTPCode_Target_3XX_Count',
                        'Dimensions': [
                            {
                                'Name': 'LoadBalancer',
                                'Value': alb_name
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Sum',
                    'Unit': 'Count'
                },
                'Label': 'ALB_HTTPCode_Target_3XX_Count'
            },
            {
                'Id': 'm5_6',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/ApplicationELB',
                        'MetricName': 'HTTPCode_Target_4XX_Count',
                        'Dimensions': [
                            {
                                'Name': 'LoadBalancer',
                                'Value': alb_name
                            },
                        ]
                    },
                    'Period': 21600,
                    'Stat': 'Sum',
                    'Unit': 'Count'
                },
                'Label': 'ALB_HTTPCode_Target_4XX_Count'
            },
        ],
        StartTime=start_time,
        EndTime=end_time,
        ScanBy='TimestampDescending',
    )

    return response_alb['MetricDataResults']



# Create a subclass of FPDF class
class PDFWithHeaderFooter(FPDF):
    def header(self):
        self.image('https://automated-athena-test.s3.amazonaws.com/winspire.png', 10, 8, 33)

        self.image('https://automated-athena-test.s3.amazonaws.com/whistlemind.png', 165, 8, 33)

        last_month = first_day_desired_month.strftime('%B %Y')
        current_year = current_date.year

        self.set_font('Times', 'B', 12)

        self.cell(0, 10, f'Technical Report {last_month}', 0, 1, 'C')

    def footer(self):
        self.set_y(-25)

        self.set_font('Helvetica', 'B', 10)
        self.cell(0, 5, '%s' % self.page_no(), 0, 0, 'R')

        self.set_font('Helvetica', '', 7.5)

        footer_text = (
            "This document is strictly confidential communication to and solely for the use of the recipient and may not be reproduced or circulated without Whistlemind  \nTechnologies LLP's prior written consent. If you are not the intended recipient, you may not disclose or use the information in this documentation in any \n way. The information is not intended as an offer or solicitation with respect to the purchase or sale of security."
        ).encode('utf-8').decode('latin-1')

        self.set_x(10)

        ending_position = self.w - 20  # 20 units from the right margin
        self.multi_cell(ending_position, 3.5, footer_text, align='L')


def create_report():
    try:
        current_date = datetime.now()
        # Get the first day of the current month
        first_day_current_month = current_date.replace(day=1)
        # Calculate the last day of the previous month
        last_day_previous_month = first_day_current_month - timedelta(days=1)
        # Get the name of the previous month
        previous_month_name = last_day_previous_month.strftime("%B %Y")

        current_month_year = current_date.strftime("%B %Y")

        pdf_filename = "technical_report"
        image1_path = "https://automated-athena-test.s3.amazonaws.com/whistlemind.png"
        image2_path = "https://automated-athena-test.s3.amazonaws.com/winspire.png"
        company_name = "Whistlemind Technologies LLP"
        address = "16/3A, Patil Complex, Ambedkar \n Chowk, Aundh Road, Pune-20"
        telephone_number = "+91 8857806297"
        web_address = "www.whistlemind.com"

        pdf_path = f"/tmp/{pdf_filename}.pdf"
        doc = SimpleDocTemplate(pdf_path, pagesize=A4)
        styles = getSampleStyleSheet()

        style_title = ParagraphStyle(
            "title",
            parent=styles["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=22,
            textColor=colors.black,
            alignment=1,
        )

        style_center = ParagraphStyle(
            "center",
            parent=styles["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=12,
            textColor=colors.black,
            alignment=1,
        )

        style_left = ParagraphStyle(
            "left",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=11,
            textColor=colors.black,
        )

        content = []
        content.append(Spacer(1, 25))

        title_text = f"<title>AWS Report - {previous_month_name}</title>"
        content.append(Paragraph(title_text, style_title))

        content.append(Spacer(1, 25))

        from_text = f"<p>From</p>"
        content.append(Spacer(1, 25))

        content.append(Paragraph(from_text, style_center))
        content.append(Spacer(1, 25))
        content.append(Image(image1_path, width=230, height=120))
        content.append(Spacer(1, 25))

        to_text = f"<p>To</p>"
        content.append(Paragraph(to_text, style_center))

        content.append(Image(image2_path, width=240, height=105))
        content.append(Spacer(1, 120))

        address_lines = address.split('\n')
        address_text = f"<b>{company_name}</b><br/>"
        for line in address_lines:
            address_text += f"{line}<br/>"
        address_text += f'Tel: {telephone_number}<br/>Web: <a href="{web_address}">{web_address}</a>'
        content.append(Paragraph(address_text, style_left))

        content.append(Spacer(1, 25))

        date_text = f"Date: {current_date.strftime('%d %B %Y')}"
        content.append(Paragraph(date_text, style_left))

        doc.build(content)
        print(f"Report generated successfully: {pdf_path}")
        return pdf_path
    except Exception as e:
        print(f"Error in create_report: {str(e)}")


def generate_content_table_pdf(file_name):
    try:
        content_data = [
            ["Sr No", "Table Contents"],
            [1, "Amazon RDS (Relational Database Service)\n    CPU Utilization"],
            [2, "Application Load Balancer\n    Latency"],
            [3, "Amazon EC2 Instancesn\n    Memory Utilization\n    CPU Utilization"],
            [4, "AWS WAF (Web Application Firewall)"],
            [5, "CloudFront\n    CloudFront Website Request"],
            [6, "Latency Analysis\n    CacheHitRate\n    OriginLatency\n    DTO"],
            [7, "Error Analysis\n    TotalErrorRate\n    4xxErrorRate\n    5xxErrorRate"],
            [8, "Security Insights\n    Top User-Agents\n    Top Blocked Rules\n    Top 10 IPs blocked"],
        ]
        pdf_canvas = canvas.Canvas(file_name, pagesize=A4)
        pdf_table = Table(content_data, colWidths=[0.7 * inch, 4 * inch])  # Adjust column widths here

        style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.white),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, -1), (-1, 0), 'CENTER'),
            ('FONT', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 13),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BOX', (0, 0), (-1, -1), 1, colors.black),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
        ])

        pdf_table.setStyle(style)

        table_width = 900
        table_height = len(content_data) * 40

        left_margin = 100
        top_margin = 100

        table_x = left_margin
        table_y = A4[1] - top_margin - table_height

        pdf_table.wrapOn(pdf_canvas, table_width, table_height)
        pdf_table.drawOn(pdf_canvas, table_x, table_y)

        pdf_canvas.save()
        print(f"Table saved as {file_name}")
    except Exception as e:
        print(f"Error in generate_content_table_pdf: {str(e)}")

def retrieve_and_save_pdf():
    try:
        tables_pdf_filename = '/tmp/athena_tables.pdf'

        account_id = boto3.client('sts').get_caller_identity().get('Account')
        current_time = datetime.utcnow()
        current_date_str = current_time.strftime('%Y%m%d')
        query_details = {
            'query1': f"{account_id}-top-user-agents-{current_date_str}",
            'query2': f"{account_id}-top-rules-blocked-{current_date_str}",
            'query3': f"{account_id}-top-ips-blocked-{current_date_str}"
        }

        column_widths = {
            'query1': [24, 300, 80, 80],
            'query2': [24, 300, 80],
            'query3': [24, 250, 50, 80]
        }

        pdf_buffer = io.BytesIO()
        pdf = SimpleDocTemplate(pdf_buffer)

        elements = []
        current_date = datetime.now()
        last_month = current_date - relativedelta(months=1)
        last_month_year = last_month.year
        last_month_numeric = last_month.month

        s3 = boto3.client('s3')

        # Create an empty list to track if Athena tables are processed
        processed_tables = []

        for query_name, file_suffix in query_details.items():
            if query_name not in processed_tables:  # Check if the table is processed
                file_key = f"athena-queries-results/{last_month_year}/{last_month_numeric}/{file_suffix}.csv"

                try:
                    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
                    csv_data = pd.read_csv(io.BytesIO(obj['Body'].read()))

                    table_data = [csv_data.columns.tolist()]
                    table_data[0].insert(0, "#")

                    for i, row in enumerate(csv_data.itertuples(), start=1):
                        content = [str(i)] + list(row)[1:]
                        content = [Paragraph(str(cell), getSampleStyleSheet()["Normal"]) for cell in content]
                        table_data.append(content)

                    col_widths_query = column_widths.get(query_name, [24] * len(csv_data.columns))

                    table = Table(table_data, colWidths=col_widths_query)
                    style = [
                        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                        ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ]

                    table.setStyle(style)
                    elements.append(table)
                    elements.append(Spacer(1, 25))

                    processed_tables.append(query_name)  # Mark the table as processed

                    print(f"CSV data fetched successfully for {query_name}")
                except Exception as e:
                    print(f"Error fetching object: {e}")

        pdf.build(elements)
        pdf_buffer.seek(0)

        with open(tables_pdf_filename, 'wb') as f:
            f.write(pdf_buffer.getvalue())

        print("Athena Table is ready")

        return tables_pdf_filename
    except Exception as e:
        print(f"Error in retrieve_and_save_pdf: {str(e)}")

def create_plots_pdf(rds_instance_id, ec2_instance_id, website_distid, assets_distid, web_acl_id, alb_name, first_day_desired_month, end_time):
    try:
        # RDS Plots
        rds_metric_results = get_rds_metrics(rds_instance_id, first_day_desired_month, end_time)

        timestamps_rds_cpu = rds_metric_results[0]['Timestamps']
        values_rds_cpu = rds_metric_results[0]['Values']

        timestamps_rds_connections = rds_metric_results[1]['Timestamps']
        values_rds_connections = rds_metric_results[1]['Values']

        pdf = PDFWithHeaderFooter()
        pdf.add_page()

        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_rds_cpu, values_rds_cpu, label='RDS_CPUUtilization')
        plt.title('RDS CPU Utilization')
        plt.xlabel('Timestamp')
        plt.ylabel('CPU Utilization (%)')
        plt.grid(True)
        plt.tight_layout()

        img_buffer_rds_cpu = BytesIO()
        plt.savefig(img_buffer_rds_cpu, format='png')
        plt.close()

        img_buffer_rds_cpu.seek(0)

        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_rds_cpu:
            temp_img_rds_cpu.write(img_buffer_rds_cpu.read())
            temp_img_path_rds_cpu = temp_img_rds_cpu.name

        title_rds_cpu = 'RDS CPU Utilization'
        description_rds_cpu = f'This plot shows the average CPU utilization of the RDS instance {rds_instance_id} for the last month.'

        position_after_description = pdf.get_y()

        pdf.image(temp_img_path_rds_cpu, x=pdf.w / 1 - 200, y=pdf.h / 2 - 100, w=175, h=100)
        pdf.ln(10)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_rds_cpu)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_rds_cpu)
        pdf.ln(80)

        # EC2 Plots
        ec2_metric_results = get_ec2_metrics(ec2_instance_id, first_day_desired_month, end_time)

        timestamps_cpu = ec2_metric_results[0]['Timestamps']
        values_cpu = ec2_metric_results[0]['Values']

        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_cpu, values_cpu, label='EC2_CPUUtilization')
        plt.title('EC2 CPU Utilization')
        plt.xlabel('Timestamp')
        plt.ylabel('CPU Utilization (%)')
        plt.grid(True)
        plt.tight_layout()

        img_buffer_ec2 = BytesIO()
        plt.savefig(img_buffer_ec2, format='png')
        plt.close()

        img_buffer_ec2.seek(0)

        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_ec2:
            temp_img_ec2.write(img_buffer_ec2.read())
            temp_img_path_ec2 = temp_img_ec2.name

        title_ec2 = 'EC2 CPU Utilization'
        description_ec2 = f'This plot shows the average CPU utilization of the EC2 instance {ec2_instance_id} for the last month.'

        pdf.image(temp_img_path_ec2, x=pdf.w / 1 - 200, y=pdf.w / 1 - 40, w=175, h=100)
        pdf.ln(20)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_ec2)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_ec2)
        
        pdf.add_page()


        # WAF Plots
        waf_metric_results = get_waf_metrics()
    
        # Extract WAF metric timestamps and values
        timestamps_blocked_requests = waf_metric_results[0]['Timestamps']
        values_blocked_requests = waf_metric_results[0]['Values']
    
        timestamps_allowed_requests = waf_metric_results[1]['Timestamps']
        values_allowed_requests = waf_metric_results[1]['Values']
    
        timestamps_counted_requests = waf_metric_results[2]['Timestamps']
        values_counted_requests = waf_metric_results[2]['Values']
    
        # Plot WAF metrics - Blocked and Allowed Requests
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_blocked_requests, values_blocked_requests, label='Blocked Requests', color='red')
        plt.plot(timestamps_allowed_requests, values_allowed_requests, label='Allowed Requests', color='green')
        plt.title('WAF Blocked and Allowed Requests')
        plt.xlabel('Timestamp')
        plt.ylabel('Number of Requests')
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
    
        img_buffer_requests = BytesIO()
        plt.savefig(img_buffer_requests, format='png')
        plt.close()
    
        img_buffer_requests.seek(0)
    
        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_requests:
            temp_img_requests.write(img_buffer_requests.read())
            temp_img_path_requests = temp_img_requests.name
    
        title_requests = 'WAF Blocked and Allowed Requests'
        description_requests = 'This plot shows the number of requests blocked and allowed by the Web Application Firewall (WAF).'
    
        pdf.image(temp_img_path_requests, x=pdf.w / 1 - 200, y=pdf.h / 2 - 100, w=175, h=100)
        pdf.ln(10)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_requests)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_requests)
        pdf.ln(90)
    
        # Plot WAF metrics - Counted Requests
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_counted_requests, values_counted_requests, label='Counted Requests', color='blue')
        plt.title('WAF Counted Requests')
        plt.xlabel('Timestamp')
        plt.ylabel('Number of Requests')
        plt.grid(True)
        plt.tight_layout()
    
        img_buffer_counted_requests = BytesIO()
        plt.savefig(img_buffer_counted_requests, format='png')
        plt.close()
    
        img_buffer_counted_requests.seek(0)
    
        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_counted_requests:
            temp_img_counted_requests.write(img_buffer_counted_requests.read())
            temp_img_path_counted_requests = temp_img_counted_requests.name
    
        title_counted_requests = 'WAF Counted Requests'
        description_counted_requests = 'This plot shows the number of requests counted by the Web Application Firewall (WAF).'
    
        pdf.image(temp_img_path_counted_requests, x=pdf.w / 1 - 200, y=pdf.w / 1 - 40, w=175, h=100)
        pdf.ln(10)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_counted_requests)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_counted_requests)
        pdf.ln(90)
    
        pdf.add_page()
    
    
    
    
        #CDN Plots
        cloudfront_metric_results = get_cloudfront_metrics()
    
        # Extract CloudFront metric timestamps and values
        timestamps_requests = cloudfront_metric_results[0]['Timestamps']
        values_requests = cloudfront_metric_results[0]['Values']
    
        timestamps_4xx_error_rate = cloudfront_metric_results[1]['Timestamps']
        values_4xx_error_rate = cloudfront_metric_results[1]['Values']
    
        timestamps_5xx_error_rate = cloudfront_metric_results[2]['Timestamps']
        values_5xx_error_rate = cloudfront_metric_results[2]['Values']
    
        timestamps_total_error_rate = cloudfront_metric_results[3]['Timestamps']
        values_total_error_rate = cloudfront_metric_results[3]['Values']
    
        timestamps_origin_latency = cloudfront_metric_results[4]['Timestamps']
        values_origin_latency = cloudfront_metric_results[4]['Values']
    
        timestamps_cache_hit_rate = cloudfront_metric_results[5]['Timestamps']
        values_cache_hit_rate = cloudfront_metric_results[5]['Values']
    
        timestamps_4xx_all_error_rate = cloudfront_metric_results[6]['Timestamps']
        values_4xx_all_error_rate = cloudfront_metric_results[6]['Values']
    
        timestamps_5xx_all_error_rate = cloudfront_metric_results[7]['Timestamps']
        values_5xx_all_error_rate = cloudfront_metric_results[7]['Values']
    
        # Plot CloudFront metrics - Requests
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_requests, values_requests, label='CloudFront_Requests')
        plt.title('CloudFront Requests')
        plt.xlabel('Timestamp')
        plt.ylabel('Requests')
        plt.grid(True)
        plt.tight_layout()
    
        img_buffer_requests = BytesIO()
        plt.savefig(img_buffer_requests, format='png')
        plt.close()
    
        img_buffer_requests.seek(0)
    
        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_requests:
            temp_img_requests.write(img_buffer_requests.read())
            temp_img_path_requests = temp_img_requests.name
    
        title_requests = 'CloudFront Requests'
        description_requests = 'This plot shows the number of requests for the CloudFront distribution.'
    
        pdf.image(temp_img_path_requests, x=pdf.w / 1 - 200, y=pdf.h / 2 - 100, w=175, h=100)
        pdf.ln(10)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_requests)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_requests)
        pdf.ln(90)
    
        # Plot CloudFront metrics - Origin Latency
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_origin_latency, values_origin_latency, label='CloudFront_Origin_Latency')
        plt.title('CloudFront Origin Latency')
        plt.xlabel('Timestamp')
        plt.ylabel('Latency (Seconds)')
        plt.grid(True)
        plt.tight_layout()
    
        img_buffer_origin_latency = BytesIO()
        plt.savefig(img_buffer_origin_latency, format='png')
        plt.close()
    
        img_buffer_origin_latency.seek(0)
    
        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_origin_latency:
            temp_img_origin_latency.write(img_buffer_origin_latency.read())
            temp_img_path_origin_latency = temp_img_origin_latency.name
    
        title_origin_latency = 'CloudFront Origin Latency'
        description_origin_latency = 'This plot shows the origin latency for the CloudFront distribution.'
    
        pdf.image(temp_img_path_origin_latency, x=pdf.w / 1 - 200, y=pdf.w / 1 - 40, w=175, h=100)
        pdf.ln(10)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_origin_latency)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_origin_latency)
        pdf.ln(90)
        
        pdf.add_page()
        
        # Plot CloudFront metrics - Cache Hit Rate
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_cache_hit_rate, values_cache_hit_rate, label='CloudFront_Cache_Hit_Rate')
        plt.title('CloudFront Cache Hit Rate')
        plt.xlabel('Timestamp')
        plt.ylabel('Cache Hit Rate (%)')
        plt.grid(True)
        plt.tight_layout()
    
        img_buffer_cache_hit_rate = BytesIO()
        plt.savefig(img_buffer_cache_hit_rate, format='png')
        plt.close()
    
        img_buffer_cache_hit_rate.seek(0)
    
        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_cache_hit_rate:
            temp_img_cache_hit_rate.write(img_buffer_cache_hit_rate.read())
            temp_img_path_cache_hit_rate = temp_img_cache_hit_rate.name
    
        title_cache_hit_rate = 'CloudFront Cache Hit Rate'
        description_cache_hit_rate = 'This plot shows the cache hit rate for the CloudFront distribution.'
    
        pdf.image(temp_img_path_cache_hit_rate, x=pdf.w / 1 - 200, y=pdf.h / 2 - 100, w=175, h=100)
        pdf.ln(10)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_cache_hit_rate)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_cache_hit_rate)
        pdf.ln(90)
    
        # Combine 4xx and 5xx Error Rate
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_4xx_error_rate, values_4xx_error_rate, label='4xx_Error_Rate', linestyle='dashed', color='blue')
        plt.plot(timestamps_5xx_error_rate, values_5xx_error_rate, label='5xx_Error_Rate', linestyle='dashed', color='red')
        plt.title('CloudFront 4xx and 5xx Error Rates')
        plt.xlabel('Timestamp')
        plt.ylabel('Error Rate (%)')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
    
        img_buffer_combined_error_rates = BytesIO()
        plt.savefig(img_buffer_combined_error_rates, format='png')
        plt.close()
    
        img_buffer_combined_error_rates.seek(0)
    
        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_combined_error_rates:
            temp_img_combined_error_rates.write(img_buffer_combined_error_rates.read())
            temp_img_path_combined_error_rates = temp_img_combined_error_rates.name
    
        title_combined_error_rates = 'CloudFront 4xx and 5xx Error Rates'
        description_combined_error_rates = 'This plot shows the combined 4xx and 5xx error rates for the CloudFront distribution.'
    
        pdf.image(temp_img_path_combined_error_rates, x=pdf.w / 1 - 200, y=pdf.w / 1 - 40, w=175, h=100)
        pdf.ln(10)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_combined_error_rates)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_combined_error_rates)
        pdf.ln(90)
        
        pdf.add_page()
    
        # Plot CloudFront metrics - Total Error Rate
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_total_error_rate, values_total_error_rate, label='CloudFront_Total_Error_Rate')
        plt.title('CloudFront Total Error Rate')
        plt.xlabel('Timestamp')
        plt.ylabel('Error Rate (%)')
        plt.grid(True)
        plt.tight_layout()
    
        img_buffer_total_error_rate = BytesIO()
        plt.savefig(img_buffer_total_error_rate, format='png')
        plt.close()
    
        img_buffer_total_error_rate.seek(0)
    
        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_total_error_rate:
            temp_img_total_error_rate.write(img_buffer_total_error_rate.read())
            temp_img_path_total_error_rate = temp_img_total_error_rate.name
    
        title_total_error_rate = 'CloudFront Total Error Rate'
        description_total_error_rate = 'This plot shows the total error rate for the CloudFront distribution.'
    
        pdf.image(temp_img_path_total_error_rate, x=pdf.w / 1 - 200, y=pdf.h / 2 - 100, w=175, h=100)
        pdf.ln(10)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_total_error_rate)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_total_error_rate)
        pdf.ln(90)
    
        # Plot CloudFront metrics - 4xx All Error Rate
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_4xx_all_error_rate, values_4xx_all_error_rate, label='CloudFront_4xx_All_Error_Rate')
        plt.title('CloudFront 4xx All Error Rate')
        plt.xlabel('Timestamp')
        plt.ylabel('Error Rate (%)')
        plt.grid(True)
        plt.tight_layout()
    
        img_buffer_4xx_all_error_rate = BytesIO()
        plt.savefig(img_buffer_4xx_all_error_rate, format='png')
        plt.close()
    
        img_buffer_4xx_all_error_rate.seek(0)
    
        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_4xx_all_error_rate:
            temp_img_4xx_all_error_rate.write(img_buffer_4xx_all_error_rate.read())
            temp_img_path_4xx_all_error_rate = temp_img_4xx_all_error_rate.name
    
        title_4xx_all_error_rate = 'CloudFront 4xx All Error Rate'
        description_4xx_all_error_rate = 'This plot shows the 4xx all error rate for the CloudFront distribution.'
    
        pdf.image(temp_img_path_4xx_all_error_rate, x=pdf.w / 1 - 200, y=pdf.w / 1 - 40, w=175, h=100)
        pdf.ln(10)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_4xx_all_error_rate)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_4xx_all_error_rate)
        pdf.ln(90)
    
        pdf.add_page()
    
        # Plot CloudFront metrics - 5xx All Error Rate
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_5xx_all_error_rate, values_5xx_all_error_rate, label='CloudFront_5xx_All_Error_Rate')
        plt.title('CloudFront 5xx All Error Rate')
        plt.xlabel('Timestamp')
        plt.ylabel('Error Rate (%)')
        plt.grid(True)
        plt.tight_layout()
    
        img_buffer_5xx_all_error_rate = BytesIO()
        plt.savefig(img_buffer_5xx_all_error_rate, format='png')
        plt.close()
    
        img_buffer_5xx_all_error_rate.seek(0)
    
        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_5xx_all_error_rate:
            temp_img_5xx_all_error_rate.write(img_buffer_5xx_all_error_rate.read())
            temp_img_path_5xx_all_error_rate = temp_img_5xx_all_error_rate.name
    
        title_5xx_all_error_rate = 'CloudFront 5xx All Error Rate'
        description_5xx_all_error_rate = 'This plot shows the 5xx all error rate for the CloudFront distribution.'
    
        pdf.image(temp_img_path_5xx_all_error_rate, x=pdf.w / 1 - 200, y=pdf.h / 2 - 100, w=175, h=100)
        pdf.ln(10)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_5xx_all_error_rate)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_5xx_all_error_rate)
        pdf.ln(90)
    
    
    
        # ALB Plots
        alb_metric_results = get_alb_metrics(alb_name, first_day_desired_month, end_time)
    
        timestamps_alb_response_time = alb_metric_results[0]['Timestamps']
        values_alb_response_time = alb_metric_results[0]['Values']
    
        timestamps_alb_healthy_hosts = alb_metric_results[1]['Timestamps']
        values_alb_healthy_hosts = alb_metric_results[1]['Values']
    
        timestamps_alb_unhealthy_hosts = alb_metric_results[2]['Timestamps']
        values_alb_unhealthy_hosts = alb_metric_results[2]['Values']
    
        # ALB Response Time
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_alb_response_time, values_alb_response_time, label='ALB_TargetResponseTime')
        plt.title('ALB Target Response Time')
        plt.xlabel('Timestamp')
        plt.ylabel('Target Response Time (Seconds)')
        plt.grid(True)
        plt.tight_layout()
    
        img_buffer_alb_response_time = BytesIO()
        plt.savefig(img_buffer_alb_response_time, format='png')
        plt.close()
    
        img_buffer_alb_response_time.seek(0)
    
        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_alb_response_time:
            temp_img_alb_response_time.write(img_buffer_alb_response_time.read())
            temp_img_path_alb_response_time = temp_img_alb_response_time.name
    
        title_alb_response_time = 'ALB Target Response Time'
        description_alb_response_time = 'This plot shows the average target response time of the ALB ' + alb_name + ' for the last month.'
    
        pdf.image(temp_img_path_alb_response_time, x=pdf.w / 1 - 200, y=pdf.w / 1 - 40, w=175, h=100)  # Adjust x, y, w, and h as needed
        pdf.ln(10)  # Move to the next line
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_alb_response_time)
        pdf.ln(10)  # Move to the next line
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_alb_response_time)
        pdf.ln(80)  # Move to the next line
    
        pdf.add_page()
    
        # Combine Healthy and Unhealthy Hosts into a single plot
        plt.figure(figsize=(10, 6))
        plt.plot(timestamps_alb_healthy_hosts, values_alb_healthy_hosts, label='Healthy_Hosts')
        plt.plot(timestamps_alb_unhealthy_hosts, values_alb_unhealthy_hosts, label='Unhealthy_Hosts')
        plt.title('ALB Host Health Status')
        plt.xlabel('Timestamp')
        plt.ylabel('Host Count')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
    
        img_buffer_alb_host_health = BytesIO()
        plt.savefig(img_buffer_alb_host_health, format='png')
        plt.close()
    
        img_buffer_alb_host_health.seek(0)
    
        with NamedTemporaryFile(delete=False, suffix=".png") as temp_img_alb_host_health:
            temp_img_alb_host_health.write(img_buffer_alb_host_health.read())
            temp_img_path_alb_host_health = temp_img_alb_host_health.name
    
        title_alb_host_health = 'ALB Host Health Status'
        description_alb_host_health = 'This plot shows the count of healthy and unhealthy hosts for the ALB ' + alb_name + ' for the last month.'
    
        pdf.image(temp_img_path_alb_host_health, x=pdf.w / 1 - 200, y=pdf.h / 2 - 100, w=175, h=100)
        pdf.ln(10)
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(40, 10, title_alb_host_health)
        pdf.ln(10)
        pdf.set_font('Arial', '', 10)
        pdf.multi_cell(0, 10, description_alb_host_health)
        pdf.ln(90)


        # Save PDF with plots
        pdf_output_path = "/tmp/plots.pdf"
        pdf.output(pdf_output_path)
        print(f"Plots saved as {pdf_output_path}")

        return pdf_output_path
    except Exception as e:
        print(f"Error in create_plots_pdf: {str(e)}")

def generate_cost_comparison_image():
    client = boto3.client('ce')

    current_date = datetime.now()
    end_date = current_date.replace(day=1) - timedelta(days=1)
    start_date = end_date.replace(day=1) - timedelta(days=1)
    start_date -= timedelta(days=30)
    
    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date.strftime('%Y-%m-%d'),
            'End': end_date.strftime('%Y-%m-%d')
        },
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        Filter={
            "Dimensions": {
                "Key": "RECORD_TYPE",
                "Values": ["Usage"]
            }
        }
    )

    cost_data = []
    month_labels = []

    for result in response['ResultsByTime']:
        month_start = result['TimePeriod']['Start']
        month_date = datetime.strptime(month_start, '%Y-%m-%d')
        month_name = calendar.month_abbr[month_date.month]
        year = month_date.year
        month_labels.append(f"{month_name}-{year}")
        cost = float(result['Total']['UnblendedCost']['Amount'])
        cost_data.append(cost)

    plt.figure(figsize=(8.27/1.5, 11.69/2))
    plt.bar(month_labels, cost_data, label='Cost')
    plt.xlabel('Month')
    plt.ylabel('Cost')
    plt.title('Cost Comparison')
    plt.legend()
    plt.grid(False)

    plt.xticks(rotation=0, ha='center')

    image_file = '/tmp/Cost.png'  # Change the extension to save in different image formats
    plt.savefig(image_file, bbox_inches='tight', pad_inches=0.1)
    plt.close()

    print(f"Image file '{image_file}' has been saved successfully.")

generate_cost_comparison_image()

def analyze_and_plot_top_services():
    def get_top_services(start, end, n):
        client = boto3.client('ce', region_name='us-east-1')
        
        response = client.get_cost_and_usage(
            TimePeriod={
                'Start': start.strftime('%Y-%m-01'),
                'End': end.strftime('%Y-%m-%d')
            },
            Granularity='MONTHLY',
            Metrics=['UnblendedCost'],
            GroupBy=[
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE'
                }
            ]
        )
        
        services_costs = defaultdict(float)
        for result in response['ResultsByTime']:
            for group in result['Groups']:
                service_name = group['Keys'][0]
                cost = float(group['Metrics']['UnblendedCost']['Amount'])
                services_costs[service_name] += cost
        
        sorted_services = sorted(services_costs.items(), key=lambda x: x[1], reverse=True)
        top_n_services = dict(sorted_services[:n])
        return top_n_services

    def get_last_two_months_cost(services):
        start_date = datetime(2023, 12, 1)
        end_date = datetime(2024, 1, 31)
        
        client = boto3.client('ce', region_name='us-east-1')

        response = client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date.strftime('%Y-%m-%d'),
                'End': end_date.strftime('%Y-%m-%d')
            },
            Granularity='MONTHLY',
            Metrics=['UnblendedCost'],
            GroupBy=[
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE'
                }
            ],
            Filter={
                "Dimensions": {
                    "Key": "RECORD_TYPE",
                    "Values": ["Usage"]
                }
            }
        )

        services_costs = defaultdict(lambda: [0.0, 0.0])

        for result in response['ResultsByTime']:
            month_start = result['TimePeriod']['Start']
            month_date = datetime.strptime(month_start, '%Y-%m-%d').replace(day=1)

            if start_date <= month_date <= end_date:
                month_diff = (end_date.year - month_date.year) * 12 + end_date.month - month_date.month
                month_index = month_diff - 1
                for group in result['Groups']:
                    service_name = group['Keys'][0]
                    if service_name in services:
                        cost = float(group['Metrics']['UnblendedCost']['Amount'])
                        if len(services_costs[service_name]) > month_index:
                            services_costs[service_name][month_index] = cost
                        else:
                            print(f"Index out of range for {service_name}: month_index={month_index}, len={len(services_costs[service_name])}")

        return services_costs

    def plot_graphs(cost_data, filename):
        services = list(cost_data.keys())
        
        width = 0.25
        fig, ax = plt.subplots()

        months = [((datetime.now() - timedelta(days=30)) - timedelta(days=30)).strftime('%b %Y'), (datetime.now() - timedelta(days=30)).strftime('%b %Y')]

        for i, service in enumerate(services):
            costs = cost_data[service]
            ax.bar(i, costs[0], width=width, label=f'{service}', color='red', align='center')
            ax.bar(i + width, costs[1], width=width, color='blue', align='center')

        ax.set_xlabel('Services')
        ax.set_ylabel('Cost')
        ax.set_title('Cost Comparison of Services for Last and This Month')
        ax.set_xticks([i + width / 2 for i in range(len(services))])
        ax.set_xticklabels(services, rotation=45, ha='right')

        ax.legend(months, title='Months', loc='upper right')

        plt.tight_layout()
        plt.savefig(filename, format='png')  # Change format to PNG
        plt.close()

    top_10_services = get_top_services(datetime.now(), datetime.now(), 10)
    costs_last_two_months = get_last_two_months_cost(top_10_services.keys())

    plot_graphs(costs_last_two_months, '/tmp/Services.png')  # Change filename extension to PNG
    print(f"PNG image has been saved successfully.")

analyze_and_plot_top_services()

def create_pdf_with_images():
    # Paths to the images and the PDF file
    image_files = ["/tmp/Services.png", "/tmp/Cost.png"]
    pdf_file = "/tmp/Cost_Service.pdf"

    # Create a PDF document
    doc = SimpleDocTemplate(pdf_file, pagesize=letter)

    # A list to store image elements
    elements = []

    # Create styles for images
    style1 = TableStyle([('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                         ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                         ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
                         ('BOX', (0, 0), (-1, -1), 0.25, colors.black)])

    style2 = TableStyle([('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                         ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                         ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
                         ('BOX', (0, 0), (-1, -1), 0.25, colors.black)])

    # Create table for first image
    table1 = Table([[Image(image_files[0], width=350, height=280)]])
    table1.setStyle(style1)

    # Create table for second image
    table2 = Table([[Image(image_files[1], width=300, height=300)]])
    table2.setStyle(style2)

    # Add tables to the elements list
    elements.append(table1)
    elements.append(Spacer(1, 24))  # Spacer to add space between images (adjust as needed)
    elements.append(table2)

    # Build the PDF document
    doc.build(elements)

    print(f"PDF file '{pdf_file}' created successfully with the images.")

# Call the function to create the PDF with images
create_pdf_with_images()


def send_email_with_attachments(attachment_paths, receiver_email):
    try:
        sender_email = "ashutosh.deshmukh@whistlemind.com"

        # Create a new SES resource
        ses_client = boto3.client('ses', region_name='us-east-1')

        # Create a multipart/mixed parent container
        msg = MIMEMultipart('mixed')
        msg['Subject'] = "Monthly Technical Report"
        msg['From'] = sender_email
        msg['To'] = receiver_email

        # Add a text/html attachment
        text = "Please find the attached monthly technical report."
        part_text = MIMEText(text, 'plain')
        msg.attach(part_text)

        # Attach files
        for attachment_path in attachment_paths:
            with open(attachment_path, 'rb') as file:
                part = MIMEApplication(file.read(), Name=os.path.basename(attachment_path))
                part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
                msg.attach(part)

        # Send the email
        response = ses_client.send_raw_email(
            Source=msg['From'],
            Destinations=[msg['To']],
            RawMessage={'Data': msg.as_string()}
        )
        print("Email sent successfully")
    except ClientError as e:
        print(f"Error in send_email_with_attachments: {str(e)}")


def lambda_handler(event, context):
    try:
        current_date = datetime.now()
        
        last_month = current_date - timedelta(days=30)
        last_month_name = last_month.strftime('%B')
        last_month_year = last_month.strftime('%Y')
        
        first_day_current_month = current_date.replace(day=1)

        end_time = first_day_current_month - timedelta(days=1)
        first_day_desired_month = end_time.replace(day=1)

        # Define your other functions here (get_rds_metrics, get_ec2_metrics, create_report, generate_content_table_pdf, retrieve_and_save_pdf, create_plots_pdf, send_email_with_attachments)

        # 1. Create report
        report_pdf_path = create_report()

        # 2. Generate content table PDF
        content_table_pdf_path = '/tmp/contents_table.pdf'
        generate_content_table_pdf(content_table_pdf_path)

        # 3. Create plots PDF
        plots_pdf_path = create_plots_pdf(rds_instance_id, ec2_instance_id, website_distid, assets_distid, web_acl_id, alb_name, first_day_desired_month, end_time)

        # 4. Retrieve and save Athena tables PDF
        athena_table_pdf_path = retrieve_and_save_pdf()
        
        # 5. Creat Cost comparison pdf
        cost_serices_pdf = '/tmp/Cost_Service.pdf'
        # 6. Combine all PDFs
        merger = PdfMerger()

        pdf_files = [report_pdf_path, content_table_pdf_path, plots_pdf_path, athena_table_pdf_path, cost_serices_pdf]

        for file in pdf_files:
            merger.append(PdfReader(file))

        combined_pdf_path = f'/tmp/AWS_Report_{last_month_name}_{last_month_year}.pdf'
        merger.write(combined_pdf_path)
        merger.close()

        # 6. Send email with combined report
        send_email_with_attachments([combined_pdf_path], 'ashutosh.deshmukh@whistlemind.com')

        return {
            'statusCode': 200,
            'body': 'Report generation and email sending completed successfully'
        }
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }

