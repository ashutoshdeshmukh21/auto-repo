import boto3
import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF
import 
from PyPDF2 import PdfReader, PdfMerger
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

ec2_instance_id = 'i-04473e2709fdf1a68'
rds_instance_id = 'database-1'
website_distid = 'E3D0HYG08X825U'
assets_distid = 'EOGCLVI5HJ52E'
waf_region = 'us-east-1'
web_acl_id = 'web-acl-id'
alb_name = 'your-application-load-balancer-name'
bucket_name = 'automated-athena-test'

current_date = datetime.now()

first_day_current_month = current_date.replace(day=1)

end_time = first_day_current_month - timedelta(days=1)
first_day_desired_month = end_time.replace(day=1)

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

# RDS Metrics
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

try:
    def create_report():
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
        
        pdf_path = f"{pdf_filename}.pdf"
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

    first_page_path = create_report()

    def generate_content_table_pdf(file_name):
        content_data = [
            ["Sr No", "Table Contents"],
            [1, "Amazon EC2 Instance\n    CPU Utilization"],
            [2, "Amazon RDS (Relational Database Service)\n    CPU Utilization"],
            [3, "AWS WAF (Web Application Firewall)\n    Blocked and Allowed Requests\n    Counted Requests"],
            [4, "CloudFront\n    CloudFront Website Request"],
            [5, "Latency Analysis\n    CacheHitRate\n    OriginLatency\n    DTO"],
            [6, "Error Analysis\n    TotalErrorRate\n    4xxErrorRate\n    5xxErrorRate"],            [8, "Security Insights\n    Top 10 IPs blocked\n    Top blocked rules\n    Top user-agents"],
            [7, "Application Load Balancer\n    Latency\n    Target Response Time\n    Host Health Status"],
            [8, "Security Insights\n    Top User-Agents\n    Top Blocked Rules\n    Top 10 IPs blocked"],
        ]
        pdf_canvas = canvas.Canvas(file_name, pagesize=A4)
        pdf_table = Table(content_data)

        style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.white),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, -1), (-1, 0), 'CENTER'),
            ('FONT', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 13),
            ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('BOX', (0, 0), (-1, -1), 0.50, colors.black),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
        ])

        pdf_table.setStyle(style)

        table_width = 900
        table_height = len(content_data) * 40

        left_margin = 70
        top_margin = 50

        table_x = left_margin
        table_y = A4[1] - top_margin - table_height

        pdf_table.wrapOn(pdf_canvas, table_width, table_height)
        pdf_table.drawOn(pdf_canvas, table_x, table_y)

        pdf_canvas.save()
        print(f"Table saved as {file_name}")

    # Calling the function with the file name
    content_file = "table_with_data.pdf"
    generate_content_table_pdf(content_file)

    # EC2 Plots
    ec2_metric_results = get_ec2_metrics(ec2_instance_id, first_day_desired_month, end_time)


    timestamps_cpu = ec2_metric_results[0]['Timestamps']
    values_cpu = ec2_metric_results[0]['Values']

    pdf = PDFWithHeaderFooter()
    pdf.add_page()

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
    description_ec2 = 'This plot shows the average CPU utilization of the EC2 instance ' + ec2_instance_id + ' for the last month.'

    pdf.image(temp_img_path_ec2, x=pdf.w / 1 - 200, y=pdf.h / 2 - 100, w=175, h=100)  # Adjust x, y, w, and h as needed
    pdf.ln(10)  # Move to the next line
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(40, 10, title_ec2)
    pdf.ln(10)  # Move to the next line
    pdf.set_font('Arial', '', 10)
    pdf.multi_cell(0, 10, description_ec2)
    pdf.ln(90)  # Move to the next line

    position_after_description = pdf.get_y()
    
    # RDS Plots
    rds_metric_results = get_rds_metrics(rds_instance_id, first_day_desired_month, end_time)

    timestamps_rds_cpu = rds_metric_results[0]['Timestamps']
    values_rds_cpu = rds_metric_results[0]['Values']

    timestamps_rds_connections = rds_metric_results[1]['Timestamps']
    values_rds_connections = rds_metric_results[1]['Values']

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
    description_rds_cpu = 'This plot shows the average CPU utilization of the RDS instance ' + rds_instance_id + ' for the last month.'

    pdf.set_y(position_after_description)

    pdf.image(temp_img_path_rds_cpu, x=pdf.w / 1 - 200, y=pdf.w / 1 - 40, w=175, h=100)  # Adjust x, y, w, and h as needed
    pdf.ln(10)  # Move to the next line
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(40, 10, title_rds_cpu)
    pdf.ln(10)  # Move to the next line
    pdf.set_font('Arial', '', 10)
    pdf.multi_cell(0, 10, description_rds_cpu)
    pdf.ln(80)  # Move to the next line

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

    def create_pdf(bucket_name, query_details, column_widths, pdf_filename):
        pdf_buffer = io.BytesIO()
        pdf = SimpleDocTemplate(pdf_buffer, pagesize=A4)
        elements = []
        last_month_numeric = last_month.month
        last_month_year = last_month.year
        
        s3 = boto3.client('s3')

        for query_name, file_suffix in query_details.items():
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
                style = TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ])

                table.setStyle(style)
                elements.append(table)
                elements.append(Spacer(1, 25))

                print(f"CSV data fetched successfully for {query_name}")
            except Exception as e:
                print(f"Error fetching object: {e}")

        pdf.build(elements)

        pdf_buffer.seek(0)

        with open(pdf_filename, 'wb') as f:
            f.write(pdf_buffer.getvalue())
        return pdf_filename
    
    if __name__ == "__main__":

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
        tables_pdf_filename = 'tables.pdf'
        create_pdf(bucket_name, query_details, column_widths, tables_pdf_filename)

    # Save the PDF to a file
    pdf_output_filename = 'merged.pdf'
    pdf.output(pdf_output_filename)

    # Close the temporary image files
    os.remove(temp_img_path_ec2)
    os.remove(temp_img_path_rds_cpu)
    os.remove(temp_img_path_alb_response_time)
    os.remove(temp_img_path_alb_host_health)
    os.remove(temp_img_path_requests)
    os.remove(temp_img_path_4xx_all_error_rate)
    os.remove(temp_img_path_5xx_all_error_rate)
    os.remove(temp_img_path_cache_hit_rate)
    os.remove(temp_img_path_counted_requests)
    os.remove(temp_img_path_origin_latency)
    os.remove(temp_img_path_combined_error_rates)

    merger = PdfMerger()
    merger.append(first_page_path)
    merger.append(content_file)
    merger.append("merged.pdf")
    merger.append(tables_pdf_filename)
    merger.write("main_final_report.pdf")
    merger.close()

    print("Final PDF generated: main_final_report.pdf")

except Exception as e:
    print(f"Error: {str(e)}")

def send_email_with_attachment(sender_email, recipient_email, subject, body_text, attachment_path):
    AWS_REGION = 'us-east-1'
    CHARSET = 'UTF-8'

    # Create a new SES resource
    client = boto3.client('ses', region_name=AWS_REGION)

    # Create the MIME message
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = recipient_email

    # Create a body in HTML format
    body = MIMEText(body_text, 'html')
    msg.attach(body)

    # Open the file in binary mode
    with open(attachment_path, 'rb') as file:
        # Attach the file with the message
        attachment = MIMEApplication(file.read(), Name='main_final_report.pdf')
        attachment['Content-Disposition'] = f'attachment; filename="{attachment_path}"'
        msg.attach(attachment)

    try:
        response = client.send_raw_email(
            Source=sender_email,
            Destinations=[recipient_email],
            RawMessage={'Data': msg.as_string()}
        )
        print("Email sent! Message ID:", response['MessageId'])
    except ClientError as e:
        print("Email not sent. Error:", e)

def lambda_handler(event, context):
    sender_email = 'ashutosh.deshmukh@whistlemind.com'
    recipient_email = 'ashutosh.deshmukh@whistlemind.com'
    subject = f"AWS Report {previous_month_name}"
    body_text = f"Please find the attached AWS Report of {previous_month_name}"

    attachment_path = '/tmp/main_final_report.pdf'

    send_email_with_attachment(sender_email, recipient_email, subject, body_text, attachment_path)

    return {
        'statusCode': 200,
        'body': 'Email sent with attachment!'
    }