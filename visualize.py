import boto3
import pandas as pd
import matplotlib.pyplot as plt
import dash
from dash import dcc, html
import plotly.express as px
from dash.dependencies import Input, Output

# Set up the S3 client
s3 = boto3.client('s3')

# Define the S3 bucket and file
bucket_name = 'your-public-bucket-name'
file_key = 'path/to/your/file.csv'

# Fetch the file from S3
response = s3.get_object(Bucket=bucket_name, Key=file_key)
csv_content = response['Body'].read().decode('utf-8')

# Load the content into a pandas DataFrame
df = pd.read_csv(pd.compat.StringIO(csv_content))

# Ensure the column name for age is correct (replace 'age' with your column name if different)
# Create the histogram using Matplotlib
plt.figure(figsize=(10, 6))
plt.hist(df['age'], bins=20, color='blue', alpha=0.7)
plt.title('Age Distribution')
plt.xlabel('Age')
plt.ylabel('Count')
plt.grid(True)

# Save the plot as an image file
plt.savefig('/tmp/age_histogram.png')

# Initialize the Dash app
app = dash.Dash(_name_)

# Layout of the dashboard
app.layout = html.Div([
    html.H1("Customer Age Distribution"),
    html.Div([
        html.Img(src='/assets/age_histogram.png', style={'width': '80%', 'height': 'auto'})
    ]),
])

# Run the app
if _name_ == '_main_':
    app.run_server(debug=True)