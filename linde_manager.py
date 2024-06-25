import requests
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
import csv
from io import StringIO
import threading
import time
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import optparse

class LindeLink():
    def __init__(self):
        self.bearer_token = None
        self.data = {}

        # Ensure the data directory exists
        if not os.path.exists(_DATADIR):
            os.makedirs(_DATADIR)

        self.load_credentials()
        self.setup_logging()
        self.get_bearer_token()

    def setup_logging(self):
        self.log_file = os.path.join(_DATADIR, 'data_log.csv')
        log_dir = os.path.dirname(self.log_file)

        # Ensure the log file has a header if it doesn't exist
        if not os.path.exists(self.log_file) or os.path.getsize(self.log_file) == 0:
            with open(self.log_file, 'w') as file:
                file.write('messageTime,bank,lastChange,content\n')

    def load_credentials(self):
        cred_file = os.path.join(_DATADIR, "credentials.json")
        with open(cred_file, 'r') as file:
            self.credentials = json.load(file)

    def get_bearer_token(self):
        username = self.credentials['username']
        password = self.credentials['password']
        client_id = self.credentials['client_id']
        client_secret = self.credentials['client_secret']
        redirect_uri = self.credentials['redirect_uri']
        auth_url = 'https://authentication.dfs.linde.com/auth/realms/digital-family/protocol/openid-connect/auth'
        
        # Initialize a session
        session = requests.Session()
        
        # Step 1: Initial authentication request
        auth_params = {
            'response_type': 'code',
            'client_id': client_id,
            'state': '',
            'redirect_uri': redirect_uri,
            'scope': 'openid profile email'
        }
        auth_response = session.get(auth_url, params=auth_params)
        
        # Step 2: Parse the login form and submit credentials
        login_page_soup = BeautifulSoup(auth_response.text, 'html.parser')
        login_form = login_page_soup.find('form')
        
        login_url = login_form['action']
        hidden_inputs = login_form.find_all('input', type='hidden')
        login_payload = {input_['name']: input_['value'] for input_ in hidden_inputs}
        login_payload.update({
            'username': username,
            'password': password
        })
        
        login_response = session.post(login_url, data=login_payload)
        
        # Step 3: Follow the redirection to capture the authorization code
        redirect_response = session.get(login_response.url, allow_redirects=True)
        parsed_url = urlparse(redirect_response.url)
        auth_code = parse_qs(parsed_url.query).get('code')
        
        if not auth_code:
            print('Failed to retrieve authorization code.')
        else:
            auth_code = auth_code[0]
            print(f'Authorization Code: {auth_code}')
        
            # Step 4: Exchange authorization code for access token
            token_url = 'https://authentication.dfs.linde.com/auth/realms/digital-family/protocol/openid-connect/token'
            token_payload = {
                'grant_type': 'authorization_code',
                'code': auth_code,
                'redirect_uri': redirect_uri,
                'client_id': client_id,
                'client_secret': client_secret
            }
            token_response = session.post(token_url, data=token_payload)
            
            if token_response.status_code == 200:
                token_data = token_response.json()
                self.bearer_token = { 
                                      "token" : token_data.get('access_token'), 
                                      "last_obtained" : datetime.now()
                                    }
    
            else:
                print(f'Failed to obtain access token. Status code: {token_response.status_code}')
                print(token_response.json())

    def get_data(self):
        
        #get a new token every hour
        if datetime.now() - self.bearer_token["last_obtained"] >= timedelta(minutes=60):
            self.get_bearer_token()
        
        # URL to fetch the JSON file
        url = "https://digitalmanifold.be.dfs.linde.com/api/v1/csv/digitalmanifolddetails/download?country=826"
    
        # Headers for the request
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-GB,en;q=0.9,it-IT;q=0.8,it;q=0.7,en-US;q=0.6,fr;q=0.5",
            "Authorization": "Bearer " + self.bearer_token["token"],
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Host": "digitalmanifold.be.dfs.linde.com",
            "Origin": "https://dfs.linde.com",
            "Referer": "https://dfs.linde.com/",
            "Sec-Ch-Ua": '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "Linux",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
        }
    
        # Make the GET request
        response = requests.get(url, headers=headers)
    
        # Check if the request was successful
        if response.status_code == 200:
            # Decode the byte string to a regular string
            csv_data = response.content.decode('utf-8')
            
            # Create a DictReader object
            csv_reader = csv.DictReader(StringIO(csv_data))
            
            # Read and store each row from the CSV into a dictionary
            for row in csv_reader:
                json_dict = dict(row)
            
            self.data = json_dict

            # Log the required data
            with open(self.log_file, 'a') as file:
                file.write(f"{json_dict.get('messageTimeLeft')},left,{json_dict.get('lastChangeLeft')},{json_dict.get('leftBankContents')}\n")
                file.write(f"{json_dict.get('messageTimeRight')},right,{json_dict.get('lastChangeRight')},{json_dict.get('rightBankContents')}\n")
 
             # Check for low content and send alert email if needed
            
            if int(json_dict.get('leftBankContents', 0)) <= 10 and _ALERT:
                self.check_and_send_alert('left')
            if int(json_dict.get('rightBankContents', 0)) <= 10 and _ALERT:
                self.check_and_send_alert('right')

            return json_dict
        else:
            return False

    def start_data_collection(self):
        self.get_data()
        threading.Timer(3600, self.start_data_collection).start()  # Scheduled to run every hour

    def send_alert_email(self, bank):
        msg = MIMEMultipart()
        msg['From'] = self.credentials['username']
        msg['To'] = self.credentials['BOC_address']
        msg['CC'] = self.credentials['username']
        msg['Subject'] = "Please deliver 40-VK to the cage between SECB and Flowers."
        
        body = (f"Dear BOC team,\n\n"
                "Please deliver 2x 40-VK cylinders to the cage space between SEC and FLOWERS building, SKEN. "
                f"To be charged on Service PO Number: {self.credentials['PO']}.\n"
                f"Please collect the two empty cylinders on the {bank} bank.\n\n"
                "Many thanks,\n"
                "Giorgio Gilestro")
        msg.attach(MIMEText(body, 'plain'))

        # Establish SMTP connection and send the email
        with smtplib.SMTP(self.credentials['smtp_server'], self.credentials['smtp_port']) as server:
            server.sendmail(self.credentials['username'], self.credentials['BOC_address'], msg.as_string())
            print(f"Alert email sent to {self.credentials['BOC_address']} for {bank} bank.")

        # Log the last alert date and time with bank
        self.last_alert_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        with open(os.path.join(_DATADIR, 'last_alert.log'), 'a') as file:
            file.write(f"{self.last_alert_time},{bank}\n")

    def check_and_send_alert(self, bank):
        alert_sent = False
        last_alert_file = os.path.join(_DATADIR, 'last_alert.log')
        if os.path.exists(last_alert_file):
            with open(last_alert_file, 'r') as file:
                for line in file:
                    last_time_str, last_bank = line.strip().split(',')
                    last_time = datetime.strptime(last_time_str, '%Y-%m-%d %H:%M')
                    if last_bank == bank and (datetime.now() - last_time) < timedelta(hours=72):
                        alert_sent = True
                        break
        if not alert_sent:
            self.send_alert_email(bank)



class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(self.generate_html().encode('utf-8'))
        elif self.path == '/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                'leftBankContents': link.data.get('leftBankContents'),
                'rightBankContents': link.data.get('rightBankContents'),
                'messageTimeLeft': link.data.get('messageTimeLeft'),
                'messageTimeRight': link.data.get('messageTimeRight')
            }
            self.wfile.write(json.dumps(response).encode('utf-8'))
        elif self.path == '/plot':
            self.send_response(200)
            self.send_header('Content-type', 'image/png')
            self.end_headers()
            self.generate_plot()
            with open('plot.png', 'rb') as file:
                self.wfile.write(file.read())
        else:
            self.send_response(404)
            self.end_headers()

    def generate_html(self):
        # Generate the current status table
        left_content = int(link.data.get('leftBankContents', 0))
        right_content = int(link.data.get('rightBankContents', 0))
        left_message_time = link.data.get('messageTimeLeft', 'N/A')
        right_message_time = link.data.get('messageTimeRight', 'N/A')
        left_last_change = link.data.get('lastChangeLeft', 'N/A')
        right_last_change = link.data.get('lastChangeRight', 'N/A')

        def get_color(value):
            if value > 70:
                return 'background-color: #9FE481;'  # pastel green
            elif value > 10:
                return 'background-color: #F68C70;'  # pastel orange
            else:
                return 'background-color: #D0342C;'  # pastel red

        def get_date_color(date_str):
            if date_str == 'N/A':
                return ''
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
                delta = datetime.now() - date_obj
                if delta.days > 3:
                    return 'background-color: #D0342C;'  # pastel red
                elif delta.days > 1:
                    return 'background-color: #F68C70;'  # pastel orange
                else:
                    return ''
            except ValueError:
                return ''

        def format_date(date_str):
            if date_str == 'N/A':
                return date_str
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
                return date_obj.strftime('%Y-%m-%d %H:%M')
            except ValueError:
                return date_str

        def get_icon(value):
            if value > 70:
                return '<i class="fa fa-tachometer-full" style="color: #9FE481;"></i>'  # pastel green
            elif value > 10:
                return '<i class="fa fa-tachometer-half" style="color: #F68C70;"></i>'  # pastel orange
            else:
                return '<i class="fa fa-tachometer" style="color: #D0342C;"></i>'  # pastel red

        left_color = get_color(left_content)
        right_color = get_color(right_content)
        left_message_time_color = get_date_color(left_message_time)
        right_message_time_color = get_date_color(right_message_time)
        left_last_change_color = get_date_color(left_last_change)
        right_last_change_color = get_date_color(right_last_change)

        left_message_time_formatted = format_date(left_message_time)
        right_message_time_formatted = format_date(right_message_time)
        left_last_change_formatted = format_date(left_last_change)
        right_last_change_formatted = format_date(right_last_change)

        left_icon = get_icon(left_content)
        right_icon = get_icon(right_content)

        # Read the last alert date and time
        last_alert_message = 'No alerts sent yet'
        last_alert_file = os.path.join(_DATADIR, 'last_alert.log')
        if os.path.exists(last_alert_file):
            with open(last_alert_file, 'r') as file:
                lines = file.readlines()
                last_entry = lines[-1].strip()  # Get the latest entry
                last_alert_time, bank_side = last_entry.split(',')  # Split the entry into time and bank side
                last_alert_message = f"The last alert was sent on {last_alert_time} for the {bank_side} bank"


        html = f"""
        <html>
        <head>
            <title>Bank Status and Plot</title>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                table {{
                    width: 50%;
                    max-width: 600px;
                    border-collapse: collapse;
                    margin: 20px auto;
                }}
                th, td {{
                    border: 1px solid #dddddd;
                    text-align: left;
                    padding: 8px;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
                .center {{ text-align: center; }}
            </style>
            <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
        </head>
        <body>
            <h2 class="center">FlyRoom CO2 Bank Status</h2>
            <p class="center">{last_alert_message}</p>
            <table>
                <tr>
                    <th>Bank</th>
                    <th>Contents</th>
                    <th>Message Time</th>
                    <th>Last Change</th>
                </tr>
                <tr>
                    <td>Left</td>
                    <td style="{left_color}">{left_content} {left_icon}</td>
                    <td style="{left_message_time_color}">{left_message_time_formatted}</td>
                    <td>{left_last_change_formatted}</td>
                </tr>
                <tr>
                    <td>Right</td>
                    <td style="{right_color}">{right_content} {right_icon}</td>
                    <td style="{right_message_time_color}">{right_message_time_formatted}</td>
                    <td>{right_last_change_formatted}</td>
                </tr>
            </table>
            <div class="center">
                <img src="/plot" alt="Bank Contents Plot">
            </div>
        </body>
        </html>
        """
        return html




    def generate_plot(self, resampling_value='3H', days=10):
        # Load data from CSV log file
        df = pd.read_csv(link.log_file, parse_dates=['messageTime'])

        # Filter data for the past 7 days
        now = datetime.now()
        time_window = now - timedelta(days=days)
        df_filtered = df[df['messageTime'] >= time_window]
        
        # Ensure 'content' column is numeric
        df_filtered['content'] = pd.to_numeric(df_filtered['content'], errors='coerce')

        # Separate left and right banks
        df_left = df_filtered[df_filtered['bank'] == 'left'].set_index('messageTime')
        df_right = df_filtered[df_filtered['bank'] == 'right'].set_index('messageTime')

        # Interpolate data
        df_left_interpolated = df_left.interpolate(method='time')
        df_right_interpolated = df_right.interpolate(method='time')

        # Read the last alert dates and times
        alert_times = {'left': [], 'right': []}
        last_alert_file = os.path.join(_DATADIR, 'last_alert.log')
        if os.path.exists(last_alert_file):
            with open(last_alert_file, 'r') as file:
                for line in file:
                    last_time_str, last_bank = line.strip().split(',')
                    last_time = datetime.strptime(last_time_str, '%Y-%m-%d %H:%M')
                    alert_times[last_bank].append(last_time)

        # Create a figure with two subplots
        fig, axs = plt.subplots(2, 1, figsize=(10, 6), sharex=True)

        # Plot left bank data on the first subplot
        axs[0].plot(df_left.index, df_left['content'], 'o', label='Left Bank Real Data', color='#1f77b4')
        axs[0].plot(df_left_interpolated.index, df_left_interpolated['content'], '-', label='Left Bank Interpolated', color='#1f77b4', alpha=0.5)
        for alert_time in alert_times['left']:
            axs[0].axvline(x=alert_time, color='#cfcfc4', linestyle='--', label='_nolegend_')
        axs[0].set_ylabel('Left Bank Content')
        axs[0].set_ylim([-5, 105])
        axs[0].grid(True)

        # Plot right bank data on the second subplot
        axs[1].plot(df_right.index, df_right['content'], 'x', label='Right Bank Real Data', color='#ff7f0e')
        axs[1].plot(df_right_interpolated.index, df_right_interpolated['content'], '-', label='Right Bank Interpolated', color='#ff7f0e', alpha=0.5)
        for alert_time in alert_times['right']:
            axs[1].axvline(x=alert_time, color='#cfcfc4', linestyle='--', label='_nolegend_')
        axs[1].set_xlabel('Time')
        axs[1].set_ylabel('Right Bank Content')
        axs[1].set_ylim([-5, 105])
        axs[1].grid(True)

        # Add a single legend below the plots
        handles0, labels0 = axs[0].get_legend_handles_labels()
        handles1, labels1 = axs[1].get_legend_handles_labels()
        handles = handles0 + handles1 + [plt.Line2D([0], [0], color='#cfcfc4', linestyle='--', label='Alert Sent')]
        labels = labels0 + labels1 + ['Alert Sent']
        fig.legend(handles=handles, labels=labels, loc='lower center', ncol=3)  # Move legend to bottom

        axs[0].set_title(f"Bank Contents for the Past {days} Days")

        plt.xlim(time_window, now)  # Ensure x-axis limits are set correctly
        plt.tight_layout(rect=[0, 0.1, 1, 0.95])  # Adjust layout to make space for the legend at the bottom
        plt.savefig('plot.png')
        plt.close()






def run_server(server_class=HTTPServer, handler_class=RequestHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting httpd server on port {port}')
    httpd.serve_forever()

if __name__ == '__main__':

    parser = optparse.OptionParser()
    parser.add_option("-p", "--path", dest="path", default="./data/", help="Set the path to the data folder")
    parser.add_option("--notify", dest="notify", default=False, help="Notify via email", action="store_true")
    parser.add_option("--port", dest="port", default=8000, help="Port for the webserver")

    (options, args) = parser.parse_args()

    option_dict = vars(options)
    _DATADIR = option_dict["path"]
    _ALERT = option_dict["notify"]
    _PORT = option_dict["port"]

    link = LindeLink()
    link.start_data_collection()
    run_server(port=_PORT)
