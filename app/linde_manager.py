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

_DEFAULT_PORT = 8000
_DATADIR = "./data/"
_ALERT = False


class LindeLink():
    def __init__(self, debug=False):
        self.bearer_token = None
        self.data = {}
        self.email_status = {'connected': True, 'last_check': None, 'error': None}

        # Ensure the data directory exists
        if not os.path.exists(_DATADIR):
            os.makedirs(_DATADIR)
        self.log_file = os.path.join(_DATADIR, 'data_log.csv')
        self.last_alert_file = os.path.join(_DATADIR, 'last_alert.log')

        self.load_credentials()
        self.load_pos()
        self.setup_logging()
        self.get_bearer_token()
        self.check_email_connection()

    def load_pos(self):
        """
        Load purchase orders from pos.json. Each PO has number, email, ratio,
        and optional created/expires dates. If pos.json is absent, synthesize
        a single-entry list from credentials.json so existing setups keep
        working unchanged.
        """
        pos_file = os.path.join(_DATADIR, 'pos.json')
        if os.path.exists(pos_file):
            with open(pos_file, 'r') as file:
                data = json.load(file)
                self.pos = data.get('pos', [])
        else:
            self.pos = [{
                'number': self.credentials.get('PO', 'N/A'),
                'email': self.credentials.get('smtp_recipient', ''),
                'ratio': 1,
                'created': None,
                'expires': None,
            }]

    def get_po_usage(self):
        """
        Count past uses of each configured PO from last_alert.log. Only lines
        recorded with the new 3-column format contribute; legacy 2-column
        lines are ignored (no PO recorded at that time).
        """
        usage = {po['number']: 0 for po in self.pos}
        if not os.path.exists(self.last_alert_file):
            return usage
        with open(self.last_alert_file, 'r') as file:
            for line in file:
                parts = line.strip().split(',')
                if len(parts) >= 3 and parts[2] in usage:
                    usage[parts[2]] += 1
        return usage

    def select_po(self):
        """
        Pick the next PO to use by weighted round-robin: among non-expired POs
        with ratio > 0, choose the one whose used_count / ratio is smallest,
        so over time usage converges to the configured ratios.

        Returns:
            dict | None: The chosen PO, or None if no PO is currently usable.
        """
        today = datetime.now().date()

        def is_valid(po):
            if po.get('ratio', 1) <= 0:
                return False
            expires = po.get('expires')
            if not expires:
                return True
            try:
                return datetime.strptime(expires, '%Y-%m-%d').date() >= today
            except ValueError:
                return True

        candidates = [po for po in self.pos if is_valid(po)]
        if not candidates:
            return None

        usage = self.get_po_usage()
        return min(candidates, key=lambda po: usage.get(po['number'], 0) / po.get('ratio', 1))

    def setup_logging(self):
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

            # Check for no data transfer and send alert email if needed
            self.check_message_time_freshness()

            return json_dict
        else:
            return False

    def start_data_collection(self):
        self.get_data()
        
        # Check for stale data even if get_data() fails
        if self.data:
            self.check_message_time_freshness()
        
        threading.Timer(3600, self.start_data_collection).start()  # Scheduled to run every hour

    def check_message_time_freshness(self):
        """
        Check if the messageTime for either bank is older than 3 days.
        If so, send an alert email to the configured smtp_sender.
        """
        current_time = datetime.now()
        alert_sent = False
        
        # Check left bank message time
        if 'messageTimeLeft' in self.data:
            try:
                left_time = datetime.strptime(self.data['messageTimeLeft'], '%Y-%m-%dT%H:%M:%S')
                delta_left = current_time - left_time
                
                if delta_left.days > 3:
                    self.send_data_staleness_alert('left', delta_left.days)
                    alert_sent = True
            except (ValueError, TypeError):
                logging.error(f"Error parsing left bank message time: {self.data.get('messageTimeLeft')}")
        
        # Check right bank message time
        if 'messageTimeRight' in self.data:
            try:
                right_time = datetime.strptime(self.data['messageTimeRight'], '%Y-%m-%dT%H:%M:%S')
                delta_right = current_time - right_time
                
                if delta_right.days > 3:
                    # Only send second alert if it's not the same general issue
                    if not alert_sent:
                        self.send_data_staleness_alert('right', delta_right.days)
                    # If we already sent an alert for left bank, let's add right bank to the log
                    elif alert_sent:
                        with open(os.path.join(_DATADIR, 'staleness_alert.log'), 'a') as file:
                            file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M')},right,{delta_right.days}\n")
            except (ValueError, TypeError):
                logging.error(f"Error parsing right bank message time: {self.data.get('messageTimeRight')}")

    def send_data_staleness_alert(self, bank, days_old):
        """
        Send an alert email when data is stale (more than 3 days old).
        
        Args:
            bank: The bank (left/right) with stale data
            days_old: Number of days since the last data update
        """
        # Check if we've already sent an alert in the last 24 hours
        alert_sent = False
        alert_log_file = os.path.join(_DATADIR, 'staleness_alert.log')
        
        if os.path.exists(alert_log_file):
            with open(alert_log_file, 'r') as file:
                for line in file:
                    last_time_str, last_bank, _ = line.strip().split(',')
                    last_time = datetime.strptime(last_time_str, '%Y-%m-%d %H:%M')
                    if (datetime.now() - last_time) < timedelta(hours=24):
                        alert_sent = True
                        break
        
        if not alert_sent:
            try:
                msg = MIMEMultipart()
                msg['From'] = self.credentials['smtp_sender']
                msg['To'] = self.credentials['smtp_sender']  # Send to the sender as requested
                msg['Subject'] = "ALERT: CO2 Bank Data Staleness"
                
                body = (f"Dear Administrator,\n\n"
                        f"The CO2 bank monitoring system has detected stale data for the {bank} bank.\n"
                        f"The last data update was {days_old} days ago.\n\n"
                        f"This may indicate a connectivity issue with the Linde Digital Manifold system.\n"
                        f"Please check the system connection and authentication.\n\n"
                        f"This is an automated message from the CO2 Bank Monitoring System.")
                
                msg.attach(MIMEText(body, 'plain'))
                
                with smtplib.SMTP(self.credentials['smtp_server'], self.credentials['smtp_port'], timeout=10) as server:
                    if eval(self.credentials['use_auth']):
                        logging.info("Authenticating to SMTP server")
                        server.login(self.credentials['smtp_username'], self.credentials['smtp_password'])
                    
                    server.sendmail(self.credentials['smtp_sender'], [self.credentials['smtp_sender']], msg.as_string())
                    logging.info(f"Data staleness alert email sent to {self.credentials['smtp_sender']} for {bank} bank.")

                    # Update email status to indicate successful send
                    self.email_status = {
                        'connected': True,
                        'last_check': datetime.now(),
                        'error': None
                    }

                    # Log the alert
                    with open(alert_log_file, 'a') as file:
                        file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M')},{bank},{days_old}\n")
                    
            except Exception as e:
                logging.error(f"Error sending data staleness alert: {e}")
                self.email_status = {
                    'connected': False,
                    'last_check': datetime.now(),
                    'error': f"Error: {str(e)}"
                }

    def send_alert_email(self, bank, test=False):
        po = self.select_po()
        if po is None:
            logging.error(f"No valid PO available to send alert for {bank} bank.")
            return
        po_number = po.get('number', 'N/A')
        po_email = po.get('email') or self.credentials.get('smtp_recipient')

        try:
            msg = MIMEMultipart()
            msg['From'] = msg['Cc'] = self.credentials['smtp_sender']

            if test:
                msg['To'] = self.credentials['smtp_sender']
            else:
                msg['To'] = po_email

            msg['Subject'] = "Please deliver 40-VK to the cage between SECB and Flowers."

            body = (f"Dear BOC team,\n\n"
                    "Please deliver 2x 40-VK cylinders to the cage space between SEC and FLOWERS building, SKEN. "
                    f"To be charged on Service PO Number: {po_number}.\n"
                    f"Please collect the two empty cylinders on the {bank} bank.\n\n"
                    "Many thanks,\n"
                    "Giorgio Gilestro")
            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(self.credentials['smtp_server'], self.credentials['smtp_port'], timeout=10) as server:
                if eval(self.credentials['use_auth']):
                    #server.starttls()
                    logging.info("Authenticating to SMTP server")
                    server.login(self.credentials['smtp_username'], self.credentials['smtp_password'])

                # Sending to both To and Cc recipients
                recipients = [msg['To'], msg['Cc']]
                server.sendmail(self.credentials['smtp_sender'], recipients, msg.as_string())
                logging.info(f"Alert email sent to {msg['To']} and cc'd {msg['Cc']} for {bank} bank.")

                # Update email status to indicate successful send
                self.email_status = {
                    'connected': True,
                    'last_check': datetime.now(),
                    'error': None
                }

                # Log the alert with bank and PO so usage drives future rotation
                self.last_alert_time = datetime.now().strftime('%Y-%m-%d %H:%M')
                with open(self.last_alert_file, 'a') as file:
                    file.write(f"{self.last_alert_time},{bank},{po_number}\n")

        except smtplib.SMTPException as e:
            logging.error(f"SMTP error occurred while sending email for {bank} bank: {e}")
            self.email_status = {
                'connected': False,
                'last_check': datetime.now(),
                'error': f"SMTP error: {str(e)}"
            }

        except Exception as e:
            logging.error(f"Unexpected error occurred while sending email for {bank} bank: {e}")
            self.email_status = {
                'connected': False,
                'last_check': datetime.now(),
                'error': f"Error: {str(e)}"
            }


    def check_and_send_alert(self, bank):
        alert_sent = False
        if os.path.exists(self.last_alert_file):
            with open(self.last_alert_file, 'r') as file:
                for line in file:
                    parts = line.strip().split(',')
                    if len(parts) < 2:
                        continue
                    last_time_str, last_bank = parts[0], parts[1]
                    last_time = datetime.strptime(last_time_str, '%Y-%m-%d %H:%M')
                    if last_bank == bank and (datetime.now() - last_time) < timedelta(hours=72):
                        alert_sent = True
                        break
        if not alert_sent:
            self.send_alert_email(bank)

    def get_orders_history(self):
        """
        Read last_alert.log and return the full order history plus per-bank
        statistics, so unusually short gaps between orders (a leak indicator)
        can be highlighted in the dashboard.

        Returns:
            tuple: (orders, median_interval) where
                orders is a list of (datetime, bank, days_since_previous_same_bank)
                sorted oldest-first,
                median_interval is a dict {'left': float|None, 'right': float|None}
                holding the median days between consecutive orders for each bank,
                computed over the full history.
        """
        last_alert_file = os.path.join(_DATADIR, 'last_alert.log')
        median_interval = {'left': None, 'right': None}
        if not os.path.exists(last_alert_file):
            return [], median_interval

        orders = []
        with open(last_alert_file, 'r') as file:
            for line in file:
                parts = line.strip().split(',')
                if len(parts) < 2:
                    continue
                try:
                    dt = datetime.strptime(parts[0], '%Y-%m-%d %H:%M')
                except ValueError:
                    continue
                orders.append((dt, parts[1]))
        orders.sort(key=lambda x: x[0])

        # Median interval per bank over the full history.
        # Reason: per-bank median is the right baseline because each bank is
        # consumed independently, so a leak shows up as a short same-bank gap.
        by_bank = {'left': [], 'right': []}
        for dt, bank in orders:
            if bank in by_bank:
                by_bank[bank].append(dt)
        for bank, dates in by_bank.items():
            if len(dates) < 2:
                continue
            intervals = sorted((dates[i] - dates[i - 1]).total_seconds() / 86400
                               for i in range(1, len(dates)))
            mid = len(intervals) // 2
            if len(intervals) % 2 == 0:
                median_interval[bank] = (intervals[mid - 1] + intervals[mid]) / 2
            else:
                median_interval[bank] = intervals[mid]

        # Annotate each order with days since the previous same-bank order.
        enriched = []
        last_seen = {}
        for dt, bank in orders:
            prev = last_seen.get(bank)
            days_since = (dt - prev).total_seconds() / 86400 if prev else None
            enriched.append((dt, bank, days_since))
            last_seen[bank] = dt

        return enriched, median_interval

    def check_email_connection(self):
        """
        Test the SMTP connection and update email_status.
        This method attempts to connect to the SMTP server to verify email functionality.
        """
        try:
            with smtplib.SMTP(self.credentials['smtp_server'], self.credentials['smtp_port'], timeout=10) as server:
                if eval(self.credentials['use_auth']):
                    server.login(self.credentials['smtp_username'], self.credentials['smtp_password'])

                self.email_status = {
                    'connected': True,
                    'last_check': datetime.now(),
                    'error': None
                }
                logging.info("Email connection test successful")

        except smtplib.SMTPAuthenticationError as e:
            self.email_status = {
                'connected': False,
                'last_check': datetime.now(),
                'error': f"Authentication failed: {str(e)}"
            }
            logging.error(f"Email authentication error: {e}")

        except smtplib.SMTPConnectError as e:
            self.email_status = {
                'connected': False,
                'last_check': datetime.now(),
                'error': f"Connection failed: {str(e)}"
            }
            logging.error(f"Email connection error: {e}")

        except Exception as e:
            self.email_status = {
                'connected': False,
                'last_check': datetime.now(),
                'error': f"Error: {str(e)}"
            }
            logging.error(f"Email connection test failed: {e}")



class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
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
                'messageTimeRight': link.data.get('messageTimeRight'),
                'emailStatus': {
                    'connected': link.email_status['connected'],
                    'lastCheck': link.email_status['last_check'].isoformat() if link.email_status['last_check'] else None,
                    'error': link.email_status['error']
                }
            }
            self.wfile.write(json.dumps(response).encode('utf-8'))
        elif self.path == '/plot':
            self.send_response(200)
            self.send_header('Content-type', 'image/png')
            self.end_headers()
            self.generate_plot()
            plot_path = os.path.join(_DATADIR, 'plot.png')
            with open(plot_path, 'rb') as file:
                self.wfile.write(file.read())
        else:
            self.send_response(404)
            self.end_headers()

    def render_pos_tab(self):
        """
        Render the Purchase Orders tab: each configured PO with its reference
        email, ratio, cumulative usage count, and create/expire dates. Expired
        rows are highlighted, and the next PO that select_po() would return is
        flagged so the rotation is visible at a glance.

        Returns:
            str: HTML fragment for the POs tab.
        """
        today = datetime.now().date()
        usage = link.get_po_usage()
        next_po = link.select_po()
        next_number = next_po['number'] if next_po else None

        rows = ''
        for po in link.pos:
            number = po.get('number', 'N/A')
            email = po.get('email') or '—'
            ratio = po.get('ratio', 1)
            created = po.get('created') or '—'
            expires = po.get('expires')

            expires_style = ''
            expires_display = expires or '—'
            if expires:
                try:
                    if datetime.strptime(expires, '%Y-%m-%d').date() < today:
                        expires_style = 'background-color: #D0342C; color: white;'
                        expires_display = f"{expires} (expired)"
                except ValueError:
                    pass

            marker = ' <span title="Next PO in rotation" style="color:#3CA055;">&#9733;</span>' if number == next_number else ''
            rows += (
                f'<tr>'
                f'<td>{number}{marker}</td>'
                f'<td>{email}</td>'
                f'<td>{ratio}</td>'
                f'<td>{usage.get(number, 0)}</td>'
                f'<td>{created}</td>'
                f'<td style="{expires_style}">{expires_display}</td>'
                f'</tr>'
            )

        if not rows:
            rows = '<tr><td colspan="6" class="center">No purchase orders configured.</td></tr>'

        return f"""
        <h2 class="center">Purchase Orders</h2>
        <p class="center"><small>Each alert email picks the PO with the lowest
        used / ratio score among non-expired entries, so over time usage
        converges to the configured ratios. The &#9733; marks the PO that will
        be used next.</small></p>
        <table>
            <tr>
                <th>PO Number</th>
                <th>Reference Email</th>
                <th>Ratio</th>
                <th>Used</th>
                <th>Created</th>
                <th>Expires</th>
            </tr>
            {rows}
        </table>
        """

    def render_orders_timeline(self, window_days=365):
        """
        Render the order history of the past `window_days` as an inline SVG
        timeline. Left-bank orders sit above the axis, right-bank below; each
        marker is colored by how short the gap to the previous same-bank order
        is, relative to that bank's median (a short gap is the leading
        indicator of a slow leak).

        Args:
            window_days (int): Size of the displayed time window in days.

        Returns:
            str: HTML fragment containing the SVG and a colour legend, or an
            empty string if there is nothing to show.
        """
        all_orders, median_interval = link.get_orders_history()
        if not all_orders:
            return ''

        now = datetime.now()
        start = now - timedelta(days=window_days)
        visible = [(dt, bank, days) for dt, bank, days in all_orders if dt >= start]
        if not visible:
            return ''

        # SVG geometry
        width, height = 820, 160
        m_left, m_right, m_top, m_bottom = 55, 20, 20, 40
        plot_w = width - m_left - m_right
        plot_h = height - m_top - m_bottom
        axis_y = m_top + plot_h / 2
        total_span = (now - start).total_seconds()

        def x_for(dt):
            frac = (dt - start).total_seconds() / total_span
            return m_left + frac * plot_w

        def color_for(days_since, bank):
            median = median_interval.get(bank)
            if days_since is None:
                return '#888888'  # first recorded order — no comparison
            if median is None:
                return '#1f77b4'
            ratio = days_since / median
            if ratio < 0.5:
                return '#D0342C'  # red — likely leak
            if ratio < 0.75:
                return '#F68C70'  # orange — faster than usual
            return '#3CA055'      # green — at or near baseline

        # Month tick marks and labels along the axis
        ticks = []
        cur = datetime(start.year, start.month, 1)
        if cur < start:
            cur = datetime(cur.year + (cur.month == 12), 1 if cur.month == 12 else cur.month + 1, 1)
        while cur <= now:
            x = x_for(cur)
            ticks.append(
                f'<line x1="{x:.1f}" y1="{axis_y - 4}" x2="{x:.1f}" y2="{axis_y + 4}" stroke="#aaa" />'
            )
            label = cur.strftime('%b %y') if cur.month == 1 else cur.strftime('%b')
            ticks.append(
                f'<text x="{x:.1f}" y="{axis_y + plot_h / 2 + 14}" '
                f'text-anchor="middle" font-size="11" fill="#555">{label}</text>'
            )
            cur = datetime(cur.year + (cur.month == 12), 1 if cur.month == 12 else cur.month + 1, 1)

        # Bank-side labels
        side_labels = [
            f'<text x="{m_left - 8}" y="{axis_y - 14}" text-anchor="end" '
            f'dominant-baseline="middle" font-size="11" fill="#555">Left</text>',
            f'<text x="{m_left - 8}" y="{axis_y + 14}" text-anchor="end" '
            f'dominant-baseline="middle" font-size="11" fill="#555">Right</text>',
        ]

        # Order markers
        markers = []
        for dt, bank, days_since in visible:
            x = x_for(dt)
            cy = axis_y - 14 if bank == 'left' else axis_y + 14
            color = color_for(days_since, bank)
            median = median_interval.get(bank)
            if days_since is None:
                detail = f"first recorded {bank}-bank order"
            else:
                ratio_str = f", {days_since / median * 100:.0f}% of {median:.0f}d median" if median else ""
                detail = f"{days_since:.1f}d since previous {bank}-bank order{ratio_str}"
            tooltip = f"{dt.strftime('%Y-%m-%d %H:%M')} | {bank} bank | {detail}"
            markers.append(
                f'<circle cx="{x:.1f}" cy="{cy:.1f}" r="5" fill="{color}" '
                f'stroke="#333" stroke-width="0.5"><title>{tooltip}</title></circle>'
            )

        axis_line = (
            f'<line x1="{m_left}" y1="{axis_y}" x2="{m_left + plot_w}" y2="{axis_y}" '
            f'stroke="#666" stroke-width="1" />'
        )

        left_med = median_interval.get('left')
        right_med = median_interval.get('right')
        left_med_str = f"{left_med:.1f} days" if left_med is not None else 'N/A'
        right_med_str = f"{right_med:.1f} days" if right_med is not None else 'N/A'

        svg = (
            f'<svg viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" '
            f'style="max-width:100%;height:auto;">'
            f'{axis_line}'
            f'{"".join(ticks)}'
            f'{"".join(side_labels)}'
            f'{"".join(markers)}'
            f'</svg>'
        )

        return f"""
        <div class="timeline-container">
            <h3>Orders over the past {window_days // 30} months</h3>
            <p><small>Median interval between same-bank orders &mdash;
            Left: {left_med_str}, Right: {right_med_str}.
            Hover a dot for details.</small></p>
            {svg}
            <div class="timeline-legend">
                <span><i class="dot" style="background:#D0342C"></i>&lt; 50% of median (likely leak)</span>
                <span><i class="dot" style="background:#F68C70"></i>50&ndash;75% of median</span>
                <span><i class="dot" style="background:#3CA055"></i>&ge; 75% of median</span>
                <span><i class="dot" style="background:#888888"></i>first recorded</span>
            </div>
        </div>
        """

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
                parts = last_entry.split(',')
                if len(parts) >= 2:
                    last_alert_time, bank_side = parts[0], parts[1]
                    last_alert_message = f"The last alert was sent on {last_alert_time} for the {bank_side} bank"

        # Orders timeline (past 12 months) — short same-bank gaps may indicate a leak
        orders_html = self.render_orders_timeline(window_days=365)

        # Purchase Orders tab content
        pos_html = self.render_pos_tab()


        # Check if email connection has problems
        email_alert_html = ''
        if not link.email_status['connected']:
            error_msg = link.email_status.get('error', 'Unknown error')
            last_check = link.email_status.get('last_check')
            if last_check:
                last_check_str = last_check.strftime('%Y-%m-%d %H:%M')
            else:
                last_check_str = 'Never'

            email_alert_html = f"""
            <div class="email-alert">
                <i class="fa fa-exclamation-triangle"></i>
                <strong>Email Connection Problem!</strong>
                <p>Email alerts are currently not working. Last check: {last_check_str}</p>
                <p class="error-detail">Error: {error_msg}</p>
            </div>
            """

        html = f"""
        <html>
        <head>
            <title>Bank Status and Plot</title>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .email-alert {{
                    background-color: #ff9999;
                    border: 2px solid #cc0000;
                    border-radius: 5px;
                    padding: 15px;
                    margin: 20px auto;
                    max-width: 600px;
                    text-align: center;
                }}
                .email-alert i {{
                    color: #cc0000;
                    font-size: 24px;
                    margin-right: 10px;
                }}
                .email-alert strong {{
                    color: #cc0000;
                    font-size: 18px;
                }}
                .email-alert p {{
                    margin: 5px 0;
                }}
                .email-alert .error-detail {{
                    font-size: 12px;
                    color: #660000;
                    font-style: italic;
                }}
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
                .timeline-container {{
                    max-width: 820px;
                    margin: 30px auto 10px;
                    text-align: center;
                }}
                .timeline-container svg circle {{ cursor: help; }}
                .timeline-legend {{
                    display: flex;
                    justify-content: center;
                    gap: 18px;
                    flex-wrap: wrap;
                    font-size: 12px;
                    margin-top: 10px;
                    color: #555;
                }}
                .timeline-legend .dot {{
                    display: inline-block;
                    width: 10px;
                    height: 10px;
                    border-radius: 50%;
                    vertical-align: middle;
                    margin-right: 5px;
                }}
                .tabs {{
                    display: flex;
                    justify-content: center;
                    margin: 20px 0 0;
                    gap: 4px;
                    border-bottom: 1px solid #dddddd;
                }}
                .tab-btn {{
                    background: #f2f2f2;
                    border: 1px solid #dddddd;
                    border-bottom: none;
                    padding: 8px 18px;
                    cursor: pointer;
                    font-size: 14px;
                    border-radius: 4px 4px 0 0;
                    color: #333;
                }}
                .tab-btn:hover {{ background: #e8e8e8; }}
                .tab-btn.active {{
                    background: #ffffff;
                    font-weight: bold;
                    position: relative;
                    top: 1px;
                }}
                .tab-pane {{ display: none; }}
                .tab-pane.active {{ display: block; }}
                footer {{
                    text-align: center;
                    margin-top: 50px;
                    padding: 10px;
                    border-top: 1px solid #dddddd;
                    font-size: 14px;
                    width: 100%;
                }}
            </style>
            <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
        </head>
        <body>
            <h2 class="center">FlyRoom CO<sub>2</sub> Bank Status</h2>
            {email_alert_html}
            <div class="tabs">
                <button class="tab-btn active" data-tab="status">Status</button>
                <button class="tab-btn" data-tab="pos">Purchase Orders</button>
            </div>
            <div id="tab-status" class="tab-pane active">
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
                {orders_html}
                <div class="center">
                    <img src="/plot" alt="Bank Contents Plot">
                </div>
            </div>
            <div id="tab-pos" class="tab-pane">
                {pos_html}
            </div>
            <footer class="center">
                CO<sub>2</sub> bank for the Dept of Life Sciences Fly Room - Imperial College London - <a href="https://dfs.linde.com/main/dashboard">Linde Dashboard</a>
            </footer>
            <script>
                document.querySelectorAll('.tab-btn').forEach(function (btn) {{
                    btn.addEventListener('click', function () {{
                        document.querySelectorAll('.tab-btn').forEach(function (b) {{ b.classList.remove('active'); }});
                        document.querySelectorAll('.tab-pane').forEach(function (p) {{ p.classList.remove('active'); }});
                        btn.classList.add('active');
                        document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
                    }});
                }});
            </script>
        </body>
        </html>
        """
        return html




    def generate_plot(self, resampling_value='3H', days=10):
        # Load data from CSV log file
        df = pd.read_csv(link.log_file, parse_dates=['messageTime'])
        df['messageTime'] = pd.to_datetime(df['messageTime'], errors='coerce')

        # Filter data for the past 7 days
        now = datetime.now()
        time_window = now - timedelta(days=days)
        df_filtered = df[df['messageTime'] >= time_window]

        # Ensure 'content' column is numeric
        df_filtered = df_filtered.copy()
        df_filtered['content'] = pd.to_numeric(df_filtered['content'], errors='coerce')

        # Separate left and right banks
        df_left = df_filtered[df_filtered['bank'] == 'left'].set_index('messageTime')
        df_right = df_filtered[df_filtered['bank'] == 'right'].set_index('messageTime')

        # Ensure that 'content' column is a valid numeric type for interpolation
        # Then Interpolate data
        df_left_interpolated = df_left.infer_objects(copy=False).interpolate(method='time')
        df_right_interpolated = df_right.infer_objects(copy=False).interpolate(method='time')

        # Read the last alert dates and times
        alert_times = {'left': [], 'right': []}
        last_alert_file = os.path.join(_DATADIR, 'last_alert.log')
        if os.path.exists(last_alert_file):
            with open(last_alert_file, 'r') as file:
                for line in file:
                    parts = line.strip().split(',')
                    if len(parts) < 2:
                        continue
                    last_time_str, last_bank = parts[0], parts[1]
                    last_time = datetime.strptime(last_time_str, '%Y-%m-%d %H:%M')
                    if last_bank in alert_times:
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
        plot_path = os.path.join(_DATADIR, 'plot.png')
        plt.savefig(plot_path)
        plt.close()


def run_server(server_class=HTTPServer, handler_class=RequestHandler, port=_DEFAULT_PORT):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting httpd server on port {port}')
    httpd.serve_forever()

if __name__ == '__main__':

    parser = optparse.OptionParser()
    parser.add_option("-p", "--path", dest="path", default="./data/", help="Set the path to the data folder")
    parser.add_option("--notify", dest="notify", default=False, help="Notify via email", action="store_true")
    parser.add_option("--port", dest="port", default=_DEFAULT_PORT, help="Port for the webserver")
    parser.add_option("--debug", dest="debug", default=False, help="Enable debug logging", action="store_true")

    (options, args) = parser.parse_args()

    # Configure logging based on debug option
    if options.debug:
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s',
            stream=os.sys.stdout
        )
    else:
        logging.basicConfig(
            level=logging.ERROR,
            format='%(message)s',
            stream=os.sys.stdout
        )

    option_dict = vars(options)
    _DATADIR = option_dict["path"]
    _ALERT = option_dict["notify"]
    _PORT = int(option_dict["port"])

    link = LindeLink()
    link.start_data_collection()
    run_server(port=_PORT)
