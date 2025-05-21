import logging
import smtplib
from email.mime.text import MIMEText
from dotenv import load_dotenv
import os

# Initialize logger
logger = logging.getLogger('DebugEmail')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def load_and_print_env_variables():
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(current_dir, '.env')

    # Check if the .env file exists
    if not os.path.exists(env_path):
        logger.error(f".env file not found at path: {env_path}")
        return None

    # Load environment variables from .env file
    load_dotenv(env_path)

    # Print environment variables for debugging
    smtp_server = os.getenv('SMTP_SERVER')
    smtp_port = os.getenv('SMTP_PORT')
    email_user = os.getenv('EMAIL_USER')
    email_pass = os.getenv('EMAIL_PASS')
    sender_email = os.getenv('SENDER_EMAIL')
    recipient_email = os.getenv('RECIPIENT_EMAIL')

    logger.info(f"SMTP_SERVER: {smtp_server}")
    logger.info(f"SMTP_PORT: {smtp_port}")
    logger.info(f"EMAIL_USER: {email_user}")
    logger.info(f"EMAIL_PASS: {email_pass}")
    logger.info(f"SENDER_EMAIL: {sender_email}")
    logger.info(f"RECIPIENT_EMAIL: {recipient_email}")

    return smtp_server, smtp_port, email_user, email_pass, sender_email, recipient_email

def initialize_notification_system(smtp_server, smtp_port, email_user, email_pass):
    try:
        # Set up the SMTP server
        server = smtplib.SMTP(smtp_server, int(smtp_port))
        server.starttls()
        server.login(email_user, email_pass)
        return server
    except Exception as e:
        logger.error(f"Failed to initialize email server: {e}")
        return None

def notify(email_server, sender_email, recipient_email, message):
    if email_server:
        try:
            # Create the email content
            msg = MIMEText(message)
            msg['Subject'] = 'Test Email from Debug Script'
            msg['From'] = sender_email
            msg['To'] = recipient_email

            # Send the email
            email_server.sendmail(sender_email, recipient_email, msg.as_string())
            logger.info(f"Notification sent: {message}")
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
    else:
        logger.error("Email server is not initialized. Cannot send notification.")

if __name__ == "__main__":
    env_variables = load_and_print_env_variables()
    
    if env_variables is None:
        logger.error("Failed to load environment variables. Exiting.")
    else:
        smtp_server, smtp_port, email_user, email_pass, sender_email, recipient_email = env_variables
        
        if not all([smtp_server, smtp_port, email_user, email_pass, sender_email, recipient_email]):
            logger.error("One or more environment variables are missing.")
        else:
            email_server = initialize_notification_system(smtp_server, smtp_port, email_user, email_pass)
            if email_server:
                test_message = "This is a test email from the Debug Script."
                notify(email_server, sender_email, recipient_email, test_message)
                email_server.quit()
