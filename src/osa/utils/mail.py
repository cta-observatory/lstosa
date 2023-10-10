"""Functionality for sending emails."""

import smtplib
from email.message import EmailMessage
from osa.configs.config import cfg


__all__ = ["send_warning_mail"]


def send_warning_mail(date: str):
    """Send a warning email to the admin."""
    msg = EmailMessage()
    msg["Subject"] = f"WARNING: {date} is not closed yet"
    msg["From"] = "lstanalyzer"
    msg["To"] = cfg.get("mail", "recipient")
    msg.set_content(f"Could not close LST1 for {date}. Please check.")

    with smtplib.SMTP("localhost") as email:
        email.send_message(msg)
