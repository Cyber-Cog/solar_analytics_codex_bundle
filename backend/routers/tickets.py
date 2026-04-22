"""
backend/routers/tickets.py
===========================
Support ticket API — creates a ticket and logs it to the DB.
Email sending is stubbed (logs to console) — connect to SMTP to enable real email.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from database import get_db
from models import User, SupportTicket
from schemas import TicketCreate, MessageResponse
from auth.routes import get_current_user
import logging

logger = logging.getLogger("tickets")
router = APIRouter(prefix="/api/tickets", tags=["Support Tickets"])


def _normalize_recipients(recipient_emails: List[str] | None) -> List[str]:
    recipients: List[str] = []
    for email in (recipient_emails or []):
        e = str(email or "").strip()
        if e and e not in recipients:
            recipients.append(e)
    fallback = os.getenv("SUPPORT_TICKET_DEFAULT_TO", "").strip()
    if fallback:
        for email in fallback.split(","):
            e = email.strip()
            if e and e not in recipients:
                recipients.append(e)
    return recipients


def _send_ticket_email(ticket: SupportTicket, recipients: List[str]) -> None:
    if not recipients:
        return

    host = os.getenv("SUPPORT_SMTP_HOST", "").strip()
    port = int(os.getenv("SUPPORT_SMTP_PORT", "587"))
    username = os.getenv("SUPPORT_SMTP_USER", "").strip()
    password = os.getenv("SUPPORT_SMTP_PASS", "").strip()
    sender = os.getenv("SUPPORT_SMTP_FROM", username or "noreply@solar.local")

    if not host:
        logger.info("[TICKET #%s] Email skipped (SMTP host not configured)", ticket.id)
        return

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = f"[Solar Ticket #{ticket.id}] {ticket.subject}"
    body = (
        f"Ticket ID: {ticket.id}\n"
        f"Raised By: {ticket.user_email}\n"
        f"Plant: {ticket.plant_id or '-'}\n"
        f"Status: {ticket.status}\n\n"
        f"Description:\n{ticket.description}\n"
    )
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(host, port, timeout=15) as server:
        server.ehlo()
        if os.getenv("SUPPORT_SMTP_TLS", "1").strip() not in ("0", "false", "False"):
            server.starttls()
            server.ehlo()
        if username and password:
            server.login(username, password)
        server.sendmail(sender, recipients, msg.as_string())


@router.post("", response_model=MessageResponse, status_code=201)
def raise_ticket(
    payload: TicketCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    ticket = SupportTicket(
        user_email  = payload.user_email or current_user.email,
        plant_id    = payload.plant_id,
        subject     = payload.subject,
        description = payload.description,
        status      = "open",
    )
    db.add(ticket)
    db.commit()
    db.refresh(ticket)

    recipients = _normalize_recipients(payload.recipient_emails)
    emailed_count = 0
    try:
        _send_ticket_email(ticket, recipients)
        emailed_count = len(recipients)
    except Exception as exc:
        logger.exception("[TICKET #%s] Email send failed: %s", ticket.id, exc)

    logger.info(
        "[TICKET #%s] From: %s | Subject: %s | Plant: %s | Recipients: %s",
        ticket.id, ticket.user_email, ticket.subject, ticket.plant_id, recipients
    )

    msg = f"Ticket #{ticket.id} raised successfully."
    if emailed_count:
        msg += f" Notification sent to {emailed_count} recipient(s)."
    else:
        msg += f" Support will contact you at {ticket.user_email}."

    return MessageResponse(
        message=msg,
        success=True,
    )
