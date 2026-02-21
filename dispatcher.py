"""
fundscope/backend/email/dispatcher.py

Email composition and dispatch — builds the weekly HTML digest and sends via
SendGrid (default), AWS SES, or SMTP fallback.
"""

import os
import logging
import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader
import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "sectors.yaml"
TEMPLATE_DIR = Path(__file__).parent


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ── Template rendering ─────────────────────────────────────────────────────────

def build_alert_list(raw_alerts: list[dict], fund_names: dict[str, str], sector_names: dict[str, str]) -> list[dict]:
    """
    Group raw alerts by fund so each fund appears once with all its period drops.
    """
    from collections import defaultdict
    grouped = defaultdict(lambda: {"drops": [], "streak_broken": 0})

    for a in raw_alerts:
        fid = a["fund_id"]
        grouped[fid]["fund_id"]     = fid
        grouped[fid]["fund_name"]   = fund_names.get(fid, fid)
        grouped[fid]["sector_name"] = sector_names.get(a["sector_code"], a["sector_code"])
        grouped[fid]["sector_code"] = a["sector_code"]
        # Use longest streak broken across periods
        if a.get("streak_broken", 0) > grouped[fid]["streak_broken"]:
            grouped[fid]["streak_broken"] = a.get("streak_broken", 0)
        grouped[fid]["drops"].append({
            "period":       a["period"],
            "prev_decile":  a.get("prev_decile", 1),
            "curr_decile":  a.get("curr_decile", 2),
            "return_value": a.get("return_value", 0.0),
        })

    return sorted(grouped.values(), key=lambda x: -x["streak_broken"])


def render_digest(
    week_date: date,
    alerts: list[dict],
    top3_by_sector: dict[str, list[dict]],
    fund_names: dict[str, str],
    sector_names: dict[str, str],
    failed_sectors: list[str],
    total_funds: int,
) -> str:
    """Render the HTML email digest from the Jinja2 template."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("digest_template.html")

    structured_alerts = build_alert_list(alerts, fund_names, sector_names)
    drop_count = len(structured_alerts)
    top3_count = sum(len(v) for v in top3_by_sector.values())
    sector_count = len([v for v in top3_by_sector.values() if v])

    context = {
        "week_date":         week_date.isoformat(),
        "week_date_display": week_date.strftime("%-d %B %Y"),
        "drop_count":        drop_count,
        "total_funds":       total_funds,
        "sector_count":      sector_count,
        "top3_count":        top3_count,
        "alerts":            structured_alerts,
        "top3_by_sector":    top3_by_sector,
        "failed_sectors":    failed_sectors,
    }
    return template.render(**context)


def build_subject(drop_count: int, week_date: date, cfg: dict) -> str:
    template = cfg["alerts"].get(
        "subject_template",
        "FundScope Weekly — {drop_count} Decile Drop{plural} · w/e {week_date}"
    )
    return template.format(
        drop_count=drop_count,
        plural="s" if drop_count != 1 else "",
        week_date=week_date.strftime("%-d %b %Y"),
    )


# ── Dispatch methods ───────────────────────────────────────────────────────────

def send_via_sendgrid(subject: str, html_body: str, recipients: list[str], cfg: dict) -> bool:
    """Send email via SendGrid API."""
    try:
        import sendgrid
        from sendgrid.helpers.mail import Mail, To
    except ImportError:
        raise ImportError("sendgrid not installed. Run: pip install sendgrid")

    api_key = os.environ.get("SENDGRID_API_KEY")
    if not api_key:
        raise ValueError("SENDGRID_API_KEY environment variable not set")

    ec = cfg.get("email", {})
    from_email = ec.get("from_address", "alerts@fundscope.app")
    from_name  = ec.get("from_name", "FundScope Weekly")

    sg = sendgrid.SendGridAPIClient(api_key=api_key)
    message = Mail(
        from_email=(from_email, from_name),
        to_emails=[To(r) for r in recipients],
        subject=subject,
        html_content=html_body,
    )
    response = sg.send(message)
    if response.status_code in (200, 202):
        logger.info(f"SendGrid: sent to {len(recipients)} recipients (status {response.status_code})")
        return True
    else:
        logger.error(f"SendGrid error: {response.status_code} {response.body}")
        return False


def send_via_ses(subject: str, html_body: str, recipients: list[str], cfg: dict) -> bool:
    """Send email via AWS SES."""
    try:
        import boto3
    except ImportError:
        raise ImportError("boto3 not installed. Run: pip install boto3")

    ec = cfg.get("email", {})
    from_addr = f"{ec.get('from_name','FundScope')} <{ec.get('from_address','alerts@fundscope.app')}>"

    client = boto3.client("ses", region_name=os.environ.get("AWS_REGION", "eu-west-1"))
    response = client.send_email(
        Source=from_addr,
        Destination={"ToAddresses": recipients},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Html": {"Data": html_body, "Charset": "UTF-8"}},
        },
    )
    msg_id = response.get("MessageId")
    logger.info(f"SES: sent to {len(recipients)} recipients (MessageId: {msg_id})")
    return True


def send_via_smtp(subject: str, html_body: str, recipients: list[str], cfg: dict) -> bool:
    """Send email via SMTP (Gmail or other). Set SMTP_PASSWORD env var."""
    ec = cfg.get("email", {})
    from_addr = ec.get("from_address", "alerts@fundscope.app")
    smtp_host = ec.get("smtp_host", "smtp.gmail.com")
    smtp_port = int(ec.get("smtp_port", 587))
    password  = os.environ.get("SMTP_PASSWORD", "")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{ec.get('from_name','FundScope')} <{from_addr}>"
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls()
        server.login(from_addr, password)
        server.sendmail(from_addr, recipients, msg.as_string())

    logger.info(f"SMTP: sent to {len(recipients)} recipients")
    return True


def dispatch_digest(
    week_date: date,
    alerts: list[dict],
    top3_by_sector: dict[str, list[dict]],
    fund_names: dict[str, str],
    sector_names: dict[str, str],
    failed_sectors: list[str],
    total_funds: int,
) -> bool:
    """
    Main entry point — renders and sends the weekly digest email.
    Returns True on success, False on failure.
    """
    cfg = load_config()
    alert_cfg = cfg.get("alerts", {})
    recipients = alert_cfg.get("recipients", [])

    if not recipients:
        logger.error("No email recipients configured in sectors.yaml")
        return False

    # Check if we should send even with no drops
    drop_count = len(set(a["fund_id"] for a in alerts))
    if not alert_cfg.get("send_even_if_no_drops", True) and drop_count == 0:
        logger.info("No drops and send_even_if_no_drops=false — skipping email")
        return True

    html_body = render_digest(
        week_date, alerts, top3_by_sector, fund_names,
        sector_names, failed_sectors, total_funds
    )
    subject = build_subject(drop_count, week_date, cfg)

    provider = cfg.get("email", {}).get("provider", "sendgrid")

    try:
        if provider == "sendgrid":
            return send_via_sendgrid(subject, html_body, recipients, cfg)
        elif provider == "ses":
            return send_via_ses(subject, html_body, recipients, cfg)
        else:
            return send_via_smtp(subject, html_body, recipients, cfg)
    except Exception as e:
        logger.error(f"Email dispatch failed: {e}")
        return False
