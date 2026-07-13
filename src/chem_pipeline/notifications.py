import requests


def send_teams_message(webhook_url, title, text, color="0076D7"):
    if not webhook_url:
        return
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": title,
        "sections": [{"activityTitle": title, "text": text, "markdown": True}],
    }
    r = requests.post(webhook_url, json=payload, timeout=30)
    r.raise_for_status()


def notify_success(webhook_url, dataset_id, mol_count):
    send_teams_message(webhook_url,
                       f"{dataset_id} processed",
                       f"Generated **{mol_count}** molecules.", "2EB886")


def notify_failure(webhook_url, dataset_id, reason):
    send_teams_message(webhook_url,
                       f"{dataset_id} failed data quality",
                       reason, "D93F3F")
