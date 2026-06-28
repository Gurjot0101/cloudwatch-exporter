import boto3
from calendar import monthrange
from datetime import datetime, timedelta, timezone

logs_client = boto3.client("logs")
s3_client = boto3.client("s3")

TERMINAL = {"COMPLETED", "FAILED", "CANCELLED"}

# ----------------------------
# Helpers (time + metadata)
# ----------------------------
def month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def month_end(dt: datetime) -> datetime:
    last = monthrange(dt.year, dt.month)[1]
    return dt.replace(day=last, hour=23, minute=59, second=59, microsecond=0)

def next_month(dt: datetime) -> datetime:
    return (dt.replace(day=1) + timedelta(days=32)).replace(day=1)

def get_log_group_meta(log_group_name: str):
    """
    Exact-match lookup for creationTime and retentionInDays.
    Avoids prefix+limit issues.
    """
    resp = logs_client.describe_log_groups(logGroupNamePrefix=log_group_name)
    for g in resp.get("logGroups", []):
        if g["logGroupName"] == log_group_name:
            created_at = datetime.fromtimestamp(g["creationTime"] / 1000, tz=timezone.utc)
            retention_days = g.get("retentionInDays")
            return created_at, retention_days
    raise ValueError(f"Log Group not found: {log_group_name}")

def has_any_events_in_window(log_group_name: str, start_ms: int, end_ms: int) -> bool:
    """
    Lightweight check: do we have at least 1 event in [start_ms, end_ms]?
    If none, we skip that month.
    """
    resp = logs_client.filter_log_events(
        logGroupName=log_group_name,
        startTime=start_ms,
        endTime=end_ms,
        limit=1
    )
    return len(resp.get("events", [])) > 0

def ensure_s3_prefix(bucket: str, prefix: str):
    """
    "Create folder" in S3 by placing a zero-byte object with key ending in '/'.
    Safe to call repeatedly.
    """
    if not prefix.endswith("/"):
        prefix += "/"
    s3_client.put_object(Bucket=bucket, Key=prefix, Body=b"")

def build_dest_prefix(s3_prefix: str, log_group_name: str, yyyy: int, mm: int) -> str:
    """
    Produces: <s3Prefix>/<log-group-name>/YYYY/MM/
    - Converts log group name into a safe path component.
    """
    safe_lg = log_group_name.strip("/").replace("/", "-")
    s3_prefix = (s3_prefix or "").strip("/")

    parts = [p for p in [s3_prefix, safe_lg, f"{yyyy}", f"{mm:02d}"] if p]
    return "/".join(parts) + "/"

# ----------------------------
# Actions invoked by Step Functions
# ----------------------------
def generate_months(event):
    """
    Step Functions input (as per your shared state machine):
    {
      "task_type": "generate_months",
      "s3Bucket": "...",
      "s3Prefix": "cloudwatch-logs",
      "logGroups": [{"logGroupName": "/aws/lambda/a"}, {"logGroupName": "/aws/lambda/b"}],
      "cutoffDays": 365   (optional, default 365)
    }

    Output:
    { "exportTasks": [ { "task_type":"create", ... }, ... ] }
    """
    log_groups = event["logGroups"]
    s3_bucket = event["s3Bucket"]
    s3_prefix = event.get("s3Prefix", "cloudwatch-logs")
    cutoff_days = int(event.get("cutoffDays", 329))

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=cutoff_days)  # only archive logs older than cutoff_days

    tasks = []

    for lg in log_groups:
        log_group = lg["logGroupName"]

        created_at, retention_days = get_log_group_meta(log_group)

        # If retention is not set, logs are kept indefinitely ("Never expire")
        if retention_days is None:
            retention_cutoff = datetime.fromtimestamp(0, tz=timezone.utc)
        else:
            retention_cutoff = now - timedelta(days=int(retention_days))

        # iterate month-by-month starting from log group creation month
        current = month_start(created_at)

        print(
            f"[GENERATE] {log_group} created={created_at} "
            f"retention_days={retention_days} retention_cutoff={retention_cutoff} cutoff={cutoff}"
        )

        while current <= cutoff:
            m_start = month_start(current)
            m_end = month_end(current)

            # Export window must be strictly <= cutoff (older than 365 days)
            to_time = min(m_end, cutoff)

            # Clamp from_time to:
            # - month start
            # - log group creation time
            # - retention cutoff (if retention is configured)
            from_time = max(m_start, created_at, retention_cutoff)

            if from_time >= to_time:
                current = next_month(current)
                continue

            from_ms = int(from_time.timestamp() * 1000)
            to_ms = int(to_time.timestamp() * 1000)

            # Key handling: if month has no events, skip this month only
            if not has_any_events_in_window(log_group, from_ms, to_ms):
                current = next_month(current)
                continue

            dest_prefix = build_dest_prefix(s3_prefix, log_group, current.year, current.month)

            tasks.append({
                "task_type": "create",
                "logGroupName": log_group,
                "s3Bucket": s3_bucket,
                "destinationPrefix": dest_prefix,
                "fromTime": from_ms,
                "toTime": to_ms,
                "year": current.year,
                "month": current.month
            })

            current = next_month(current)

    print(f"[GENERATE] Total export tasks generated: {len(tasks)}")
    return {"exportTasks": tasks}

def create_export_tasks(event):
    """
    Step Functions Map item input:
    {
      "task_type":"create",
      "logGroupName":"...",
      "s3Bucket":"...",
      "destinationPrefix":".../YYYY/MM/",
      "fromTime": <ms>,
      "toTime": <ms>,
      "year": 2025,
      "month": 2
    }

    Output includes taskId and switches to poll.
    """
    # Ensure YYYY/MM "folder" exists in S3 for audit/readability
    ensure_s3_prefix(event["s3Bucket"], event["destinationPrefix"])

    response = logs_client.create_export_task(
        logGroupName=event["logGroupName"],
        fromTime=event["fromTime"],
        to=event["toTime"],
        destination=event["s3Bucket"],
        destinationPrefix=event["destinationPrefix"]
    )

    task_id = response["taskId"]
    print(
        f"[CREATE] {event['logGroupName']} {event['year']}/{event['month']:02d} "
        f"TaskId={task_id} Dest={event['destinationPrefix']}"
    )

    return {
        **event,
        "task_type": "poll",
        "taskId": task_id
    }

def poll_status(event):
    """
    Step Functions poll input:
    {
      "task_type":"poll",
      "taskId":"?",
      "logGroupName":"?",
      "year": ?,
      "month": ?
    }
    """
    tasks = logs_client.describe_export_tasks(taskId=event["taskId"]).get("exportTasks", [])
    if not tasks:
        raise ValueError(f"No export task found: {event['taskId']}")

    task = tasks[0]
    status = task["status"]["code"]
    msg = task["status"].get("message", "")

    print(f"[POLL] {event['logGroupName']} {event['year']}/{event['month']:02d} Status={status}")

    return {
        **event,
        "status": status,
        "statusMessage": msg,
        "isComplete": status in TERMINAL,
        "isFailed": status in ("FAILED", "CANCELLED")
    }

# ----------------------------
# Lambda Entry Point
# ----------------------------
def lambda_handler(event, context):
    """
    Routes based on task_type:
    - generate_months
    - create
    - poll
    """
    action = event.get("task_type")
    if action == "generate_months":
        return generate_months(event)
    elif action == "create":
        return create_export_tasks(event)
    elif action == "poll":
        return poll_status(event)

    return {"error": "Invalid task_type. Use: generate_months | create | poll"}
