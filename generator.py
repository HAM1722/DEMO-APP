import random
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

STATUSES = ("OK", "WARN", "FAIL")


def _random_status(ok_ratio: float, warn_ratio: float) -> str:
    roll = random.random()
    if roll < ok_ratio:
        return "OK"
    if roll < ok_ratio + warn_ratio:
        return "WARN"
    return "FAIL"


def generate_run_data(
    min_records: int = 10,
    max_records: int = 30,
    ok_ratio: float = 0.75,
    warn_ratio: float = 0.2,
) -> Tuple[Dict, List[Dict]]:
    total_records = random.randint(min_records, max_records)
    created_at = datetime.utcnow()
    records: List[Dict] = []

    ok_count = 0
    warn_count = 0
    fail_count = 0

    for idx in range(total_records):
        status = _random_status(ok_ratio, warn_ratio)
        value = round(random.uniform(10.0, 100.0), 2)
        sensor_id = f"S-{random.randint(100, 999)}"
        record_time = created_at - timedelta(seconds=(total_records - idx) * 3)

        if status == "OK":
            ok_count += 1
        elif status == "WARN":
            warn_count += 1
        else:
            fail_count += 1

        records.append(
            {
                "sensor_id": sensor_id,
                "value": value,
                "status": status,
                "created_at": record_time.isoformat(),
            }
        )

    if ok_count >= fail_count:
        run_status = "OK"
    else:
        run_status = "WARN"

    run_summary = {
        "created_at": created_at.isoformat(),
        "status": run_status,
        "total_records": total_records,
        "ok_records": ok_count,
        "warn_records": warn_count,
        "fail_records": fail_count,
    }

    return run_summary, records
