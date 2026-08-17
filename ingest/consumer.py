#!/usr/bin/env python3
"""Consumer đọc topic `ai-events` và ghi xuống bảng stream — NHIỆM VỤ 5.

Chạy tay:
    python ingest/consumer.py --db data/crash/crash.duckdb \
        --topic data/crash/topic.jsonl --offset data/crash/offsets.json

Kịch bản sự cố (tools/crash_test.py tự lo):
    thêm --crash-at-batch 7  -> tiến trình tự chết ở lô thứ 7, y hệt kill -9.

KHUNG THỰC HIỆN — NHIỆM VỤ 5

  Chạy `make crash-test` trước. Đọc kết quả: bạn MẤT bản ghi hay bạn có bản
  ghi TRÙNG? Con số đó xác định consumer đang ở ngữ nghĩa nào.

      at-most-once   : commit offset TRƯỚC khi ghi  -> crash = mất dữ liệu
      at-least-once  : commit offset SAU khi ghi    -> crash = trùng dữ liệu
      exactly-once   : không tồn tại ở tầng giao vận

  Hai hạng mục cần xử lý, thiếu một là chưa đủ:

    (a) Thứ tự thao tác trong consume() — xem khối được đánh dấu bên dưới.
        Đổi thứ tự chuyển ngữ nghĩa từ nhóm này sang nhóm kia. Câu hỏi: nếu
        tiến trình chết ở điểm maybe_crash(), lô hiện tại đã được ghi chưa,
        offset đã dịch chưa, và lần khởi động lại sẽ đọc từ đâu?

    (b) Tính idempotent của write_batch() — đổi thứ tự ở (a) khiến một số lô
        được phát lại. Với câu lệnh INSERT hiện tại, phát lại nghĩa là gì?

            INSERT INTO <bảng> VALUES (...)
            ON CONFLICT (<cột khoá>) DO <UPDATE ... | NOTHING>

        DuckDB chỉ chấp nhận mệnh đề ON CONFLICT khi cột khoá có ràng buộc
        PRIMARY KEY hoặc UNIQUE — xem hằng DDL ngay bên dưới.

        Câu hỏi cho báo cáo: DO UPDATE và DO NOTHING khác nhau ở đâu khi một
        message được phát lại với nội dung ĐÃ ĐỔI? Bạn chọn cái nào, vì sao?
"""

from __future__ import annotations

import argparse
import os
import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from ingest.log_client import LogConsumer  # noqa: E402

TABLE = "bronze_events_stream"

DDL = f"""
create table if not exists {TABLE} (
    -- primary key là điều kiện để DuckDB chấp nhận ON CONFLICT, và là thứ biến
    -- phép ghi thành idempotent: phát lại cùng event_id không sinh hàng mới.
    event_id      varchar primary key,
    ticket_id     varchar,
    customer_id   varchar,
    customer_name varchar,
    event_type    varchar,
    latency_ms    integer,
    event_time    timestamp,
    _ingested_at  timestamp
);
"""


def _sql_literal(v: object) -> str:
    """Giá trị -> literal SQL.

    Lô được ghép thành một câu INSERT nhiều hàng thay vì `executemany` với
    tham số bind. Lý do là hiệu năng đo được của môi trường này: DuckDB 1.5.5
    trên Python 3.14 tốn ~100 ms cho mỗi lần bind một hàng 8 tham số, tức ~50
    giây cho một lô 500 hàng; cùng lô đó ghép thành một câu lệnh chạy trong
    ~25 ms. Dữ liệu vào đây là message của topic nội bộ, và mọi chuỗi đều được
    escape ở dưới.
    """
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def write_batch(con: duckdb.DuckDBPyConnection, batch: list[dict]) -> None:
    """Ghi một lô message xuống kho — phép ghi idempotent (upsert theo event_id).

    At-least-once ở tầng giao vận nghĩa là một số message CHẮC CHẮN được phát
    lại. INSERT thuần biến mỗi lần phát lại thành một hàng mới; upsert theo khoá
    tự nhiên `event_id` thì phát lại bao nhiêu lần cũng cho đúng một hàng.

    DO UPDATE thay vì DO NOTHING: nếu message được phát lại với nội dung đã đổi
    (ví dụ latency_ms được đính chính), DO NOTHING giữ bản cũ và bảng đứng yên ở
    trạng thái sai; DO UPDATE hội tụ về bản phát sau cùng. Với message bất biến
    thì hai lựa chọn cho kết quả như nhau, nên DO UPDATE là lựa chọn an toàn hơn.
    """
    if not batch:
        return

    # Một lô = MỘT câu lệnh. Đơn vị ghi vì thế trùng đúng đơn vị offset: kho
    # không bao giờ dừng ở trạng thái "nửa lô" khi tiến trình bị giết.
    #
    # Lô có thể chứa cùng một event_id hai lần (nguồn phát lại ngay trong một
    # lô). ON CONFLICT không cho phép cập nhật cùng một hàng hai lần trong một
    # câu lệnh, nên khử trùng ngay ở đây, giữ bản đến sau — cùng quy ước với
    # DO UPDATE bên dưới.
    dedup = {r["event_id"]: r for r in batch}
    values = ", ".join(
        "(" + ", ".join(
            _sql_literal(v) for v in (
                r["event_id"], r["ticket_id"], r["customer_id"], r["customer_name"],
                r["event_type"], r["latency_ms"], r["event_time"], r["_ingested_at"],
            )
        ) + ")"
        for r in dedup.values()
    )
    con.execute(f"""
        insert into {TABLE} values {values}
        on conflict (event_id) do update set
            ticket_id     = excluded.ticket_id,
            customer_id   = excluded.customer_id,
            customer_name = excluded.customer_name,
            event_type    = excluded.event_type,
            latency_ms    = excluded.latency_ms,
            event_time    = excluded.event_time,
            _ingested_at  = excluded._ingested_at
    """)


def maybe_crash(batch_no: int, crash_at: int | None) -> None:
    """Mô phỏng `kill -9`: chết ngay, không rollback, không flush."""
    if crash_at is not None and batch_no == crash_at:
        print(f"  [consumer] 💥 tiến trình bị giết ở lô {batch_no}", flush=True)
        os._exit(137)


def consume(
    db: str,
    topic: str,
    offset_file: str,
    batch_size: int = 500,
    crash_at: int | None = None,
) -> int:
    con = duckdb.connect(db)
    con.execute(DDL)

    written = 0
    with LogConsumer(topic, offset_file) as consumer:
        batch_no = 0
        while True:
            batch = consumer.poll(batch_size)
            if not batch:
                break
            batch_no += 1

            # ── at-least-once: GHI TRƯỚC, COMMIT OFFSET SAU ───────────────
            # Chết ở giữa hai bước: dữ liệu đã nằm trong kho, offset chưa dịch,
            # nên lần khởi động lại đọc lại đúng lô đó. Không mất bản ghi, đổi
            # lại là có bản ghi trùng — và write_batch() upsert theo event_id
            # nên bản trùng bị nuốt gọn.
            write_batch(con, batch)           # ghi dữ liệu (idempotent)
            maybe_crash(batch_no, crash_at)   # sự cố xảy ra tại đây
            consumer.commit()                 # ghi nhận offset
            # ─────────────────────────────────────────────────────────────

            written += len(batch)

    con.close()
    return written


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--offset", required=True)
    ap.add_argument("--batch-size", type=int, default=500)
    ap.add_argument("--crash-at-batch", type=int, default=None)
    a = ap.parse_args()
    n = consume(a.db, a.topic, a.offset, a.batch_size, a.crash_at_batch)
    print(f"  [consumer] đã ghi {n:,} message")
    return 0


if __name__ == "__main__":
    sys.exit(main())
