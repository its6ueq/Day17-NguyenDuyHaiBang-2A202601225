#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


# Ba quyết định của layout mới:
#
#   PARTITION_BY (event_date)   Dashboard lọc theo NGÀY và theo KHÁCH HÀNG.
#                               event_date có 14 giá trị -> 14 thư mục, engine
#                               loại 13/14 dữ liệu chỉ bằng đường dẫn, không mở
#                               file nào. customer_name có 650 giá trị: partition
#                               theo cột đó lại đẻ ra 650 thư mục file tí hon,
#                               tức tái tạo đúng small-file problem đang sửa.
#
#   ORDER BY event_date, customer_name, event_time
#                               Trong một partition, hàng của cùng một khách nằm
#                               liền nhau, nên min/max customer_name của mỗi row
#                               group hẹp và engine bỏ qua được row group không
#                               chứa 'ACME'. Dữ liệu nguồn xếp ngẫu nhiên nên
#                               min/max của mọi row group đều là A..Z — thống kê
#                               tồn tại nhưng vô dụng.
#
#   ROW_GROUP_SIZE 2048         Một ngày ~9.340 hàng. Để mặc định 122.880 thì cả
#                               ngày nằm gọn trong MỘT row group, min/max phủ
#                               toàn bộ khách hàng và mất hết tác dụng lọc.
#                               2.048 hàng -> ~5 row group/ngày, đủ mịn để cắt
#                               mà chưa nhỏ tới mức quay lại small-file problem.
PARTITION_COL = "event_date"
ORDER_BY = "event_date, customer_name, event_time"
ROW_GROUP_SIZE = 2048


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    src_rows = con.execute(
        f"select count(*) from read_parquet('{SRC}/*.parquet')"
    ).fetchone()[0]

    con.execute(f"""
        copy (
            select *
            from read_parquet('{SRC}/*.parquet')
            order by {ORDER_BY}
        ) to '{DST}' (
            format          parquet,
            partition_by    ({PARTITION_COL}),
            overwrite_or_ignore,
            row_group_size  {ROW_GROUP_SIZE}
        )
    """)

    dst_files = sorted(DST.glob("**/*.parquet"))
    dst_rows = con.execute(
        f"select count(*) from read_parquet('{DST}/**/*.parquet', hive_partitioning = true)"
    ).fetchone()[0]

    assert src_rows == dst_rows, f"mất hàng: {src_rows:,} -> {dst_rows:,}"

    print(f"  đích  : {DST}  ({len(dst_files):,} file)")
    print(f"  số hàng: {src_rows:,} -> {dst_rows:,}  (khớp)")
    print(f"  partition_by({PARTITION_COL})  order by {ORDER_BY}  "
          f"row_group_size {ROW_GROUP_SIZE:,}")
    print("\n  bước tiếp: sửa queries/dashboard.sql trỏ vào dataset mới rồi "
          "chạy `make explain`.\n")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
