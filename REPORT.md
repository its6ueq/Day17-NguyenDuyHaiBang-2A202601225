
# Báo cáo LAB 17 - Data Pipeline Engineering

**Họ tên:** Nguyễn Duy Hải Bằng
**Ngày:** 17/08/2026
**Môi trường:** WSL2, Ubuntu 26.04, Python 3.14.4, DuckDB 1.5.5, dbt-core 1.12.2, dbt-duckdb 1.11.0

---

## 0. Kết quả kiểm tra

Tôi chạy `make verify` ba lần và cả 4 tiêu chí đều đạt:

| Bảng                  | Số hàng | Checksum cả 3 lượt |
| ---------------------- | --------: | --------------------- |
| `gold_training_set`  |    12.480 | `8dd7c98653`        |
| `gold_feature_daily` |     9.100 | `3db448685c`        |
| `gold_doc_chunks`    |    31.200 | `92d8e50131`        |
| `quarantine_tickets` |       312 | `ebb89036fb`        |

`dbt test`: **13/13 pass** · `priority`: không NULL, chỉ từ 1 đến 4 · `gold_training_set`: không lặp ticket · DAG: `catchup=False`, `max_active_runs=1`.

---

## 1. Bảng training tăng sau mỗi lần chạy

**Triệu chứng:** `gold_training_set` tăng từ 12.480 lên 25.615 rồi 38.750 hàng sau ba lượt chạy, dù pipeline không báo lỗi.

**Root cause:** Nguồn CDC có bản ghi `op='u'`, nhưng model incremental không khai báo `unique_key`, nên dbt dùng `INSERT`; mỗi lần chạy lại sẽ ghi thêm bản ghi thay vì cập nhật ticket cũ.

**Cách fix:** Trong `gold_training_set.sql`, thêm `unique_key='ticket_id'` và `incremental_strategy='merge'`. Trong DAG, đặt `catchup=False` và `max_active_runs=1` để tránh nhiều lượt chạy ghi đồng thời.

**Bằng chứng:** Bảng còn đúng **12.480 hàng**, mỗi ticket xuất hiện một lần và checksum ba lượt đều là `8dd7c98653`.

---

## 2. Bảng feature thiếu dữ liệu ở ngày cũ

**Triệu chứng:** `gold_feature_daily` chỉ có 8.645/9.100 hàng. Bảng vẫn ổn định qua nhiều lượt chạy nhưng bị thiếu dữ liệu đến muộn.

**P99 độ trễ:** **65,4 giờ, tương đương 2,73 ngày**. Giá trị lớn nhất là 2,95 ngày, vì vậy tôi chọn lookback **3 ngày**.

**Root cause:** Điều kiện `event_date > max(event_date)` chỉ đọc ngày mới hơn mốc hiện có. Event xảy ra ở ngày cũ nhưng đến kho muộn sẽ nằm dưới mốc này và bị bỏ qua vĩnh viễn.

**Cách fix:** Trong `gold_feature_daily.sql`, lùi cửa sổ ba ngày và dùng khóa `['event_date', 'customer_id']` với chiến lược `merge` để dữ liệu được tính lại nhưng không bị cộng dồn.

```sql
where event_date >= (select max(event_date) from {{ this }}) - interval 3 day
```

Tôi dùng P99 thay vì `max` vì một bản ghi trễ bất thường có thể làm cửa sổ tăng quá lớn và khiến pipeline phải đọc lại nhiều partition ở mọi lượt chạy. Phần dữ liệu nằm ngoài P99 có thể xử lý bằng job backfill riêng.

**Bằng chứng:** Số hàng tăng từ **8.645 lên 9.100**, checksum ba lượt đều là `3db448685c`.

---

## 3. Cột `priority` đổi kiểu biểu diễn

**Triệu chứng:** Từ ngày 10/08, `silver_tickets.priority` có 6.606 giá trị NULL nhưng 9 test ban đầu vẫn pass.

**Root cause:** Nguồn đổi `priority` từ số sang nhãn chữ, còn Silver dùng `try_cast` nên các nhãn hợp lệ bị biến thành NULL; ngược lại, các giá trị số ngoài miền như `0`, `5`, `-1` vẫn lọt qua. Contract và test ban đầu chưa kiểm tra lỗi này.

**Ba nhóm giá trị:**

- `'1'..'4'`: giữ nguyên.
- `urgent/high/medium/low`: quy đổi thành `1/2/3/4`.
- `P1`, `P2`, `unknown`, `0`, `5`, `-1`, chuỗi rỗng và NULL: đưa vào quarantine.

**Cách fix:** Sửa macro `normalize_priority.sql`; lọc bản ghi lỗi trước khi chạy `row_number()` trong `silver_tickets.sql`; dùng cùng macro cho `quarantine_tickets.sql`; bật `contract: enforced: true` và thêm test `not_null`, `accepted_values [1,2,3,4]`.

Tôi xử lý ở Silver để Bronze vẫn giữ nguyên dữ liệu gốc phục vụ điều tra. Pipeline không cần dừng vì 312 bản ghi lỗi không nên chặn toàn bộ dữ liệu hợp lệ; các bản ghi này được giữ trong quarantine cùng lý do loại.

**Bằng chứng:** `quarantine_tickets` có đúng **312 hàng**, `dbt test` đạt **13/13**, `priority` chỉ từ 1 đến 4 và Silver vẫn đủ **12.480 ticket**.

---

## 4. Bài mở rộng

### Extra A - Tối ưu dashboard

**Root cause:** 130.683 hàng bị chia thành 5.000 file Parquet nhỏ, không partition; truy vấn còn bọc `event_time` trong `strftime`, nên engine phải mở và quét quá nhiều file.

**Cách fix:** Gom dữ liệu thành 14 partition theo `event_date`, sắp xếp theo `customer_name`, đặt `row_group_size=2048` và lọc trực tiếp bằng `event_date`.

**Bằng chứng:** Số file giảm **5.000 → 14**, rows scanned giảm **5.000.000 → 9.324 (536,3 lần)** và hash kết quả không đổi.

### Extra B - Consumer bị dừng giữa batch

**Root cause:** Consumer commit offset trước khi ghi dữ liệu nên nếu bị dừng giữa hai bước, batch chưa ghi sẽ bị bỏ qua. Đây là cơ chế `at-most-once` và có nguy cơ mất dữ liệu.

**Cách fix:** Ghi dữ liệu trước rồi mới commit offset để chuyển sang `at-least-once`; dùng `event_id` làm khóa chính và `ON CONFLICT DO UPDATE` để việc phát lại không tạo bản ghi trùng.

**Bằng chứng:** Crash-test kết thúc với **20.000 hàng/20.000 `event_id`**, không mất và không trùng dữ liệu.

---

## 5. Tổng kết

Tự đối chiếu theo rubric và kết quả các công cụ kiểm tra, bài đã đáp ứng đủ **100 điểm chính** và hai bài Extra tương ứng **10 điểm thưởng**.
