# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Hà Huy Nhất / 2A202602401
**Repo:** K4-Track02-Day17-HaHuyNhat-2A202602401-DataPipelineEngineering
**Commit bài nộp:** bd105b78a8f996615b18e6bd7d4ff8cecce404dd
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Có dùng Codex để đọc đề, tìm lỗi, sửa pipeline, chạy kiểm chứng và soạn report.
**Nguồn tham khảo khác (nếu có):** README.md, docs/RUBRIC.md, docs/CHECKPOINTS.md và các model dbt trong repo.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

|                                         | Lỗi Silver                                                                                                                                             | Lỗi late data                                                                                                                                     | Lỗi xoá (CDC)                                                                                                                                                      |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Triệu chứng**                 | `verify` báo `silver_tickets` có 24 rows cho 12 tickets; T-91 trả về 3 trạng thái.                                                            | `gold_feature_daily` lệch full recompute; u05 ngày 2026-08-12 chỉ có `(2, 0)` thay vì `(5, 1)`; P99 lateness 3 ngày nhưng lookback 0. | T-97 không thành tombstone, vẫn còn dữ liệu cá nhân; latest training snapshot còn T-97 và RAG index còn 2 chunks.                                         |
| **Nguyên nhân gốc**            | `upsert_silver_tickets` chỉ `INSERT`, không merge theo khóa; replay batch cũ/new làm sinh nhiều hàng và Gold join bị nhân bản.           | `LOOKBACK_DAYS = 0`, daily run chỉ ghi lại partition của ingest day nên event đến muộn không được tính về event day.                | `ticket_changes_sql` lấy `ticket_id` từ `after`; với Debezium delete thì `after = null`, bản ghi delete bị loại bởi `WHERE ticket_id IS NOT NULL`. |
| **Cách sửa** (file, vài dòng) | Trong`pipeline/silver.py`, đổi insert thành `MERGE INTO silver_tickets ON ticket_id`, chỉ update khi `s._lsn > t._lsn`, insert khi chưa có. | Trong`pipeline/config.py`, đặt `LOOKBACK_DAYS = 3` theo `ceil(P99)` đo từ Bronze.                                                        | Trong`pipeline/staging.py`, dùng `coalesce(after.ticket_id, before.ticket_id, key.ticket_id)` để giữ delete change; các field PII từ `after` tự null.   |
| **Khái niệm trên slide**       | Silver có khóa, upsert idempotent, LSN guard để trạng thái mới nhất thắng.                                                                     | Late data theo event time, overwrite-partition với lookback đo từ Bronze.                                                                       | CDC log-based: delete khác Kafka tombstone; xoá phải lan xuống Silver/Gold.                                                                                      |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: ticket là entity có trạng thái hiện tại nên cần upsert theo `ticket_id`; feature daily là aggregate theo ngày nên xoá/ghi lại cửa sổ lookback đơn giản và idempotent.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ dấu vết delete và LSN mới nhất để replay batch cũ không làm ticket sống lại, đồng thời Gold có tín hiệu để loại dữ liệu khỏi snapshot mới/RAG.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: training set cần reproducible theo version; late feedback tạo version mới thay vì âm thầm thay đổi dữ liệu đã phát hành.
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: dữ liệu seed nhỏ, cần zero-key/local và kiểm chứng nhanh; dbt dùng để chứng minh cùng logic bằng SQL contract, Spark là quá nặng cho lab này.

## 4. Hai câu hỏi suy ngẫm

1. Với quyền xoá dữ liệu, em sẽ giữ snapshot bất biến ở tầng kỹ thuật nhưng thêm cơ chế redaction/tombstone theo subject id: khi T-97 bị xoá, các snapshot cũ được đánh dấu không được dùng để train/export, hoặc rebuild lại bản sanitized có audit log riêng. Như vậy lineage vẫn giải thích được, còn dữ liệu cá nhân không tiếp tục được phục vụ.
2. Regex chỉ là chốt tối thiểu ở Silver cho email/số điện thoại. Em sẽ thêm PII scanner ở Bronze->Silver cho free text, gồm rule cho tên tiếng Việt, địa chỉ, định danh và một bước ML/LLM classifier có quarantine; đo bằng test seed có nhãn, tỉ lệ leak trong Silver/Gold, và sampling định kỳ trước khi dữ liệu vào training/RAG.

## 5. Output

```text
$ make verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

$ make test
..................................                                       [100%]
34 passed in 1.12s

$ make rerun3
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ make lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ make dbt
Running with dbt=1.12.5
Registered adapter: duckdb=1.11.0
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.93 seconds (0.93s).
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ make parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
