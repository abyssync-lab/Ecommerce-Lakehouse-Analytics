# Incremental Processing và Batch Metadata

Incremental processing của GlobalCart được thiết kế theo content identity, không
theo tên file. Điều này cho phép đổi tên file, retry sau lỗi và phát hiện batch
ID bị dùng lại với nội dung khác.

## Metadata của batch

### File manifest

Một row đại diện cho một source file theo khóa:

~~~text
(source_system, source_hash)
~~~

Các trường quan trọng bám đúng `FileManifest.SCHEMA`:

- source_system và source_hash;
- source_uri, file_size_bytes, contract_version;
- first_seen_at, first_run_id và last_run_id;
- bronze_status (`DISCOVERED`, `BRONZE_COMMITTED` hoặc `FAILED`);
- bronze_committed_at, raw_rows;
- last_error_code, last_error_message và last_updated_at.

File manifest là nơi trả lời file đã được commit vào Bronze chưa. Không đọc chung
event registry để suy luận trạng thái file.

### Pipeline run registry

Một row cho một run_id. Schema lifecycle duy nhất gồm:

~~~text
run_id, batch_id, source_system, source_uri, source_hash
status, started_at, completed_at
raw_rows, exact_duplicate_rows, rejected_rows, valid_event_rows
superseded_rows, inserted_rows, updated_rows, unchanged_rows
stale_rows, deleted_rows, orphan_delete_rows
gold_run_id, published_version
error_code, error_message
pipeline_version, contract_version
~~~

Registry update bằng merge/update theo run_id. Không append các row có schema
khác nhau cho từng trạng thái.

### Published snapshot

Một pointer cho publication name gold:

~~~text
publication_name
current_run_id
published_at
status
~~~

Power BI không đọc staging trực tiếp. Stable view join serving data với pointer
hiện hành.

## Hash và idempotency

Hash được tính bằng SHA-256 trên bytes của file:

~~~text
source_hash = sha256(file_content)
~~~

Luồng quyết định:

| Điều kiện | Kết quả |
|---|---|
| Hash chưa có | Register và process |
| Hash đã SUCCESS/PUBLISHED | SKIPPED |
| Hash FAILED | Retry được |
| Batch ID tồn tại với hash khác | Conflict và FAILED |
| Tên file đổi nhưng hash giống | SKIPPED |

Không được sinh hash từ filename, URI hoặc batch_id. Việc đó không bảo vệ được
replay khi content không đổi.

## Retry sau sự cố

Nếu process chết sau khi append Bronze nhưng trước khi manifest được cập nhật,
incremental flow đọc Bronze theo source_hash. Nếu event đã tồn tại, pipeline
không append lại mà khôi phục manifest về trạng thái Bronze committed.

Nếu process chết trong Gold staging, retry cùng run có thể ghi lại path staging
của run đó. Nếu publication chưa đổi, Power BI vẫn dùng run trước.

Nếu snapshot pointer đã đổi nhưng bước ghi metadata cuối lỗi, run được đánh dấu
PUBLISH_METADATA_PENDING để không đánh dấu FAILED sai một snapshot đã visible.

## Lifecycle status

~~~text
PROCESSING
  -> VALIDATED
  -> SILVER_MERGED
  -> GOLD_BUILT
  -> RECONCILED
  -> PUBLISHED
~~~

Các nhánh lỗi:

- FAILED: lỗi trước publication;
- SKIPPED: content hash đã xử lý thành công;
- PUBLISH_METADATA_PENDING: snapshot đã commit nhưng metadata update cuối chưa xong.

## Metrics bắt buộc

Run registry phải lưu raw_rows, rejected_rows, duplicate_rows, valid_event_rows,
superseded_rows, inserted_rows, updated_rows, unchanged_rows, stale_rows và
deleted_rows. Đây là operational evidence, không chỉ log.
