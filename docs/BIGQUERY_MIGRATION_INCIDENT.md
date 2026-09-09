# BigQuery migration: nguyên nhân, ảnh hưởng và cách xử lý

## Tóm tắt

Đây không phải migration chỉ để đổi tên cột. Data model cũ có một số lỗi về
grain, CDC và referential integrity. Nếu chỉ deploy code mới rồi tiếp tục chạy
incremental, dữ liệu cũ vẫn giữ cấu trúc và lỗi cũ, trong khi dữ liệu mới dùng
quy tắc mới. Gold layer khi đó sẽ chứa hai định nghĩa dữ liệu khác nhau.

Vì vậy pipeline cần một lần migration có kiểm soát:

1. Sao lưu Gold hiện tại.
2. Dựng model mới trong dataset tạm.
3. Reconcile dữ liệu cũ và dữ liệu mới.
4. Chạy toàn bộ dbt tests.
5. Chỉ promote sau khi kiểm tra đạt.
6. Giữ backup để rollback và xóa các dataset tạm.

## Những lỗi gốc

### 1. `fact_order_items` không có grain ổn định

Source cũ không có `order_item_id`. Gold tạo surrogate key bằng:

```sql
FARM_FINGERPRINT(CONCAT(order_id, product_id, seller_id))
```

Khóa này giả định một order chỉ có tối đa một dòng cho cùng product và seller.
Trong thực tế, khách hàng có thể mua hai sản phẩm giống nhau từ cùng seller.
Hai dòng hợp lệ khi đó nhận cùng một khóa và làm sai test `unique`, phép merge
hoặc phép đếm item.

Code mới thêm `order_item_id` ngay từ generator. Với dữ liệu Silver lịch sử
chưa có cột này, staging tạo khóa tương thích từ toàn bộ thuộc tính item và số
thứ tự của các dòng trùng nhau. Nhờ vậy dữ liệu cũ vẫn migrate được mà không bị
xóa.

Đây là breaking schema change: checkpoint, Delta schema và bảng Gold cũ không
tự thay đổi chỉ vì Python schema đã được sửa.

### 2. CDC shipment từng ghi đè thuộc tính bằng `NULL`

CDC event cũ chỉ gửi trạng thái mới; các trường như `order_id`, `carrier` và
`tracking_number` được gửi dưới dạng `NULL`. Silver lại dùng:

```python
whenMatchedUpdateAll()
```

Khi shipment thay đổi trạng thái, Delta cập nhật toàn bộ cột và ghi đè các giá
trị trước đó bằng `NULL`. Đây là lý do nhiều shipment lịch sử không còn liên
kết được với order.

Code mới xử lý theo hai lớp:

- Generator phát full after-image cho CDC event.
- Silver chỉ nhận event có `event_timestamp` mới hơn và dùng `coalesce` để
  không xóa thuộc tính cũ bằng `NULL`.

Sửa code chỉ ngăn lỗi phát sinh trong tương lai; nó không thể tự phục hồi giá
trị đã mất trong dữ liệu hiện hữu. Vì vậy các shipment không thể khôi phục quan
hệ được chuyển vào quality dataset thay vì âm thầm xuất hiện trong Gold.

### 3. Watermark toàn bảng làm bỏ sót late-arriving shipment

Model cũ chỉ đọc event thỏa điều kiện:

```sql
event_timestamp > (select max(event_timestamp) from fact_shipments)
```

Ví dụ Gold đã có event lúc `10:00`, sau đó một shipment mới đến muộn với
timestamp `09:55`. Event `09:55` không lớn hơn max timestamp và bị bỏ qua vĩnh
viễn, dù `shipment_id` đó chưa từng tồn tại trong fact.

Model mới merge snapshot hiện tại của toàn bộ Silver theo `shipment_id`. Cách
này tốn thêm một lần scan Silver nhưng không bỏ mất shipment chỉ vì timestamp
đến không đúng thứ tự.

### 4. Dimension cũ chưa thể hiện đúng SCD

Các dimension cũ có cột `valid_from`, nhưng chỉ thêm `CURRENT_TIMESTAMP()` mỗi
lần build. Chúng không có `valid_to`, `is_current` hoặc một phiên bản riêng cho
mỗi lần thay đổi. Vì vậy đó chưa phải SCD Type 2.

Model mới áp dụng:

- `dim_customers`: SCD Type 2 bằng dbt snapshot, có `customer_sk`,
  `valid_from`, `valid_to`, `is_current` và `is_inferred`.
- `dim_products`: SCD Type 1, thuộc tính mới ghi đè thuộc tính cũ.
- `dim_sellers`: SCD Type 1; seller đến trễ được tạo inferred member.

`fact_orders.customer_sk` được gán khi order được load lần đầu và không bị đổi
khi customer có phiên bản mới. Đây là điều giữ lịch sử phân tích ổn định.

### 5. Gold chứa orphan facts

Kiểm tra trên dữ liệu thật phát hiện nhiều child record không tìm thấy parent
order hoặc seller tương ứng. Nguyên nhân có thể gồm event đến lệch thứ tự,
pipeline run trước đó không đồng bộ và dữ liệu đã bị CDC cũ làm mất khóa.

Không nên thực hiện một trong hai cách sau:

- Tắt relationship tests để pipeline nhìn có vẻ thành công.
- Xóa orphan rows mà không lưu lại dấu vết.

Migration đưa các record này vào `lakehouse_gold_quality`:

| Bảng quarantine | Số dòng lúc migration | Lý do |
| --- | ---: | --- |
| `dq_orphan_order_items` | 15,748 | Không tìm thấy parent order |
| `dq_orphan_payments` | 11,055 | Không tìm thấy parent order |
| `dq_orphan_reviews` | 2,551 | Không tìm thấy parent order |
| `dq_orphan_shipments` | 23,030 | Thiếu hoặc không tìm thấy parent order |

Những dòng này không bị mất. Chúng vẫn có thể được điều tra, replay hoặc đưa
trở lại Gold khi parent record xuất hiện.

## Vì sao bắt buộc phải chạy `--full-refresh` một lần

Incremental merge chỉ cập nhật hoặc thêm row theo cấu trúc bảng đang tồn tại.
Nó không đảm bảo sửa được:

- Grain và khóa surrogate đã tính sai trước đây.
- Cột mới như `customer_sk`, `purchase_date_id` và `order_item_id`.
- Việc tách payment và review thành fact riêng.
- Row đã bị bỏ sót bởi watermark cũ.
- Row cần chuyển từ Gold sang quality dataset.
- Dimension cần chuyển từ một dòng/business key sang nhiều version/business key.

Full refresh tái tính toàn bộ Gold từ Silver theo một định nghĩa duy nhất. Nó
không đồng nghĩa với xóa source data: Silver, Bronze và bản backup vẫn được giữ.

## Quy trình migration đã thực hiện

### Bước 1: Backup vật lý

Toàn bộ 7 bảng Gold cũ được copy sang:

```text
lakehouse_gold_backup_20260906
```

Row count của từng bảng source và backup đã được đối chiếu bằng nhau trước khi
tiếp tục.

### Bước 2: Canary build

Model mới được build trong `lakehouse_gold_v2`, snapshot và quality dataset
riêng. Lần build đầu giúp phát hiện lỗi schema và các orphan record trên dữ liệu
thật mà unit test cục bộ không thể thấy.

Sau khi sửa model và thêm quarantine, kết quả canary là:

```text
PASS=61 WARN=0 ERROR=0 SKIP=0
```

### Bước 3: Promote production

Cùng revision model đã pass được full-refresh vào `lakehouse_gold`. Kết quả
production cũng là:

```text
PASS=61 WARN=0 ERROR=0 SKIP=0
```

### Bước 4: Reconciliation

Production và canary được so sánh theo row count của cả 9 model chính. Tất cả
đều khớp. Tổng payment cũng khớp chính xác:

```text
4,386,281,309,000 VND
```

Sau đó bốn dataset `lakehouse_gold_v2*` mới được xóa. Backup không bị xóa.

## Trạng thái sau migration

| Model production | Row count |
| --- | ---: |
| `dim_customers` | 37,553 |
| `dim_date` | 2,224 |
| `dim_products` | 25,619 |
| `dim_sellers` | 12,793 |
| `fact_orders` | 98,215 |
| `fact_order_items` | 126,732 |
| `fact_payments` | 87,848 |
| `fact_reviews` | 20,491 |
| `fact_shipments` | 54,822 |

Số dòng Gold mới có thể thấp hơn Silver hoặc Gold cũ ở một vài fact vì orphan
records đã được chuyển sang quality dataset. Đây không phải data loss. Công thức
đối chiếu đúng là:

```text
conformed Gold rows + quarantined rows = eligible source rows
```

## Rollback

Nếu BI hoặc downstream consumer gặp vấn đề, dữ liệu cũ vẫn còn tại
`lakehouse_gold_backup_20260906`. Không nên xóa backup cho đến khi:

- Dashboard đã được kiểm tra với schema mới.
- Downstream query đã chuyển sang các fact mới.
- Một vài DAG incremental tiếp theo chạy thành công.
- Thời hạn lưu backup nội bộ đã kết thúc.

Rollback nên thực hiện bằng cách copy bảng backup sang một dataset phục hồi rồi
đổi consumer sang dataset đó. Không nên ghi đè production ngay khi chưa xác
định nguyên nhân.

## Cách phòng tránh lần sau

1. Chốt grain và natural key trước khi tạo fact table.
2. CDC event phải là full after-image hoặc merge từng cột có điều kiện.
3. Không dùng global max timestamp làm điều kiện duy nhất cho late-arriving data.
4. Relationship tests phải chạy; orphan data cần quarantine thay vì bị xóa.
5. Breaking schema change phải đi qua backup, canary, reconciliation và rollback plan.
6. Không deploy thẳng model chưa test vào dataset mà BI đang sử dụng.
