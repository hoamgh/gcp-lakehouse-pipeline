# CHUYÊN ĐỀ PHỎNG VẤN: ĐIỀU HÀNH BẢNG DELTA LAKE TỐI ƯU
*(Delta Lake Maintenance: OPTIMIZE & VACUUM)*

## 1. Bản chất của OPTIMIZE và VACUUM khác nhau ra sao?
**Câu trả lời ghi điểm:** Cả 2 lệnh đều phục vụ tối ưu hóa nhưng tính chất hoàn toàn ngược nhau:
*   **OPTIMIZE (Compaction):** Là thao tác gom mảnh. Trong streaming, dữ liệu được ghi vào các file Parquet nhỏ (KB). OPTIMIZE gộp hàng ngàn file nhỏ thành 1 file lớn (chuẩn là 1GB) để giảm tải siêu dữ liệu (metadata) và tăng tốc độ đọc (I/O) cho các câu lệnh `SELECT`. Dữ liệu KHÔNG mất đi mà chỉ thay đổi cách đóng gói.
*   **VACUUM (Garbage Collection):** Do bản chất Delta Lake không bao giờ ghi đè hoặc xóa trực tiếp (để phục vụ Time Travel), lệnh OPTIMIZE gộp file xong sẽ chỉ "đánh dấu ẩn" các file nhỏ. VACUUM là người "lao công" thực thi việc xóa vật lý (Physical Delete) các file bị đánh dấu này ra khỏi ổ cứng.

## 2. Câu hỏi bẫy: Tại sao tạo file to xong không VACUUM xóa ngay file nhỏ đi để nhẹ ổ cứng, mà phải chờ 7 ngày?
**Phản biện (Red Flag nếu trả lời là xóa ngay):** "Trong môi trường Enterprise thực tế của Lakehouse (đọc/ghi phân tán cùng thời điểm), việc VACUUM ngay lập tức (0 hours) là cấm kỵ vì 3 rủi ro chí mạng gây Corrupt hệ thống:"

1.  **Sập luồng đọc (Concurrent Reads Conflict):** Giả sử một Dashboard kết nối trực tiếp vào Data Lake hoặc câu query của Data Analyst cực nặng cần 15 phút để chạy. Nếu luồng Đọc đang quét các file gốc mà tiến trình VACUUM quét qua xóa sổ file đó ngay lập tức, truy vấn sẽ văng lỗi `FileNotFoundException` và sập toàn bộ luồng BI.
2.  **Sập luồng ghi (Concurrent Writes/Streaming):** Khi chạy Spark Structured Streaming, file có thể mất vài phút mới được commit hoàn toàn vào Transaction Log. Nếu VACUUM dọn dẹp mù quáng, nó có thể cuốn bay luôn file đang được viết dở dang, gây corrupt bảng triệt để.
3.  **Tước bỏ quyền lợi Cứu Hộ (Disaster Recovery):** 7 ngày (168 tiếng) mặc định là "quãng thời gian vàng". Nó đủ dài để tuần sau một Data Engineer kịp thời rollback (`VERSION AS OF`) sửa lỗi ghi nhầm vào ngày Thứ Sáu; nhưng cũng đủ ngắn để không làm chi phí lưu trữ GCS/S3 phình to.

## 3. Câu hỏi nâng cao kinh nghiệm thực chiến: Đã từng chạy VACUUM 0 HOUR bao giờ chưa?
**Trả lời:** "Bản thân Delta Lake có một chốt khóa an toàn (Retention Duration Check) sẽ quăng lỗi thẳng mặt nếu chúng ta cố tình VACUUM dưới ngưỡng 168 giờ. Từng làm các Data Pipeline nhỏ hoặc demo cá nhân bị giới hạn dung lượng ổ cứng, em từng phải dùng lệnh `SET spark.databricks.delta.retentionDurationCheck.enabled = false` để tắt khóa cầu dao hòng giải phóng dung lượng ngay tại chỗ. Tuy nhiên ở cấp độ Production, em luôn đặt DAG chạy VACUUM 7 ngày riêng biệt với các luồng khác để đảm bảo tính toàn vẹn dư liệu tuyệt đối."
