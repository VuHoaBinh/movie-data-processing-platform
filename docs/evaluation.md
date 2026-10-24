# Đánh giá hệ thống: tính đúng đắn, hiệu năng, hiệu quả ứng dụng và công nghệ

Mọi con số trong tài liệu này được đo thật trên máy phát triển (Windows 11, Docker Desktop/WSL2, 20 CPU logic,
8 GB cấp cho Docker; Spark 3.5.3 `local[*]`, Cassandra 5.0 một node RF=1, Flink 2.1, Kafka 4.0, Python 3.12 trong
container). Script và kết quả thô nằm trong `benchmarks/` và `benchmarks/results/` (CSV + markdown); lệnh chạy ở README.
Phần 7 liệt kê những gì **không** đo được hoặc chỉ là nhận định.

## 1. Tiêu chí đánh giá công nghệ (định nghĩa và cách đo)

| # | Tiêu chí | Định nghĩa | Đơn vị | Cách đo / bằng chứng |
|---|---|---|---|---|
| C1 | Tính đúng đắn dữ liệu (correctness) | Đầu ra của mỗi công nghệ bằng giá trị kỳ vọng tính độc lập; số dòng bị loại đúng bằng số dòng lỗi đã chèn | % bảng/ô đúng, số dòng | `tests/` (50 test): bảng fixture tính tay, parity Pandas = Spark, `--dirty-rate`, Cassandra = Hive; producer stream tự tính top-10 kỳ vọng |
| C2 | Độ trễ truy vấn (query latency) | Thời gian một truy vấn dashboard trả đủ dòng | ms, p50/p95 | `benchmarks/storage_latency.py`: 200 lần (Cassandra), 20 lần (Spark SQL) sau warm-up |
| C3 | Độ trễ kết quả stream (result latency) | Từ lúc kết thúc cửa sổ 10 s đến khi top-10 đúng của cửa sổ đó xuất hiện trong Cassandra | s, p50/p95 | `benchmarks/stream_latency.py`, mọi đồng hồ là của host |
| C4 | Thông lượng (throughput) | Khối lượng xử lý trên đơn vị thời gian | sự kiện/s, truy vấn/s, hàng/s | burst 100k sự kiện; 250-8000 sự kiện/s; 8 luồng đọc Cassandra |
| C5 | Thời gian chạy batch | Từ CSV thô đến 10 bảng dashboard | s | `benchmarks/batch_engines.py` (3 lần ≤1M, 1 lần 5M), `load_times.py` |
| C6 | Khả năng mở rộng (scalability) | Thời gian và RAM tăng thế nào khi dữ liệu tăng 50 lần (100k→5M) | hệ số tăng, MB | cùng benchmark C5, RSS cả cây tiến trình lấy mẫu mỗi 0,5 s từ `/proc` |
| C7 | Dung lượng lưu trữ | Kích thước trên đĩa của cùng dữ liệu | MB | `benchmarks/storage_footprint.py` |
| C8 | Chịu lỗi (fault tolerance) | Cơ chế khôi phục khi tiến trình/node chết | định tính | **Không đo thực nghiệm** (xem phần 7); đánh giá theo cơ chế: replication, checkpoint, lưu offset |
| C9 | Độ phức tạp vận hành | Số thành phần phải cài, cấu hình, theo dõi | định tính 1-5 | Đếm dịch vụ/cấu hình trong `compose.yaml`, số lỗi cấu hình gặp khi triển khai |
| C10 | Chi phí | Chi phí tài nguyên/giấy phép để chạy | định tính | License (tất cả Apache-2.0) + RAM đo được ở C6/docker stats |
| C11 | Hệ sinh thái / connector | Có connector chính thức, tài liệu, cộng đồng | định tính 1-5 | Connector thực tế đã dùng (spark-cassandra-connector, cassandra-driver, flink-sql-connector-kafka) |
| C12 | Phù hợp mô hình truy cập | Truy vấn dashboard có ánh xạ tự nhiên không | định tính 1-5 | Ánh xạ BR → bảng/truy vấn (`docs/storage_decision.md`) |

Thang điểm 1-5 ở phần 6: 5 = tốt nhất trong các lựa chọn so sánh cho tiêu chí đó. Điểm dựa trên số đo khi tiêu chí
có số đo (ghi rõ), còn lại là nhận định của nhóm.

## 2. Tính đúng đắn (C1)

Chạy: `python -m pytest` (host, cần Java) và `docker compose --profile tools run --rm --no-deps -e RUN_HIVE_TESTS=1 benchmark python -m pytest -q` (đủ cả test tích hợp).

| Kiểm thử | Nội dung | Kết quả |
|---|---|---|
| `test_fixture_tables.py` | Bộ dữ liệu 4 user / 4 movie / 50 lượt xem (có timestamp ISO có offset, đủ 3 segment, phim chưa ai xem); giá trị từng ô của 10 bảng suy ra bằng tay, chạy trên cả Pandas và Spark | đạt |
| `test_parity.py` | Pandas và Spark cho **10 bảng giống hệt** và cùng số dòng bị loại trên fixture, dữ liệu sạch, dữ liệu bẩn và dữ liệu thưa; cờ `--csv-output-dir` tương thích | đạt |
| `test_data_quality.py` | 7 luật (trùng id, timestamp không parse được, null, giá trị sai, timestamp tương lai, mồ côi user/movie — các dòng mồ côi sẽ bị mất âm thầm ở phép join nếu không đếm) với thứ tự ưu tiên; `--dirty-rate` chèn dòng bẩn thì số dòng bị loại **bằng đúng** số đã chèn | đạt |
| `test_cassandra_counts.py` | Số dòng 13 bảng Cassandra = số dòng tính độc lập bằng Pandas từ CSV; `summary_metrics` khớp; rank 1..10 của `live_trending*` | đạt |
| `test_storage_equivalence.py` | 12 truy vấn dashboard cho kết quả giống hệt giữa Cassandra và Hive/Parquet | đạt |
| `test_rounding.py` | `round_half_up` khác làm tròn chẵn của pandas | đạt |

Báo cáo dòng bị loại (`python -m src.data_quality`, Spark in ra log và lưu vào bảng Hive `curated.quality_report` mỗi lần batch; không có Hive thì dùng `--quality-report`), ví dụ
dữ liệu sinh với `--dirty-rate 0.05` (`benchmarks/results/data_quality_demo.txt`):

```
table              input      duplicate_id invalid_timestamp        null_value     invalid_value  future_timestamp       orphan_user      orphan_movie    output
users              5,000                 0                 0                 0                 0                 0                 0                 0     5,000
movies             2,000                 0                 0                 0                 0                 0                 0                 0     2,000
ratings           52,499               357               357               357               357               357               357               357    50,000
watch_history    104,998               714               714               714               714               714               714               714   100,000
```

Lỗi thật do bộ test phát hiện và đã sửa trong quá trình làm: (1) Spark gán `completion_rate = 100%` cho phim chưa
ai xem (`least()` bỏ qua null) trong khi Pandas cho null; (2) Spark bỏ mất segment rỗng ở bảng `user_segments`
(Pandas giữ đủ 3 segment) — chỉ lộ ra khi dữ liệu thưa; (3) hai pipeline trước đây không cùng quy tắc làm sạch
(chỉ khử trùng + `dropna`), nay dùng chung `src/data_quality.py`.
Đối chiếu stream: ở mọi kịch bản độ trễ, top-10 từ Flink SQL, Flink Python và Spark Streaming đều **đúng tuyệt đối**
so với giá trị kỳ vọng do producer tính (cột `windows_exact`), trừ các ô Flink Python bị quá tải ở phần 3.4.

## 3. Hiệu năng

### 3.1 Batch: Pandas và Spark (C5, C6) — `benchmarks/results/batch_engines.md`

Dữ liệu sinh vào thư mục tạm, users 50k, movies 20k, ratings = watches / 2. Toàn bộ: đọc CSV → làm sạch → 10 bảng → ghi CSV.

| Số dòng watch | Pandas (s) | Spark (s) | RAM đỉnh Pandas (MB) | RAM đỉnh Spark (MB) | Đầu ra giống nhau |
|---|---|---|---|---|---|
| 100k | 9,9 | 54,9 | 196 | 2730 | có |
| 1M | 15,3 | 58,5 | 592 | 3602 | có |
| 5M | 31,5 | 104,7 | 2192 | 4393 | có |

* Với dữ liệu ≤ 5M trên một máy, **Pandas nhanh hơn Spark 3-6 lần**; Spark mất ~50 s cố định (khởi động JVM, lập kế hoạch, shuffle).
* Hệ số tăng khi dữ liệu tăng 50 lần: Pandas ×3,2 thời gian, ×11 RAM (≈0,45 GB/1M dòng watch ⇒ ~16M dòng là hết 8 GB); Spark ×1,9 thời gian, RAM bị chặn bởi heap 4 GB (spill ra đĩa) nên mở rộng được vượt RAM — nhưng chỉ đo tới 5M, nên "mở rộng ngang" là suy luận, không phải số đo.
* Lần đo đầu (Spark tính lại dữ liệu đã làm sạch cho từng bảng đầu ra, chưa `persist`): 65 / 89 / 135 s (`batch_engines_spark_no_cache.md`); thêm `persist` còn 55 / 58 / 105 s.

Các giai đoạn của batch đầy đủ (1M dòng, `load_times.md`): CSV→raw Parquet 35,5 s; →curated 89,3 s; →10 bảng analytics 88,3 s; →Cassandra 36,5 s; tổng 249,6 s.

### 3.2 Lưu trữ: Cassandra và Hive/Parquet (C2, C4, C7) — `storage_latency.md`, `storage_footprint.md`

| Truy vấn (BR) | Cassandra p50 / p95 (ms) | Hive/Parquet (Spark SQL) p50 / p95 (ms) |
|---|---|---|
| Top 10 xem nhiều (BR2) | 1,4 / 1,9 | 275 / 304 |
| Gợi ý: thể loại của 1 user ngẫu nhiên (BR3) | 2,2 / 3,8 | 242 / 270 |
| Top 8 theo thể loại (BR3) | 1,4 / 2,0 | 269 / 303 |
| Top 10 rating (BR4) | 1,3 / 1,8 | 256 / 276 |
| Phổ biến theo thể loại (BR5) | 1,9 / 2,5 | 436 / 490 |
| Giờ xem (BR6) | 1,4 / 2,1 | 411 / 430 |
| Top 6000 phim (BR7) | 32 / 45 | 278 / 332 |
| Phim ít xem nhất (BR8) | 1,0 / 1,4 | 266 / 327 |
| Phân khúc (BR9) | 1,7 / 2,5 | 476 / 564 |
| Xu hướng ngày (BR10) | 1,7 / 2,4 | 526 / 592 |
| Danh mục một thể loại, 2505 dòng (BR1) | 20 / 30 | 656 / 759 |
| **Quét 50k user (BR6 histogram)** | **382 / 431** | **383 / 455** |
| Ad hoc: 1 tuần từ `curated.watch_events` | không có bảng | 485 / 522 |

* Truy vấn theo khoá: Cassandra nhanh hơn **~100-400 lần** (1-2 ms so với 240-650 ms).
* Truy vấn quét toàn bảng 50k dòng: **hòa** (382 ms so với 383 ms) — lợi thế của Cassandra chỉ ở truy vấn theo khoá/top-N đã sắp sẵn.
* Chỉ Hive trả lời được câu hỏi ad hoc chưa có bảng tính sẵn (tuần bất kỳ, nhờ cắt tỉa partition `watch_date`).
* Thông lượng đọc Cassandra: 1.389 truy vấn/s với 8 luồng client (70% tra user, 30% top-10); bị giới hạn bởi client Python (GIL) chứ không phải node Cassandra, nên là cận dưới.
* Dung lượng (`storage_footprint.md`): CSV thô 74,3 MB; Parquet raw 28,8; curated 29,7; analytics 1,4; 13 bảng Cassandra 37,0 MB (dư thừa có chủ đích do mô hình query-first). Parquet nhỏ hơn CSV ~2,5 lần.

### 3.3 Stream: Flink và Spark Structured Streaming (C3, C4) — `stream_latency.md`

Cùng một topic Kafka, cùng sự kiện, cửa sổ 10 s, top-10 mỗi cửa sổ; ba bộ xử lý chạy đồng thời trên một máy.
`flink-python` là job PyFlink DataStream của dự án; `flink-sql` là job Flink SQL (`flink/flink_trending_sql_job.py`, thêm trong quá trình đánh giá); `spark` là `src/spark_streaming_trending.py`.

| Tải | flink-python p50 / p95 (s) | flink-sql p50 / p95 (s) | spark p50 / p95 (s) |
|---|---|---|---|
| 250 sự kiện/s | 10,0 / 10,1 | 0,50 / 0,51 | 2,9 / 2,9 |
| 500 sự kiện/s | 20,7 / 26,1 | 0,53 / 0,77 | 2,7 / 2,8 |
| 1000 sự kiện/s | 79 / 98 | 0,55 / 0,75 | 2,7 / 2,9 |
| 2000 sự kiện/s | 152 / 188 | 0,71 / 1,5 | 3,0 / 3,1 |
| 4000 sự kiện/s | 219 / 279 (2/3 cửa sổ đúng) | 0,61 / 0,67 | 2,7 / 2,7 |
| ~6000 sự kiện/s (cấu hình 8000, producer Python chỉ đạt 5979) | không hoàn tất | 0,94 / 1,4 | 2,8 / 3,1 |
| Burst 100k sự kiện (thông lượng xử lý) | 330 sự kiện/s (xong sau 290 s) | 7.297 sự kiện/s (xong sau 0,6 s) | 3.849 sự kiện/s (xong sau 12,8 s) |

Cách đọc:
* Job PyFlink của dự án chịu được **~330-500 sự kiện/s** (đúng bằng tốc độ producer demo 500/s, tức không có dư địa). Từ 1000 sự kiện/s trở lên latency là do tồn đọng (backlog), không phải độ trễ ổn định. Chẩn đoán bằng REST của Flink: toán tử cửa sổ chạy ~70% busy ở 300 bản ghi/s — state/hàm Python của DataStream là điểm nghẽn (thử `python.execution-mode=thread` không chạy, chỉnh bundle chỉ tăng 480→640 sự kiện/s). Ở 250 sự kiện/s độ trễ 10 s = chu kỳ checkpoint (KafkaSink at-least-once xả khi checkpoint).
* Cùng engine Flink, khi viết bằng SQL (cửa sổ chạy trong JVM) đạt ≥ 7,3k sự kiện/s (bị giới hạn bởi producer) và độ trễ 0,5-1,4 s: **chênh lệch là do cách viết job, không phải do Flink so với Spark.**
* Spark Structured Streaming: độ trễ ổn định ~2,7-3,1 s (chu kỳ trigger 1 s + ghi Cassandra), không tăng theo tải tới ~6k sự kiện/s; không cần Kafka trung gian và materializer.
* Giới hạn phép đo: producer Python chỉ đạt ~6k sự kiện/s nên chưa tìm được điểm bão hòa của Flink SQL và Spark; chỉ có cận dưới.

## 4. Hiệu quả ứng dụng theo yêu cầu nghiệp vụ (đánh giá mục tiêu 5)

`python -m benchmarks.br_effectiveness` chạy các truy vấn của từng màn hình trên Cassandra và so với tính toán Pandas độc lập từ CSV (1M lượt xem).

| BR | Nhóm người dùng | Câu hỏi trả lời | Màn hình | Tiêu chí thành công | Kết quả đo | Đạt | Thời gian truy vấn (ms) |
|---|---|---|---|---|---|---|---|
| BR1 | Viewer | Tìm phim theo tên/thể loại | Viewer › BR1 (ô tìm kiếm + lọc thể loại) | browse-all returns every movie (8 genre partitions) | 20,000 of 20,000 movies | yes | 818.4 |
| BR1 | Viewer | Tìm phim theo tên/thể loại | Viewer › BR1 (ô tìm kiếm + lọc thể loại) | title search 'spider' finds the Spider-Man titles | 2 titles | yes | 818.4 |
| BR2 | Viewer | Phim nào đang thịnh hành (tổng và cửa sổ 5 phút gần nhất) | Viewer › BR2 (bảng + panel live, làm mới 10 s) | all-time top-10 equals the Pandas ranking | True | yes | 6.0 |
| BR2 | Viewer | Phim nào đang thịnh hành (tổng và cửa sổ 5 phút gần nhất) | Viewer › BR2 (bảng + panel live, làm mới 10 s) | live window has ranks 1..10 and is < 10 min old | 10 rows, newest window 8.1 min old | yes | 9.0 |
| BR3 | Viewer | Nên xem phim gì tiếp theo | Viewer › BR3 (chọn user) | recommendations stay in the user's top-2 genres (200 random users) | 199/199 users with history OK | yes | 14.9 |
| BR3 | Viewer | Nên xem phim gì tiếp theo | Viewer › BR3 (chọn user) | share of users that get personalised results | 99.66% (rest are cold-start, shown popular picks) | yes | 0.0 |
| BR4 | Analyst | Phim nào được đánh giá cao đáng tin | Analyst › BR4 | top-10 by weighted rating equals the Pandas ranking | True | yes | 15.6 |
| BR4 | Analyst | Phim nào được đánh giá cao đáng tin | Analyst › BR4 | no top-10 title rests on very few ratings | min rating_count in top-10 = 169 | yes | 15.6 |
| BR5 | Analyst | Thể loại nào được xem nhiều | Analyst › BR5 (biểu đồ cột) | sum of genre views = total views | 1,000,000 vs 1,000,000 | yes | 20.5 |
| BR6 | Analyst | Mức độ tương tác của người dùng, giờ xem, gói thuê bao | Analyst › BR6 | engagement histogram covers all users; views add up | 50,000 users, 1,000,000 views | yes | 3795.5 |
| BR7 | Analyst | Phim nào hiệu quả (lượt xem, rating, completion) | Analyst › BR7 (scatter top 30%) | top 30% of movies by views are returned, with the minimum views above the 70th percentile | 6,000 movies, min views 37 | yes | 254.5 |
| BR8 | Manager | Phim nào ít người xem | Manager › BR8 | 10 lowest-viewed titles equal the Pandas list | views [1, 1, 2, 2, 2, 2, 2, 2, 2, 2] | yes | 10.4 |
| BR9 | Manager | Phân khúc người dùng | Manager › BR9 | segments cover all users exactly once | 50,000 users in 3 segments | yes | 15.9 |
| BR10 | Manager | Xu hướng theo ngày | Manager › BR10 | daily series has one point per day and adds up to total views | 180 days, 1,000,000 views | yes | 21.4 |

Thời gian truy vấn đo khi máy đang chạy cả stack streaming và benchmark nên dao động (BR6 quét 50k dòng: 382 ms ở phần 3.2 khi máy rảnh). Chưa đo thời gian render của Streamlit và chưa có khảo sát người dùng thật: "hiệu quả ứng dụng" ở đây là đúng + đủ + nhanh của dữ liệu phục vụ từng màn hình.

## 5. Chi phí và vận hành: số liệu thực tế

* Stack demo gồm 11 dịch vụ Docker; bộ nhớ đo bằng `docker stats` khi chạy benchmark stream: Cassandra ~2,0 GiB, Flink TaskManager ~1,1 GiB, Spark Streaming ~1,1 GiB, Kafka ~1,0 GiB, JobManager ~0,8 GiB.
* Spark batch cần ~4 phút cho 1M dòng ở dạng đầy đủ (Hive + Cassandra); form "Record a movie view" chạy lại nền nên giao diện không bị khoá.
* Lỗi vận hành gặp khi làm: `cassandra-driver` trên Python 3.12 phải dùng reactor mặc định (AsyncioConnection treo), PySpark 3.5 `toPandas()` cần `distutils` (đã bị xoá ở 3.12), Derby metastore chỉ cho một tiến trình Spark, TaskManager 2 slot không chạy được hai job Flink (nay 4 slot).

## 6. Đánh giá công nghệ theo tiêu chí

### 6.1 Cassandra và Hive/Parquet (phục vụ dashboard)

| Tiêu chí | Cassandra | Hive/Parquet (Spark SQL) | Bằng chứng |
|---|---|---|---|
| C1 Đúng đắn | 5 | 5 | 12/12 truy vấn trả kết quả giống nhau |
| C2 Độ trễ truy vấn khoá | 5 | 1 | 1-2 ms so với 240-650 ms |
| C2 Độ trễ quét lớn / ad hoc | 3 | 4 | hòa ở 50k dòng; chỉ Hive làm được ad hoc |
| C4 Thông lượng | 4 | 2 | 1.389 qps (cận dưới) so với ~4 qps tuần tự (1/0,25 s) |
| C6 Mở rộng | 4 | 4 | suy luận (phân vùng/shard), chưa đo cụm nhiều node |
| C7 Dung lượng | 3 | 5 | 37 MB (13 bảng, dư thừa) so với 1,4 MB analytics / 29,7 MB curated |
| C8 Chịu lỗi | 4 | 3 | replication + commitlog (RF=1 ở demo nên chưa có); Hive: file Parquet nhưng Derby một node |
| C9 Vận hành | 3 | 3 | Cassandra thêm 1 dịch vụ + schema; Hive cần metastore nhúng |
| C10 Chi phí | 3 | 5 | Cassandra ~2 GiB RAM thường trực; Parquet chỉ là file |
| C11 Hệ sinh thái | 4 | 5 | cả hai có connector Spark chính thức |
| C12 Phù hợp truy cập | 5 (khoá/top-N) | 4 (quét/ad hoc) | ánh xạ BR → bảng |

Kết luận: dùng **kết hợp** — Cassandra phục vụ dashboard, Hive/Parquet làm lake để tính lại và truy vấn ad hoc.

### 6.2 Flink và Spark Structured Streaming (top-10 theo cửa sổ)

| Tiêu chí | Flink (Python DataStream) | Flink (SQL) | Spark Structured Streaming | Bằng chứng |
|---|---|---|---|---|
| C1 Đúng đắn | 5 | 5 | 5 | top-10 khớp kỳ vọng ở mọi cửa sổ không bị quá tải |
| C3 Độ trễ kết quả | 2 | 5 | 3 | 10-20 s ở tải thấp / 0,5-1,4 s / 2,7-3,1 s |
| C4 Thông lượng | 1 | 5 | 4 | 330 / ≥7.297 / 3.849 sự kiện/s (burst) |
| C6 Mở rộng | 3 | 4 | 4 | suy luận: song song theo partition; chưa đo |
| C8 Chịu lỗi | 4 | 4 | 4 | Flink: checkpoint 10 s + offset Kafka (checkpoint nằm trong container, chưa có volume); Spark: checkpoint offset/state đặt trên named volume `spark-checkpoint` nên sống sau khi restart container — cơ chế có, **chưa thử kill** |
| C9 Vận hành | 2 | 3 | 4 | Flink cần JobManager/TaskManager + Kafka trung gian + materializer; Spark chạy một tiến trình, ghi thẳng Cassandra |
| C10 Chi phí | 2 | 3 | 3 | RAM: TM ~1,1 GiB + JM ~0,8 GiB so với ~1,1 GiB |
| C11 Hệ sinh thái | 4 | 5 | 5 | Spark: chung connector với batch; Flink SQL: connector Kafka chính thức |

Kết luận: giữ Flink cho BR2 nhưng **đã chuyển job mặc định sang Flink SQL** (`compose.yaml`): khi chạy stack đầy đủ, job PyFlink không theo kịp producer demo 500 sự kiện/s (backlog tăng dần, kết quả live chậm vài giờ sau ~35 phút chạy — quan sát thật), job SQL thì bắt kịp. Spark Structured Streaming là phương án thay thế vận hành đơn giản hơn với độ trễ ~3 s.

### 6.3 Spark và Pandas (batch)

| Tiêu chí | Pandas | Spark | Bằng chứng |
|---|---|---|---|
| C1 Đúng đắn | 5 | 5 | cùng 10 bảng ở 100k/1M/5M và dữ liệu bẩn |
| C5 Thời gian (≤5M) | 5 | 2 | 9,9-31,5 s so với 55-105 s |
| C6 Mở rộng | 2 | 4 | RAM Pandas ≈0,45 GB/1M dòng, Spark bị chặn bởi heap |
| C9 Vận hành | 5 | 3 | Pandas chỉ cần pip; Spark cần Java, Ivy/Maven cho connector |
| C10 Chi phí | 4 | 3 | RAM đỉnh Spark ~3-4 GB đã cấp sẵn |
| C11 Hệ sinh thái | 3 | 5 | Spark có Hive, Cassandra, Kafka, Parquet |

Kết luận: ở quy mô đồ án Pandas đủ và nhanh hơn; Spark cần thiết khi dữ liệu vượt RAM một máy và để ghi Hive/Cassandra bằng connector.

## 7. Giới hạn và những gì chưa đo

* Một máy, một node Cassandra (RF=1), một Kafka broker một partition, Flink parallelism 1: **không đo được mở rộng ngang và chịu lỗi thật** (C6 chỉ đo tăng dữ liệu trên một máy; C8 chỉ là đánh giá cơ chế). Chưa chạy thử kill node/TaskManager.
* Producer Python chỉ đạt ~6k sự kiện/s nên chưa tìm được điểm bão hòa của Flink SQL và Spark; các số là cận dưới.
* Các engine stream chạy đồng thời và tranh nhau CPU; các số tuyệt đối có thể khác khi chạy riêng. Các ô `flink-python` từ 1000 sự kiện/s trở lên bị ảnh hưởng bởi tồn đọng (drain tối đa 5 phút mỗi kịch bản).
* Hive đo bằng Spark SQL trong một phiên đã warm-up, kết quả được `collect()` về driver; không phải Hive on Tez/LLAP.
* Điểm 1-5 của các tiêu chí định tính (C8-C12) là nhận định của nhóm, không đo bằng thực nghiệm.
