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
| C5 | Thời gian chạy batch | Từ CSV thô đến bảng dashboard (10 bảng khi đo `batch_engines`; 20 bảng analytics ở `load_times`) | s | `benchmarks/batch_engines.py` (3 lần ≤1M, 1 lần 5M), `load_times.py` |
| C6 | Khả năng mở rộng (scalability) | Thời gian và RAM tăng thế nào khi dữ liệu tăng 50 lần (100k→5M) | hệ số tăng, MB | cùng benchmark C5, RSS cả cây tiến trình lấy mẫu mỗi 0,5 s từ `/proc` |
| C7 | Dung lượng lưu trữ | Kích thước trên đĩa của cùng dữ liệu | MB | `benchmarks/storage_footprint.py` |
| C8 | Chịu lỗi (fault tolerance) | Cơ chế khôi phục khi tiến trình/node chết | định tính | **Không đo thực nghiệm** (xem phần 7); đánh giá theo cơ chế: replication, checkpoint, lưu offset |
| C9 | Độ phức tạp vận hành | Số thành phần phải cài, cấu hình, theo dõi | định tính 1-5 | Đếm dịch vụ/cấu hình trong `compose.yaml`, số lỗi cấu hình gặp khi triển khai |
| C10 | Chi phí | Chi phí tài nguyên/giấy phép để chạy | định tính | License (tất cả Apache-2.0) + RAM đo được ở C6/docker stats |
| C11 | Hệ sinh thái / connector | Có connector chính thức, tài liệu, cộng đồng | định tính 1-5 | Connector thực tế đã dùng (spark-cassandra-connector, cassandra-driver, flink-sql-connector-kafka) |
| C12 | Phù hợp mô hình truy cập | Truy vấn dashboard có ánh xạ tự nhiên không | định tính 1-5 | Ánh xạ BR → bảng/truy vấn (`docs/storage_decision.md`) |
| C13 | Chất lượng gợi ý ngoại tuyến (BR3) | Trên tập kiểm tra theo thời gian (30 ngày cuối), trong 10 phim gợi ý có bao nhiêu phim user thật sự xem sau đó | precision@10, recall@10, NDCG@10, hit rate@10 | `benchmarks/recommender_eval.py` → `recommender_eval.md` |
| C14 | Độ phủ và tỉ lệ phục vụ của gợi ý | Số phim khác nhau được gợi ý trên tổng danh mục; tỉ lệ user nhận đủ 10 phim từ chính phương pháp đó | % danh mục, % user | cùng benchmark |

Thang điểm 1-5 ở phần 6: 5 = tốt nhất trong các lựa chọn so sánh cho tiêu chí đó. Điểm dựa trên số đo khi tiêu chí
có số đo (ghi rõ), còn lại là nhận định của nhóm.

## 2. Tính đúng đắn (C1)

Chạy: `python -m pytest` (host, cần Java) và `docker compose --profile tools run --rm --no-deps -e RUN_HIVE_TESTS=1 benchmark python -m pytest -q` (đủ cả test tích hợp).

| Kiểm thử | Nội dung | Kết quả |
|---|---|---|
| `test_fixture_tables.py` | Bộ dữ liệu 4 user / 4 movie / 50 lượt xem (có timestamp ISO có offset, đủ 3 segment, phim chưa ai xem); giá trị từng ô của 19 bảng suy ra bằng tay (tăng trưởng, ngưỡng đồng đẳng và `reason`, phân vị nearest-rank, ma trận đối chiếu `activity_profile`, ngày thường / cuối tuần), chạy trên cả Pandas và Spark | đạt |
| `test_fixture_peer_tables.py`, `test_trends.py` | Bộ dữ liệu thứ hai (13 phim Comedy + 2 phim Drama chưa ai xem, ngày tham chiếu 2026-03-15) tính tay: cửa sổ tăng trưởng 7+7 ngày và sàn 10 lượt, hoà, ngưỡng p10 theo thể loại với ô đúng ngưỡng, thể loại không phim nào đủ 5 lượt, lưới thể loại × ngày đủ ô, trung bình trượt 7 ngày (8,14 và 20,43), chuỗi từng phim; `dense_series` dùng cùng định nghĩa trung bình trượt | đạt |
| `test_parity.py` | Pandas và Spark cho **19 bảng giống hệt** và cùng số dòng bị loại trên fixture, bộ dữ liệu đồng đẳng, dữ liệu sạch, dữ liệu bẩn và dữ liệu thưa; cờ `--csv-output-dir` tương thích. Bảng ALS chỉ có ở Spark nên nằm ngoài so sánh này | đạt |
| `test_recommendations.py` | BR3: bộ dữ liệu thứ hai (4 user, 8 phim) suy tay danh sách gợi ý của cả hai engine, gồm hoà điểm, phim bù theo độ phổ biến và user cold start; không có phim đã xem nào trong danh sách trên dữ liệu sạch, bẩn và thưa (cả Pandas lẫn Spark); mọi user có đủ 10 phim; ALS: đủ 10 hạng, không có phim đã xem, **hai lần chạy cùng seed cho kết quả giống hệt**, đổi seed thì kết quả đổi | đạt |
| `test_data_quality.py` | 7 luật (trùng id, timestamp không parse được, null, giá trị sai, timestamp tương lai, mồ côi user/movie — các dòng mồ côi sẽ bị mất âm thầm ở phép join nếu không đếm) với thứ tự ưu tiên; `--dirty-rate` chèn dòng bẩn thì số dòng bị loại **bằng đúng** số đã chèn | đạt |
| `test_cassandra_counts.py` | Số dòng 25 bảng Cassandra (24 bảng phục vụ và `quality_report`) = số dòng tính độc lập bằng Pandas từ CSV; `summary_metrics` khớp; rank 1..10 của `live_trending*`; 200 user ngẫu nhiên: không phim đã xem nào trong `user_recommendations` và `user_recommendations_als`; 50 danh sách theo thể loại bằng danh sách Pandas; `quality_report`, `batch_table_rows`, `batch_runs` khớp báo cáo Pandas và `COUNT(*)` | đạt |
| `test_storage_equivalence.py` | 23 truy vấn dashboard (12 cũ, 2 truy vấn gợi ý của BR3 và 9 truy vấn mới của BR2, BR8, BR9, BR10) cho kết quả giống hệt giữa Cassandra và Hive/Parquet | đạt |
| `test_schema_migration.py` | Volume Cassandra cũ được nâng cấp: tạo định nghĩa cũ của `low_engagement_movies` (không có `priority`), chạy `apply_schema`, bảng được tạo lại với cột mới (`priority`, `reason`, ...); lần chạy thứ hai không đổi gì; bảng giữ dữ liệu (`live_trending`) không bị xoá và gây `SchemaConflict` | đạt |
| `test_app_pages.py` | Streamlit `AppTest` chạy `app.py` trên dữ liệu thật: bốn trang (Viewer, Analyst, Manager, Data Engineering) hiển thị không ngoại lệ và không banner lỗi; ô chọn user ở Viewer và ô chọn phim ở Manager chỉ nhận tối đa 50 lựa chọn, kể cả sau khi gõ tìm kiếm | đạt |
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

Dữ liệu sinh vào thư mục tạm, users 50k, movies 20k, ratings = watches / 2. Toàn bộ: đọc CSV → làm sạch → 10 bảng → ghi CSV. Phép đo này làm trước khi thêm 8 bảng của BR2/BR8/BR9/BR10 (hiện 18 bảng, trong đó `movie_daily_views` có thêm một lần gom nhóm theo (phim, ngày)); thời gian của hai engine chưa đo lại cho 18 bảng.

| Số dòng watch | Pandas (s) | Spark (s) | RAM đỉnh Pandas (MB) | RAM đỉnh Spark (MB) | Đầu ra giống nhau |
|---|---|---|---|---|---|
| 100k | 9,9 | 54,9 | 196 | 2730 | có |
| 1M | 15,3 | 58,5 | 592 | 3602 | có |
| 5M | 31,5 | 104,7 | 2192 | 4393 | có |

* Với dữ liệu ≤ 5M trên một máy, **Pandas nhanh hơn Spark 3-6 lần**; Spark mất ~50 s cố định (khởi động JVM, lập kế hoạch, shuffle).
* Hệ số tăng khi dữ liệu tăng 50 lần: Pandas ×3,2 thời gian, ×11 RAM (từ 100k đến 5M; độ dốc biên 400 MB cho mỗi triệu dòng watch thêm, tính từ 592 MB ở 1M và 2.192 MB ở 5M trong `batch_engines.md`: RAM(N) ≈ 592 MB + (N − 1M) × 400 MB/M ⇒ hết 8 GB (8.192 MB) ở khoảng 20M dòng); Spark ×1,9 thời gian, RAM bị chặn bởi heap 4 GB (spill ra đĩa) nên mở rộng được vượt RAM — nhưng chỉ đo tới 5M, nên "mở rộng ngang" là suy luận, không phải số đo.
* Lần đo đầu (Spark tính lại dữ liệu đã làm sạch cho từng bảng đầu ra, chưa `persist`): 65 / 89 / 135 s (`batch_engines_spark_no_cache.md`); thêm `persist` còn 55 / 58 / 105 s.
  Lần đo đầu đó ghi `outputs_identical = False` ở 100k: do bảng `user_segments` — với 100k lượt xem không ai vượt 35 lượt nên Spark chỉ trả 2 segment (thiếu "Active") còn Pandas trả 3; đã sửa bằng cách join với danh sách segment cố định, chạy lại 100k với code hiện tại cho `True`. Cột RAM của Spark trong file đó (109-138 MB) chỉ đo tiến trình Python bọc ngoài, không gồm JVM, nên **không so sánh** với `batch_engines.md`.

Các giai đoạn của batch đầy đủ (1M dòng, `load_times.md`, lần đo sau khi thêm các bảng BR2/BR8/BR9/BR10): CSV→raw Parquet 18,4 s; →curated 52,2 s; kế hoạch bảng analytics (các phép tính vô hướng chạy ngay) 47,4 s; ALS huấn luyện và chấm điểm 40,7 s; →20 bảng analytics 149,8 s; →Cassandra (24 bảng phục vụ, `quality_report` và siêu dữ liệu) 53,5 s; tổng 362,0 s. Lần đo ngay trước đó (chưa có tám bảng mới, 12 bảng analytics): 87,3 / 50,8 / 36,8 / 37,3 / 76,5 / 48,6 s, tổng 337,3 s; lần đo đầu, khi chưa có gợi ý và ALS: 35,5 / 89,3 / 88,3 / 36,5, tổng 249,6 s. Giai đoạn analytics dài hơn do tám bảng mới, đặc biệt các phép gom nhóm theo (phim, ngày) và cửa sổ trượt; các giai đoạn dao động mạnh giữa các lần chạy trên máy này (`hive_raw` 18,4-209,9 s qua các lần chạy, vì máy dùng chung với Docker, trình duyệt và các tiến trình khác). Với các bảng mới, batch tự chạy trong 8 nhân (`SPARK_LOCAL_CORES`, mặc định 8) thay cho mọi nhân của máy (`local[*]`, 20 tác vụ song song): với `local[*]` và heap 1,5 GB, JVM của Spark bị hệ điều hành giết vì hết bộ nhớ (hai lần liên tiếp, sự kiện `oom` của Docker) trên máy ảo 8 GB đang chạy cả stack. Lần chạy đầu có ALS (heap 2 GB, giai đoạn `als` lúc đó gồm cả `build_tables`) mất 153,1 s; từ khi tách giai đoạn và lưu ứng viên trung gian xuống đĩa, ALS mất 37,3-44,4 s qua các lần chạy.

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

* Bảng này đo trước khi thêm các truy vấn của BR2 (đang lên), BR8 (so với đồng đẳng), BR9 (ngưỡng, ma trận) và BR10 (lưới thể loại, chuỗi từng phim, ngày thường / cuối tuần); các truy vấn đó đã có trong `src/dashboard_queries.py` và được kiểm tương đương Cassandra-Hive, nhưng độ trễ của chúng chưa đo bằng `storage_latency.py` (hàng "Phim ít xem nhất (BR8)" đo trên danh sách cũ, nay là `lowest_viewed`); thời gian Cassandra của chúng nằm ở cột `query_ms` của `br_effectiveness.md` (1-30 ms).
* Truy vấn theo khoá: Cassandra nhanh hơn **~100-400 lần** (1-2 ms so với 240-650 ms).
* Truy vấn quét toàn bảng 50k dòng: **hòa** (382 ms so với 383 ms) — lợi thế của Cassandra chỉ ở truy vấn theo khoá/top-N đã sắp sẵn.
* Chỉ Hive trả lời được câu hỏi ad hoc chưa có bảng tính sẵn (tuần bất kỳ, nhờ cắt tỉa partition `watch_date`).
* Thông lượng đọc Cassandra: 1.389 truy vấn/s với 8 luồng client (70% tra user, 30% top-10); bị giới hạn bởi client Python (GIL) chứ không phải node Cassandra, nên là cận dưới.
* Dung lượng (`storage_footprint.md`): CSV thô 74,3 MB; Parquet raw 28,0; curated 29,6; analytics 7,2 (20 bảng, gồm `movie_daily_views` 587.082 dòng và hai bảng gợi ý); keyspace Cassandra 43,6 MB không tính snapshot (24 bảng phục vụ, bảng siêu dữ liệu và hai bảng live; lần đo trước 31,4 MB cho 18 bảng). Dư thừa có chủ đích do mô hình query-first. Mỗi lần batch truncate các bảng và Cassandra tự chụp snapshot của dữ liệu cũ (khoảng 11-13 MB cho mỗi bảng gợi ý mỗi lần), và `DROP TABLE` ở bước nâng cấp lược đồ cũng chụp snapshot; snapshot được thu hồi bằng `nodetool clearsnapshot --all` trước khi đo. Parquet nhỏ hơn CSV ~2,5 lần.

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

### 3.4 Gợi ý BR3: ba phương pháp (C13, C14, C5) — `recommender_eval.md`

Cùng một dữ liệu, cùng cách loại phim đã xem, đánh giá ngoại tuyến bằng chia theo thời gian: huấn luyện trên sự kiện trước 2026-08-28 23:59 UTC (834.438 lượt xem, 417.330 đánh giá), kiểm tra trên 30 ngày cuối đến 2026-09-27 (165.562 lượt xem). Mỗi phương pháp chỉ thấy dữ liệu huấn luyện. Người được đánh giá là 40.865 user xem ít nhất một phim **mới** (chưa xem trước cutoff) trong giai đoạn kiểm tra, trung bình 3,8 phim mỗi user; 181 user trong đó chưa có lịch sử (cold start). Định nghĩa các chỉ số và cách tính ở phần ghi chú của `recommender_eval.md`; user không nhận được danh sách tính là 0 điểm.

| Phương pháp | precision@10 | recall@10 | NDCG@10 | hit rate@10 | độ phủ danh mục | user nhận đủ 10 phim | huấn luyện (s) | chấm điểm (s) |
|---|---|---|---|---|---|---|---|---|
| Popularity (mốc: phim xem nhiều nhất chưa xem) | 0,0314 | 0,0815 | 0,0543 | 0,2637 | 0,13% | 100,00% | 0,0 | 0,8 |
| Theo thể loại, xếp theo điểm Bayesian (triển khai) | 0,0147 | 0,0394 | 0,0299 | 0,1365 | 0,48% | 100,00% | 0,0 | 3,8 |
| Theo thể loại, xếp theo lượt xem (độ nhạy, không triển khai) | 0,0279 | 0,0726 | 0,0505 | 0,2374 | 0,53% | 100,00% | 0,0 | 3,9 |
| ALS rank 32, alpha 40 (triển khai) | 0,0262 | 0,0679 | 0,0472 | 0,2246 | 2,67% | 99,56% | 10,8 | 10,8 |
| ALS rank 16 (độ nhạy) | 0,0292 | 0,0732 | 0,0500 | 0,2448 | 2,79% | 99,56% | 5,4 | 5,7 |
| ALS rank 64 (độ nhạy) | 0,0218 | 0,0579 | 0,0421 | 0,1938 | 2,67% | 99,56% | 18,1 | 13,5 |
| ALS alpha 10 (độ nhạy) | 0,0273 | 0,0694 | 0,0494 | 0,2315 | 2,67% | 99,56% | 7,1 | 7,7 |

Cách đọc (mọi nhận xét dưới đây là về bộ dữ liệu sinh, nơi `generate_data.py` đặt 43% lượt xem của mỗi user vào thể loại ưa thích duy nhất của họ, 27% vào 34 phim blockbuster và 15% vào 500 phim "hit" cố định; không suy ra cho người xem thật):
* **Popularity (mốc so sánh) mạnh hơn cả hai phương pháp cá nhân hoá.** Precision@10 0,0314 so với 0,0262 của ALS và 0,0147 của gợi ý theo thể loại. Dữ liệu sinh dồn lượt xem vào ít phim (1% phim chiếm 33,24% lượt xem, `data_profile.md`), nên "phim được xem nhiều nhất mà user chưa xem" đã bắt được phần lớn tín hiệu; nhưng nó gợi ý cho mọi người gần như cùng một danh sách (độ phủ danh mục 0,13%).
* **ALS đạt 83% precision của popularity trong khi gợi ý cá nhân hoá** (độ phủ 2,67%, khoảng 20 lần popularity) và hơn gợi ý theo thể loại 1,8 lần về precision@10. Cái giá: huấn luyện 10,8 s và chấm điểm 10,8 s trên Spark `local[*]` (Pandas cho gợi ý theo thể loại mất 3,8 s, không cần huấn luyện), và 181 user cold start (0,44% số người được đánh giá) không có danh sách từ ALS: `users_with_full_list` = 0,9956. Dashboard xử lý bằng cách quay về danh sách theo thể loại.
* **Gợi ý theo thể loại (xếp theo điểm Bayesian) thấp nhất về độ chính xác** (hit rate 0,1365 so với 0,2637). Nguyên nhân chủ yếu là khoá xếp hạng: khi giữ nguyên ứng viên (hai thể loại xem nhiều nhất, chưa xem) và chỉ đổi khoá xếp từ điểm Bayesian sang lượt xem huấn luyện, precision@10 tăng từ 0,0147 lên 0,0279 và hit rate từ 0,1365 lên 0,2374, gần bằng popularity. Điểm đánh giá của dữ liệu sinh phản ánh "chất lượng" phim chứ không dự báo lượt xem sắp tới, nên các phim điểm cao nhưng ít người xem đứng đầu danh sách (hạn chế của khoá xếp hạng đã triển khai, không phải của việc chọn theo thể loại).
* **Độ nhạy của ALS** (các hàng "ALS rank 16/64", "ALS alpha 10" chỉ để xem độ nhạy, không dùng để chọn lại cấu hình đã triển khai, vì chọn theo tập kiểm tra sẽ là rò rỉ): rank 16 cho 0,0292, rank 64 cho 0,0218 precision@10; alpha 10 cho 0,0273. Cấu hình triển khai (`rank=32`, `alpha=40`) được chọn trước khi chạy đánh giá và không được chỉnh lại sau khi thấy kết quả.

## 4. Hiệu quả ứng dụng theo yêu cầu nghiệp vụ (đánh giá mục tiêu 5)

`python -m benchmarks.br_effectiveness` chạy các truy vấn của từng màn hình trên Cassandra và so với tính toán Pandas độc lập từ CSV (1M lượt xem).

| BR | Nhóm người dùng | Câu hỏi trả lời | Màn hình | Tiêu chí thành công | Kết quả đo | Đạt | Thời gian truy vấn (ms) |
|---|---|---|---|---|---|---|---|
| BR1 | Viewer | Tìm phim theo tên/thể loại | Viewer › BR1 (ô tìm kiếm + lọc thể loại) | browse-all returns every movie (8 genre partitions) | 20,000 of 20,000 movies | yes | 109.2 |
| BR1 | Viewer | Tìm phim theo tên/thể loại | Viewer › BR1 (ô tìm kiếm + lọc thể loại) | title search 'spider' finds the Spider-Man titles | 2 titles | yes | 109.2 |
| BR2 | Viewer | Phim nào đang thịnh hành (tổng, tăng trong tuần, cửa sổ 5 phút) | Viewer › Trending (bảng toàn thời gian, "Rising now", panel live) | all-time top-10 equals the Pandas ranking | True | yes | 2.2 |
| BR2 | Viewer | Phim nào đang thịnh hành (tổng, tăng trong tuần, cửa sổ 5 phút) | Viewer › Trending (bảng toàn thời gian, "Rising now", panel live) | live window has ranks 1..10 and is < 10 min old | 10 rows, newest window 7.2 min old | yes | 1.5 |
| BR2 | Viewer | Phim nào đang thịnh hành (tổng, tăng trong tuần, cửa sổ 5 phút) | Viewer › Trending (bảng toàn thời gian, "Rising now", panel live) | rising-now top-10 equals an independent growth ranking (reference date 2026-09-27) | True; best growth 1.056 (27 vs 8 views) | yes | 2.6 |
| BR2 | Viewer | Phim nào đang thịnh hành (tổng, tăng trong tuần, cửa sổ 5 phút) | Viewer › Trending (bảng toàn thời gian, "Rising now", panel live) | volume floor + damping keep tiny titles out of the top-10 (vs a naive recent/previous ratio) | mean recent views 22.2 vs 7.2 for the naive ratio; min 14; 460 of 20,000 movies pass the floor | yes | 2.6 |
| BR3 | Viewer | Nên xem phim gì tiếp theo (không phải phim đã xem) | Viewer › Recommended for you (chọn user và bộ gợi ý) | genre-based: no watched title is recommended (200 random users) | 0 watched titles in 2,000 recommendations for 200 users with a list | yes | 2.2 |
| BR3 | Viewer | Nên xem phim gì tiếp theo (không phải phim đã xem) | Viewer › Recommended for you (chọn user và bộ gợi ý) | genre-based: favourite-genre rows stay in the user's top-2 genres and lists equal the Pandas lists | 0 users with a foreign genre; 200/200 lists identical | yes | 2.2 |
| BR3 | Viewer | Nên xem phim gì tiếp theo (không phải phim đã xem) | Viewer › Recommended for you (chọn user và bộ gợi ý) | ALS: no watched title is recommended (200 random users) | 0 watched titles in 1,990 recommendations for 199 users with a list | yes | 2.2 |
| BR3 | Viewer | Nên xem phim gì tiếp theo (không phải phim đã xem) | Viewer › Recommended for you (chọn user và bộ gợi ý) | share of users that get favourite-genre recommendations (the rest are cold-start, shown popular titles) | 99.66% genre-based; every user has a list: True | yes | 0.0 |
| BR3 | Viewer | Nên xem phim gì tiếp theo (không phải phim đã xem) | Viewer › Recommended for you (chọn user và bộ gợi ý) | ALS serves every user with watch history (cold-start users fall back to the genre-based list) | 49,832 of 49,832 users with history (99.66% of all users) | yes | 372.6 |
| BR4 | Analyst | Phim nào được đánh giá cao đáng tin | Analyst › BR4 | top-10 by weighted rating equals the Pandas ranking | True | yes | 2.4 |
| BR4 | Analyst | Phim nào được đánh giá cao đáng tin | Analyst › BR4 | no top-10 title rests on very few ratings | min rating_count in top-10 = 169 | yes | 2.4 |
| BR5 | Analyst | Thể loại nào được xem nhiều, bởi bao nhiêu người | Analyst › BR5 (hai biểu đồ cột: lượt xem và người xem duy nhất) | sum of genre views = total views | 1,000,000 vs 1,000,000 | yes | 1.9 |
| BR5 | Analyst | Thể loại nào được xem nhiều, bởi bao nhiêu người | Analyst › BR5 (hai biểu đồ cột: lượt xem và người xem duy nhất) | unique viewers per genre equal the Pandas distinct count and never exceed the user count | 8 genres, max 42,915 of 50,000 users | yes | 1.9 |
| BR6 | Analyst | Mức độ tương tác: lượt xem, phút xem, số ngày hoạt động, giờ xem, gói thuê bao | Analyst › BR6 (thẻ chỉ số, ba biểu đồ phân phối, bộ lọc gói và vùng) | engagement scan covers all users; views add up | 50,000 users, 1,000,000 views | yes | 432.6 |
| BR6 | Analyst | Mức độ tương tác: lượt xem, phút xem, số ngày hoạt động, giờ xem, gói thuê bao | Analyst › BR6 (thẻ chỉ số, ba biểu đồ phân phối, bộ lọc gói và vùng) | watch minutes and active days per user equal the Pandas values | 94,838,155 minutes, active days 0-58 (mean 18.3, median 12), median watch minutes 1141 | yes | 432.6 |
| BR7 | Analyst | Phim nào hiệu quả (lượt xem, rating, completion) | Analyst › BR7 (scatter top 30%) | top 30% of movies by views are returned, with the minimum views above the 70th percentile | 6,000 movies, min views 37 | yes | 24.8 |
| BR8 | Manager | Phim nào ít tương tác so với cùng thể loại | Manager › BR8 | top-10 of the peer-relative list equals the Pandas list | reasons ['both'] | yes | 1.3 |
| BR8 | Manager | Phim nào ít tương tác so với cùng thể loại | Manager › BR8 | every genre has flagged titles that satisfy the peer rule; stored counts add up (8 genre partitions) | 4,171 of 20,000 titles; per-genre share 19.4%-23.2% | yes | 33.3 |
| BR8 | Manager | Phim nào ít tương tác so với cùng thể loại | Manager › BR8 | baseline: the 10 lowest-viewed titles (no peers) equal the Pandas list | 8 of 10 are Horror (Horror 8, Animation 2) | yes | 1.3 |
| BR9 | Manager | Phân khúc theo phân vị và mức khớp nhãn hoạt động | Manager › BR9 | segments cover all users exactly once | 50,000 users in 3 segments | yes | 1.2 |
| BR9 | Manager | Phân khúc theo phân vị và mức khớp nhãn hoạt động | Manager › BR9 | stored thresholds equal independent nearest-rank quantiles of views per user | p[25, 75] = [7, 37] views | yes | 0.9 |
| BR9 | Manager | Phân khúc theo phân vị và mức khớp nhãn hoạt động | Manager › BR9 | segments agree with the generator's activity_profile (confusion matrix, quantile vs former fixed 10/35) | 80.96% agreement vs 70.55% with fixed thresholds; matrix (profile -> Inactive/Casual/Active users): Inactive -> 9,464/495/0; Casual -> 3,646/18,783/0; Active -> 0/5,377/12,235 | yes | 1.6 |
| BR9 | Manager | Phân khúc theo phân vị và mức khớp nhãn hoạt động | Manager › BR9 | sensitivity of the agreement to the chosen quantile pair (information only) | p25/p75 (7/37 views) 81.0%; p33/p67 (9/31 views) 80.7%; p20/p80 (6/40 views) 76.6%; p20/p65 (6/26 views) 93.4% | yes | 0.0 |
| BR10 | Manager | Xu hướng theo ngày, thể loại, phim; ngày thường so với cuối tuần; ngưỡng cảnh báo | Manager › BR10 và cảnh báo | daily series has one point per day and adds up to total views | 180 days, 1,000,000 views | yes | 1.9 |
| BR10 | Manager | Xu hướng theo ngày, thể loại, phim; ngày thường so với cuối tuần; ngưỡng cảnh báo | Manager › BR10 và cảnh báo | genre x day grid is complete (days x genres) and its per-day sums equal the daily series | 1,440 rows = 180 days x 8 genres | yes | 5.6 |
| BR10 | Manager | Xu hướng theo ngày, thể loại, phim; ngày thường so với cuối tuần; ngưỡng cảnh báo | Manager › BR10 và cảnh báo | 7-day moving average of the last day equals a manual mean (Action) | 1574.71 vs 1574.71; first 6 days empty: True | yes | 5.6 |
| BR10 | Manager | Xu hướng theo ngày, thể loại, phim; ngày thường so với cuối tuần; ngưỡng cảnh báo | Manager › BR10 và cảnh báo | per-movie daily series adds up to the movie's views (200 random movies, 1 partition each) | 200/200 movies OK; sparse table holds 587,082 rows (a dense grid would be 3,600,000) | yes | 1.8 |
| BR10 | Manager | Xu hướng theo ngày, thể loại, phim; ngày thường so với cuối tuần; ngưỡng cảnh báo | Manager › BR10 và cảnh báo | weekday + weekend views add up to total views; weekend uplift per day is reported | weekday 5552.1/day over 128 days, weekend 5564.1/day over 52 days (+0.2%) | yes | 1.6 |
| BR10 | Manager | Xu hướng theo ngày, thể loại, phim; ngày thường so với cuối tuần; ngưỡng cảnh báo | Manager › BR10 và cảnh báo | Manager alerts: limits evaluated on the stored tables (Inactive share, flagged share, worst genre week on week) | Inactive 26.2% (limit 30%); flagged 20.9% (limit 25%); genre week on week -3.5% (Thriller) to +3.3% (Comedy), drop limit 10%; 0 of 3 alerts firing | yes | 0.0 |

Các tiêu chí mới của BR2 (danh sách đang lên), BR8 (so với nhóm đồng đẳng), BR9 (ngưỡng phân vị, mức khớp) và BR10 (lưới thể loại, trung bình trượt, chuỗi từng phim, ngày thường / cuối tuần) đã được thêm vào `benchmarks/br_effectiveness.py`; bảng trên là kết quả của lần chạy trước khi thêm chúng, nên các hàng mới sẽ xuất hiện sau lần chạy tiếp theo trên stack. Các số liệu tính độc lập bằng Pandas từ cùng CSV:

* **BR9, mức khớp giữa phân khúc theo phân vị (p25 = 7, p75 = 37 lượt xem) và nhãn `activity_profile` của bộ sinh: 80,96%** (40.482/50.000 user), so với 70,55% của ngưỡng cố định 10/35 cũ. Ma trận nhầm lẫn (hàng: nhãn của bộ sinh; cột: phân khúc suy ra từ lượt xem): Inactive → 9.464 / 495 / 0; Casual → 3.646 / 18.783 / 0; Active → 0 / 5.377 / 12.235 (Inactive / Casual / Active). Mọi sai lệch chỉ lệch một bậc. Cặp phân vị 33/67 cho 80,7%, 20/80 cho 76,6%, 20/65 (khớp tỉ lệ nhãn 20/45/35 của bộ sinh, cần biết trước nhãn) cho 93,4%.
* **BR8, so với nhóm đồng đẳng:** gắn cờ 4.171/20.000 phim (485-579 phim mỗi thể loại, 19,4%-23,2%), trong khi 10 phim ít xem nhất không xét thể loại gồm 8 phim Horror và 2 phim Animation.
* **BR2, danh sách đang lên:** 460/20.000 phim đủ sàn 10 lượt xem trong 7 ngày cuối (ngày tham chiếu 2026-09-27); top 10 có tối thiểu 14 lượt xem gần đây, trung bình 22,2, so với 7,2 nếu xếp theo tỉ lệ `recent/previous` thô.
* **BR10, ngày thường so với cuối tuần:** 5.552,1 so với 5.564,1 lượt xem mỗi ngày (+0,2%).

Thời gian truy vấn đo khi máy đang chạy cả stack streaming nên dao động (BR6 quét 50k dòng sáu cột: 485,0 ms ở lần đo này, 3.795,5 ms ở lần đo trước khi máy đang bận hơn; 382 ms cho truy vấn một cột ở phần 3.2 khi máy rảnh). Với BR3 `query_ms` là thời gian trung bình mỗi user (một truy vấn một partition). Chưa đo thời gian render của Streamlit và chưa có khảo sát người dùng thật: "hiệu quả ứng dụng" ở đây là đúng + đủ + nhanh của dữ liệu phục vụ từng màn hình.

## 5. Chi phí và vận hành: số liệu thực tế

* Stack demo gồm 11 dịch vụ Docker; bộ nhớ đo bằng `docker stats` khi chạy benchmark stream: Cassandra ~2,0 GiB, Flink TaskManager ~1,1 GiB, Spark Streaming ~1,1 GiB, Kafka ~1,0 GiB, JobManager ~0,8 GiB.
* Spark batch cần 337 s (~5,6 phút) cho 1M dòng ở dạng đầy đủ (Hive + Cassandra + ALS), 5-10 phút khi máy bận; driver 1,5 GB (`SPARK_DRIVER_MEMORY`) vì máy ảo Docker 8 GB đã chứa Cassandra, Kafka và Flink: với heap 2-3 GB, tiến trình Spark bị hệ điều hành kết thúc (OOM) trong lúc ghi bảng gợi ý 500.000 dòng; form "Record a movie view" chạy lại nền nên giao diện không bị khoá.
* Lỗi vận hành gặp khi làm: `cassandra-driver` trên Python 3.12 phải dùng reactor mặc định (AsyncioConnection treo), PySpark 3.5 `toPandas()` cần `distutils` (đã bị xoá ở 3.12), Derby metastore chỉ cho một tiến trình Spark, TaskManager 2 slot không chạy được hai job Flink (nay 4 slot).

## 6. Đánh giá công nghệ theo tiêu chí

### 6.1 Cassandra và Hive/Parquet (phục vụ dashboard)

| Tiêu chí | Cassandra | Hive/Parquet (Spark SQL) | Bằng chứng |
|---|---|---|---|
| C1 Đúng đắn | 5 | 5 | 23/23 truy vấn trả kết quả giống nhau |
| C2 Độ trễ truy vấn khoá | 5 | 1 | 1-2 ms so với 240-650 ms |
| C2 Độ trễ quét lớn / ad hoc | 3 | 4 | hòa ở 50k dòng; chỉ Hive làm được ad hoc |
| C4 Thông lượng | 4 | 2 | 1.389 qps (cận dưới) so với ~4 qps tuần tự (1/0,25 s) |
| C6 Mở rộng | 4 | 4 | suy luận (phân vùng/shard), chưa đo cụm nhiều node |
| C7 Dung lượng | 3 | 5 | 43,6 MB (cả keyspace, dư thừa, không tính snapshot) so với 7,2 MB analytics / 29,6 MB curated |
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
| C6 Mở rộng | 2 | 4 | RAM Pandas ≈ 592 MB + (N − 1M) × 400 MB/M (`batch_engines.md`), Spark bị chặn bởi heap |
| C9 Vận hành | 5 | 3 | Pandas chỉ cần pip; Spark cần Java, Ivy/Maven cho connector |
| C10 Chi phí | 4 | 3 | RAM đỉnh Spark ~3-4 GB đã cấp sẵn |
| C11 Hệ sinh thái | 3 | 5 | Spark có Hive, Cassandra, Kafka, Parquet |

Kết luận: ở quy mô đồ án Pandas đủ và nhanh hơn; Spark cần thiết khi dữ liệu vượt RAM một máy và để ghi Hive/Cassandra bằng connector.

### 6.4 Ba phương pháp gợi ý BR3

| Tiêu chí | Popularity (mốc) | Theo thể loại (giải thích được, dự phòng cold start) | ALS (mặc định trên dashboard) | Bằng chứng |
|---|---|---|---|---|
| C13 Chất lượng gợi ý | 5 | 2 | 4 | precision/NDCG/hit rate ở `recommender_eval.md` |
| C14 Độ phủ, cá nhân hoá | 1 | 2 | 4 | độ phủ 0,13% / 0,48% / 2,67% của danh mục |
| C14 Phục vụ user cold start | 5 | 5 | 1 | 100% / 100% / 99,56% user nhận đủ 10 phim |
| C5 Thời gian tính | 5 | 4 | 2 | gần 0 s / 3,8 s (Pandas) / 21,6 s (huấn luyện + chấm điểm, Spark) |
| C9 Vận hành | 5 | 4 | 3 | ALS cần thêm `pyspark.ml`, hyperparameter, và NumPy trên worker; không có bản Pandas |
| Giải thích được cho người xem | 3 | 5 | 2 | cột "Why": thể loại ưa thích / phổ biến / người xem tương tự |
| C1 Đúng đắn (không gợi ý phim đã xem) | 5 | 5 | 5 | `tests/test_recommendations.py`, `br_effectiveness.md` (200 user ngẫu nhiên, 0 vi phạm) |

Kết luận: không phương pháp nào thắng mọi tiêu chí. Popularity là mốc cần vượt và là phần bù cho user cold start; **ALS cá nhân hoá hơn và chính xác hơn gợi ý theo thể loại trên dữ liệu này nhưng cần lịch sử xem và tốn huấn luyện**; gợi ý theo thể loại rẻ nhất trong các phương pháp cá nhân hoá, giải thích được và luôn có kết quả. Dashboard cho người xem chọn giữa hai phương pháp cá nhân hoá và **mặc định là ALS** vì các số đo trên; gợi ý theo thể loại vẫn là lựa chọn thứ hai và là dự phòng cho user chưa có lịch sử. Siêu tham số ALS chưa được tinh chỉnh: việc đó cần một tập kiểm định riêng cắt từ giai đoạn huấn luyện (không dùng tập kiểm tra), là việc cần làm sau; điểm 1-5 là nhận định của nhóm dựa trên các số đo ở trên.

## 7. Giới hạn và những gì chưa đo

* Một máy, một node Cassandra (RF=1), một Kafka broker một partition, Flink parallelism 1: **không đo được mở rộng ngang và chịu lỗi thật** (C6 chỉ đo tăng dữ liệu trên một máy; C8 chỉ là đánh giá cơ chế). Chưa chạy thử kill node/TaskManager.
* Producer Python chỉ đạt ~6k sự kiện/s nên chưa tìm được điểm bão hòa của Flink SQL và Spark; các số là cận dưới.
* Các engine stream chạy đồng thời và tranh nhau CPU; các số tuyệt đối có thể khác khi chạy riêng. Các ô `flink-python` từ 1000 sự kiện/s trở lên bị ảnh hưởng bởi tồn đọng (drain tối đa 5 phút mỗi kịch bản).
* Hive đo bằng Spark SQL trong một phiên đã warm-up, kết quả được `collect()` về driver; không phải Hive on Tez/LLAP.
* Điểm 1-5 của các tiêu chí định tính (C8-C12) là nhận định của nhóm, không đo bằng thực nghiệm.
* Đánh giá gợi ý (C13, C14) là ngoại tuyến, trên dữ liệu tổng hợp và một mốc cắt thời gian duy nhất; chưa có thử nghiệm A/B hay người dùng thật, nên không biết gợi ý nào làm tăng giờ xem thật. Siêu tham số ALS không được tinh chỉnh, và các số về thời gian huấn luyện lẫn thời gian chấm điểm chỉ mang tính tham khảo vì Pandas và Spark được đo trên hai engine khác nhau, trên máy đang chạy cả stack.
* Chưa đo độ chính xác của danh sách theo từng nhóm người dùng (Inactive/Casual/Active) và chưa đo độ đa dạng thể loại trong danh sách.
