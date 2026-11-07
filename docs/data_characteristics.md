# Nguồn dữ liệu và đặc điểm dữ liệu (mục 2 của báo cáo kỹ thuật)

Tài liệu này mô tả dữ liệu mà hệ thống Movie Streaming Analytics Platform xử lý, các đặc điểm Big Data của nó và
những quyết định thiết kế rút ra từ các đặc điểm đó. Mọi con số đều lấy từ một file trong `benchmarks/results/` và
được ghi nguồn ngay cạnh số liệu:

* `data_profile.md` / `data_profile.csv`: kết quả `python -m src.profile_data` chạy trên `data/` (bảng gốc, chưa làm sạch);
* `data_quality_real.txt`, `data_quality_demo.txt`: báo cáo dòng bị loại của 7 luật chất lượng (dữ liệu thật trong `data/` và dữ liệu bẩn có chủ đích);
* `storage_footprint.md`, `load_times.md`, `batch_engines.md`, `storage_latency.md`, `stream_latency.md`, `br_effectiveness.md`: các phép đo hiệu năng.

Những con số **tính từ** các số trên (nhân, chia, ước lượng tăng trưởng) được đánh dấu "tính từ" kèm phép tính và giả định.
Phần nào là nhận định thì ghi rõ là nhận định, không phải số đo.

## 1. Nguồn dữ liệu

### 1.1 Bốn nguồn batch (CSV)

**Trung thực về nguồn gốc:** cả bốn file CSV trong `data/` là dữ liệu **tổng hợp (synthetic)**, không phải dữ liệu của một nền
tảng thật. Chúng được sinh bởi `generate_data.py` với `random.seed(42)` nên chạy lại cho đúng cùng một dữ liệu. Tên
phim "gần đây" (34 tên trong `RECENT_TITLES`, ví dụ *Inside Out 2*, *Mai*) là tên thật để dashboard dễ hiểu; phần
còn lại là tên ghép ngẫu nhiên (ví dụ "Neon Horizon 0035"). Người dùng, lượt xem và đánh giá đều do chương trình sinh ra.

| Nguồn | File | Cách sinh trong `generate_data.py` | Nguồn thật mà nó đại diện (giả định thiết kế) | Quy mô (`data_profile.md`) |
|---|---|---|---|---|
| `users` | `users.csv` | Gán ngẫu nhiên tuổi 18-60, giới tính, 5 vùng, ngày đăng ký, gói thuê bao (trọng số 45/40/15) và `activity_profile` Active/Casual/Inactive (35/45/20) | Cơ sở dữ liệu tài khoản / CRM của nền tảng (OLTP) | 50.000 dòng, 7 cột, 2,42 MB |
| `movies` | `movies.csv` | 34 phim đầu là tên gần đây có sẵn thể loại; các phim còn lại có thể loại luân phiên theo `movie_id`; năm phát hành 1995-2026 (nặng về năm gần đây); thời lượng 80-180 phút; 85% Movie / 15% Series; ngôn ngữ ngẫu nhiên | Danh mục nội dung (CMS / catalogue) | 20.000 dòng, 8 cột, 1,15 MB |
| `watch_history` | `watch_history.csv` | Mỗi lượt xem chọn người dùng theo `activity_profile` (trọng số Active 72 / Casual 24 / Inactive 4), phim theo hỗn hợp: 43% theo thể loại ưa thích, 27% bom tấn, 15% "hit", phần còn lại phim mới/ngẫu nhiên; giờ xem có đỉnh buổi tối; thời lượng xem phụ thuộc "chất lượng" phim và gói Premium | Log sự kiện phát video (player / CDN), dạng clickstream | 1.000.000 dòng, 6 cột, 49,28 MB |
| `ratings` | `ratings.csv` | Lấy mẫu ngẫu nhiên 500.000 lượt xem; điểm = hàm của chất lượng phim, tỉ lệ xem xong và nhiễu Gauss, làm tròn 1-5 | Dịch vụ đánh giá của người dùng (OLTP) | 500.000 dòng, 5 cột, 21,46 MB |

Các tham số sinh (mặc định `--users 50000 --movies 20000 --ratings 500000 --watches 1000000`) và cột của từng file nằm trong
`data_dictionary.md`. Tổng dung lượng CSV 74,3 MB (`storage_footprint.md`, bằng tổng bốn file ở trên).
Vì dữ liệu sinh ra có cấu trúc rõ ràng nên **các kết quả phân tích (ví dụ phim nào hot, giờ nào đông) phản ánh tham số của bộ sinh dữ liệu,
không phải hành vi thật**. Giá trị của đồ án nằm ở đường ống xử lý và cách đo nó, không phải ở các insight.

Bộ sinh còn có một tuỳ chọn `--dirty-rate` chèn có chủ đích bảy loại dòng lỗi để chứng minh các luật chất lượng (mục 3.4).

### 1.2 Nguồn stream (Kafka)

* **Topic `watch-events`**, thông điệp JSON do `src/kafka_event_producer.py` (hàm `send_events`) gửi, mỗi thông điệp có
  `watch_id`, `user_id`, `movie_id`, `title`, `genre`, `source_timestamp`; khoá thông điệp là `movie_id`.
  **Thời gian sự kiện (event time) là timestamp của bản ghi Kafka**, lấy tại thời điểm gửi, không phải `source_timestamp`; nghĩa là thực chất gần với ingestion time, watermark độ trễ 0 s và dữ liệu đến muộn chưa được xử lý (event time thật từ `source_timestamp` là cải tiến dự kiến, task #19).
* **Đây là nguồn demo lặp lại được, không phải lưu lượng thật:** producer đọc `watch_history.csv` từ đầu đến cuối, phát lại từng
  dòng với tốc độ cấu hình (mặc định `--rate 500` sự kiện/s, `--limit 100000` sự kiện mỗi vòng rồi lặp lại), gắn timestamp hiện tại.
  Do vậy các lượt xem phát lại là lịch sử cũ được "đóng dấu" thời gian mới.
* **Form "Record a movie view" của dashboard** (`app.py`) tạo một sự kiện thật từ người dùng: ghi một dòng vào
  `data/watch_history.csv` (mã `FORM-<uuid>`, timestamp UTC dạng `YYYY-MM-DD HH:MM:SS`), gửi cùng sự kiện đó vào Kafka nếu Kafka
  chạy, rồi khởi động lại batch Spark ở nền.
* Kết quả stream: Flink SQL ghi top-10 mỗi cửa sổ vào topic `trending-results`, `src/trending_materializer.py` ghi vào Cassandra
  `live_trending`.

### 1.3 Các kho dữ liệu dẫn xuất

| Kho | Nội dung | Dung lượng (`storage_footprint.md`) |
|---|---|---|
| Hive/Parquet `raw.*` | Bản sao Parquet của 4 CSV, schema tường minh, timestamp còn là chuỗi | 28,8 MB |
| Hive/Parquet `curated.*` | Đã làm sạch bằng 7 luật, timestamp UTC; `curated.watch_events` phân vùng theo `watch_date`; thêm `curated.quality_report` | 29,7 MB |
| Hive/Parquet `analytics.*` | 21 bảng tổng hợp phục vụ dashboard (20 bảng dùng chung với Pandas, `user_recommendations_als`) | 7,2 MB (`storage_footprint.md`) |
| Cassandra `movie_analytics` | 28 bảng do batch ghi theo mô hình query-first (25 bảng của dashboard, `quality_report` và hai bảng siêu dữ liệu `batch_table_rows`, `batch_runs`), cộng 2 bảng `live_trending`, `live_trending_spark` cho stream | 43,6 MB cho cả keyspace, không tính snapshot (RF=1) |

Luồng: `CSV → raw → curated → analytics → Cassandra` (`src/spark_batch_job.py`, hàm `run`). Cassandra là bản phục vụ có thể dựng lại bất cứ lúc nào.

## 2. Lược đồ và quan hệ

| Bảng | Khoá chính | Khoá ngoại | Ghi chú | Số khác biệt (`data_profile.md`) |
|---|---|---|---|---|
| `users` | `user_id` (`U00001`...) | không | 50.000 `user_id` duy nhất | `region` 5, `subscription_plan` 3, `gender` 3 |
| `movies` | `movie_id` (`M00001`...) | không | 20.000 `movie_id` và 20.000 `title` duy nhất | `genre` 8, `language` 5, `content_type` 2 |
| `ratings` | `rating_id` (`R000001`...) | `user_id → users`, `movie_id → movies` | 0 mồ côi | 48.587 user và 19.979 phim có ít nhất một đánh giá |
| `watch_history` | `watch_id` (`W000001`...) | `user_id → users`, `movie_id → movies` | 0 mồ côi | 49.832 user và 20.000 phim có ít nhất một lượt xem |

Quan hệ và số lượng (`data_profile.md`; cột "Trung bình" là **tính từ** số dòng chia số dòng bảng cha):

| Quan hệ | Lực lượng | Thực tế trong dữ liệu |
|---|---|---|
| `users 1—N watch_history` | 0..N lượt xem mỗi user | 168 user (0,34%) không có lượt xem nào; trung bình 1.000.000 / 50.000 = 20 lượt/user |
| `movies 1—N watch_history` | 0..N lượt xem mỗi phim | không phim nào có 0 lượt xem; trung bình 1.000.000 / 20.000 = 50 lượt/phim |
| `users 1—N ratings` | 0..N đánh giá mỗi user | 1.413 user chưa đánh giá phim nào |
| `movies 1—N ratings` | 0..N đánh giá mỗi phim | 21 phim chưa có đánh giá; trung vị 13 đánh giá/phim |
| `ratings` và `watch_history` | không có khoá ngoại trực tiếp | bộ sinh lấy mẫu đánh giá từ lượt xem nên cùng cặp (user, movie) |

Sự toàn vẹn khoá ngoại và tính duy nhất khoá chính được kiểm tra trong `data_profile.md` (mục `orphans`, `keys`): mọi khoá chính duy nhất,
mọi `orphans` bằng 0. Khoá ngoại không được cơ sở dữ liệu ép buộc (CSV không có ràng buộc); luật `orphan_user` và `orphan_movie`
trong `src/data_quality.py` thay vai trò đó khi làm sạch.

Lược đồ phục vụ trong Cassandra (28 bảng do batch ghi, gồm hai bảng gợi ý và chín bảng của BR2/BR8/BR9/BR10, `quality_report` và 2 bảng siêu dữ liệu, cộng 2 bảng stream) nằm trong `sql/cassandra_schema.cql`; ánh xạ BR → bảng ở `docs/storage_decision.md` và mục 4 của `docs/business_requirements.md`.

## 3. Đặc điểm Big Data

### 3.1 Volume (khối lượng)

| Đại lượng | Giá trị | Nguồn |
|---|---|---|
| Số dòng | users 50.000; movies 20.000; ratings 500.000; watch_history 1.000.000 | `data_profile.md` |
| CSV thô | 74,3 MB (watch_history 49,28 MB; ratings 21,46 MB) | `storage_footprint.md`, `data_profile.md` |
| Parquet raw / curated / analytics | 28,0 / 29,6 / 7,2 MB | `storage_footprint.md` |
| Cassandra (cả keyspace, không tính snapshot, RF=1, nén LZ4) | 43,6 MB | `storage_footprint.md` |
| Khoảng thời gian | 180 ngày (2026-04-01 đến 2026-09-27) | `data_profile.md` |
| Thời gian batch đầy đủ (1M dòng xem) | 337,3 s | `load_times.md` |
| Pandas / Spark ở 1M và 5M dòng xem | 15,3 s / 58,5 s và 31,5 s / 104,7 s; RAM đỉnh 592 / 3.602 MB và 2.192 / 4.393 MB | `batch_engines.md` |

**Ước lượng tăng trưởng (tính từ, giả định nêu rõ).** Giả định: số lượt xem mỗi người dùng mỗi ngày không đổi; kích thước file tuyến tính theo số dòng; không tính users/movies (nhỏ, tăng chậm).

| Kịch bản | Lượt xem / 180 ngày | Lượt xem / năm | CSV hai bảng sự kiện / năm | Cách tính |
|---|---|---|---|---|
| Hiện tại (50k user) | 1.000.000 | ≈ 2,03 triệu | ≈ 143 MB | 1.000.000 × 365/180; (49,28 + 21,46) MB × 365/180 |
| 10 lần user (500k) | 10 triệu | ≈ 20,3 triệu | ≈ 1,43 GB | nhân 10 |
| 100 lần user (5 triệu) | 100 triệu | ≈ 203 triệu | ≈ 14,3 GB | nhân 100 |

Hệ quả (tính từ `batch_engines.md`): RAM đỉnh của Pandas tăng từ 592 MB (1M) lên 2.192 MB (5M), tức độ dốc biên 400 MB mỗi triệu dòng xem thêm: RAM(N) ≈ 592 MB + (N − 1M) × 400 MB/M. Với giả định tuyến tính này
10 triệu dòng cần khoảng 4,2 GB (592 + 9 × 400 MB), 100 triệu dòng khoảng 40 GB, và 8 GB (8.192 MB, mức cấp cho Docker trong lần đo, `evaluation.md`) hết ở khoảng 20 triệu dòng (1M + (8.192 − 592)/400 triệu). Cùng công thức được dùng trong `evaluation.md` mục 3.1. Spark chỉ được đo đến 5M dòng;
RAM của nó bị chặn bởi heap 4 GB. Khả năng mở rộng ở các kịch bản 10 lần và 100 lần là **suy luận từ hai điểm đo, chưa được đo**.

### 3.2 Velocity (tốc độ)

| Đường xử lý | Nhịp / tốc độ | Nguồn |
|---|---|---|
| Batch | Chạy khi khởi động stack, bấm "Refresh batch (Spark)" hoặc khi gửi form; **không có lịch tự động trong mã**. Một lượt đầy đủ 337,3 s với 1M dòng xem (`load_times.md`; 5-10 phút trên máy đang bận, `batch_runs`) | `load_times.md`, `app.py`, `compose.yaml` |
| Stream, cửa sổ sản xuất | Cửa sổ tumbling 5 phút tính trên timestamp bản ghi Kafka (`TREND_WINDOW_MINUTES` mặc định 5), dashboard làm mới panel mỗi 10 s | `flink/flink_trending_sql_job.py`, `app.py` |
| Stream, tốc độ demo | 500 sự kiện/s; trong một cửa sổ 5 phút là 150.000 sự kiện (tính từ 500 × 300 s) | `src/kafka_event_producer.py` |
| Stream, đã đo cao nhất | Producer đạt 5.979 sự kiện/s (cấu hình 8.000); Flink SQL và Spark Streaming đều xử lý đúng mọi cửa sổ ở tốc độ này | `stream_latency.md` |
| Stream, xử lý dồn (burst 100.000 sự kiện) | Flink SQL 7.297 sự kiện/s; Spark 3.849; PyFlink 330 | `stream_latency.md` |
| Độ trễ kết quả ở 500 sự kiện/s (cửa sổ đo 10 s) | Flink SQL p50 0,53 s, p95 0,77 s; Spark 2,65 / 2,76 s; PyFlink 20,68 / 26,09 s | `stream_latency.md` |

Hai lưu ý để so sánh đúng. Một, độ trễ trong `stream_latency.md` đo với cửa sổ 10 s để chạy được nhiều kịch bản; cửa sổ chạy thật của dashboard là 5 phút.
Hai, **tốc độ tự nhiên của dữ liệu thấp hơn rất nhiều tốc độ demo**: 1.000.000 lượt xem trong 180 ngày là 5.556 sự kiện/ngày trung vị (`data_profile.md`),
trung bình khoảng 0,064 sự kiện/s (tính từ 1.000.000 / (180 × 86.400)). Con số 500 sự kiện/s là tải thử áp lực cho mô-đun stream, không phải tải thực của bộ dữ liệu.

### 3.3 Variety (đa dạng)

| Định dạng | Nơi dùng | Đặc điểm |
|---|---|---|
| CSV | 4 nguồn batch | Dạng bảng, có header, timestamp là chuỗi (mọi giá trị trong `data/` đều parse được; xem 4.3) |
| JSON | `watch-events`, `trending-results` | Bán cấu trúc, mỗi sự kiện một thông điệp; Flink đọc bằng `format = json` và bỏ qua thông điệp lỗi (`json.ignore-parse-errors`) |
| Parquet trên Hive | `raw.*`, `curated.*`, `analytics.*` | Dạng cột, nén, schema trong metastore; `curated.watch_events` phân vùng theo ngày |
| Cassandra wide rows | 28 bảng do batch ghi + 2 bảng live | Một partition chứa nhiều dòng sắp xếp sẵn theo clustering key (ví dụ `live_trending`: partition là ngày, sắp theo `window_start DESC, rank`) |

### 3.4 Veracity (độ tin cậy)

Bảy luật chất lượng nằm trong `src/data_quality.py`, dùng chung cho Pandas và Spark; mỗi dòng chỉ bị tính vào **luật đầu tiên** nó vi phạm
theo thứ tự dưới đây nên số dòng bị loại cộng lại đúng:

1. `duplicate_id`: trùng id (giữ dòng đầu); 2. `invalid_timestamp`: có chuỗi thời gian nhưng không parse được; 3. `null_value`: thiếu cột bắt buộc;
4. `invalid_value`: `watch_duration` ≤ 0, thời lượng phim ≤ 0 hoặc điểm ngoài 1-5; 5. `future_timestamp`: thời gian sau lúc chạy;
6. `orphan_user`; 7. `orphan_movie`: khoá ngoại không có trong bảng cha đã làm sạch (nếu không đếm thì các dòng này mất âm thầm ở phép join).

| Phép thử | Kết quả | Nguồn |
|---|---|---|
| Dữ liệu `data/` hiện tại | **0 dòng bị loại** ở cả 7 luật, cả 4 bảng (input = output: 50.000 / 20.000 / 500.000 / 1.000.000) | `data_quality_real.txt` |
| Hồ sơ dữ liệu | 0% null ở mọi cột, 0 id trùng, 0 mồ côi, 0 timestamp không parse được; điểm 1-5; `watch_duration` nhỏ nhất 5, thời lượng phim nhỏ nhất 80 | `data_profile.md` |
| Dữ liệu bẩn có chủ đích (`--dirty-rate 0.05`, 5.000 user / 2.000 phim / 50.000 đánh giá / 100.000 lượt xem) | `ratings`: 52.499 dòng vào, mỗi luật loại 357 dòng, còn 50.000; `watch_history`: 104.998 vào, mỗi luật loại 714, còn 100.000; `users`, `movies`: 0 | `data_quality_demo.txt` |

**Nói thẳng về ý nghĩa:** dữ liệu sinh ra sạch ngay từ đầu nên 0 dòng bị loại là điều dự kiến, không chứng minh dữ liệu thật sẽ sạch.
Khả năng loại đúng được chứng minh bằng dữ liệu bẩn có chủ đích, nơi số dòng bị loại bằng đúng số dòng đã chèn (357 × 7 = 2.499 = 52.499 − 50.000 ở `ratings`,
và 714 × 7 = 4.998 = 104.998 − 100.000 ở `watch_history`). Bộ kiểm thử `tests/test_data_quality.py` kiểm tra điều này tự động.

### 3.5 Value (giá trị)

| BR | Dữ liệu dùng | Cột chính |
|---|---|---|
| BR1 | `movies`, `watch_history` | `title`, `genre`, `release_year`, `duration`, số lượt xem |
| BR2 | `watch_history`, `movies`; stream: `watch-events` | `movie_id`, số lượt xem toàn thời gian và trong hai cửa sổ 7 ngày; stream: `movie_id`, `user_id`, `title`, `genre`, event time |
| BR3 | `watch_history`, `movies`, `ratings` | `user_id`, `genre` (thể loại xem nhiều nhất), `rating` |
| BR4 | `ratings`, `movies` | `rating`, `movie_id` |
| BR5 | `watch_history`, `movies` | `genre`, `user_id` |
| BR6 | `watch_history`, `users` | `user_id`, `watch_duration`, `watch_timestamp`, `subscription_plan` |
| BR7 | `watch_history`, `ratings`, `movies` | `watch_duration`, `duration`, `rating` |
| BR8 | `watch_history`, `movies` | số lượt xem, `completion_rate`, `genre` (nhóm đồng đẳng) |
| BR9 | `watch_history`, `users` | số lượt xem mỗi user; `activity_profile` (chỉ để đối chiếu) |
| BR10 | `watch_history`, `movies` | `watch_timestamp`, `movie_id`, `genre` |

Chi tiết từng BR (bảng, truy vấn, mã) ở `docs/business_requirements.md`.

## 4. Điểm nổi bật từ hồ sơ dữ liệu

Toàn bộ số liệu trong mục này lấy từ `data_profile.md`, trừ khi ghi khác.

### 4.1 Độ lệch (skew)

| Chỉ số | Giá trị |
|---|---|
| Phần lượt xem của 1% phim hàng đầu (200 phim) | 33,24% |
| Phần lượt xem của 10% phim hàng đầu (2.000 phim) | 53,61% |
| Gini của lượt xem theo phim | 0,5959 |
| Lượt xem mỗi phim p10 / p50 / p90 / p99 | 10 / 26 / 57 / 353 |
| Gini của lượt xem theo user | 0,4417 |
| Lượt xem mỗi user p10 / p50 / p90 / p99 | 4 / 12 / 45 / 53 |
| User có ≤ 10 lượt xem / ≤ 35 lượt xem | 42,18% / 71,86% |
| Thể loại Action | 28,61% lượt xem nhưng chỉ 12,53% danh mục (2.505/20.000 phim) |
| Thể loại Thriller | 4,80% lượt xem, 12,48% danh mục |

Lượt xem tập trung vào ít phim (1% phim chiếm một phần ba lượt xem) và vào một vài thể loại; theo phim lệch hơn theo user (Gini 0,5959 so với 0,4417).
Danh mục phân bố đều giữa 8 thể loại (từ 2.496 đến 2.505 phim mỗi thể loại) trong khi lượt xem thì không.

### 4.2 Quy luật thời gian

* Khoảng thời gian 180 ngày, 180 ngày đều có sự kiện, số sự kiện/ngày min / trung vị / max là 5.318 / 5.556,5 / 5.731: số lượng theo ngày gần như phẳng.
* Giờ trong ngày có bậc thang rõ rệt: giờ 20 có 116.720 lượt, giờ 01 có 7.780 lượt (gấp 15 lần, tính từ hai số này); năm giờ từ 17 đến 21 chiếm khoảng 56,1% (tính từ 9,52 + 11,66 + 11,63 + 11,67 + 11,66).
* Cuối tuần: 52 ngày cuối tuần, 28,93% sự kiện; trung bình mỗi ngày cuối tuần 5.564,1 sự kiện so với 5.552,1 ngày thường, tức **không có hiệu ứng cuối tuần ở mức ngày**; bộ sinh chỉ dịch giờ xem về buổi tối (18-23 giờ) vào cuối tuần.
* Đặc điểm này của bộ sinh có nghĩa là biểu đồ BR10 (xu hướng theo ngày, theo thể loại, trung bình trượt 7 ngày, ngày thường so với cuối tuần) gần như đường phẳng, ít kể được câu chuyện kinh doanh; danh sách "đang lên" của BR2 chủ yếu phản ánh dao động ngẫu nhiên giữa hai tuần.

### 4.3 Phân phối điểm đánh giá và độ phủ

* Điểm 1-5 lệch về phía cao: 0,30% điểm 1, 5,72% điểm 2, 22,87% điểm 3, 38,71% điểm 4, 32,41% điểm 5; trung bình 3,9721.
* **Ít đánh giá trên mỗi phim:** trung vị 13, p10 là 5, nhỏ nhất 1, lớn nhất 4.097; 21 phim chưa có đánh giá nào. Số đánh giá bằng 50,0% số lượt xem.
* Tỉ lệ hoàn thành (`watch_duration / duration`): trung bình 0,7399, trung vị 0,7471, tối đa 1,0; 16,86% lượt xem hoàn thành ≥ 90%.
* Tỉ lệ thiết bị gần như đều (Mobile 25,13%, Web 25,01%, TV 25,00%, Tablet 24,86%); gói Basic 45,27%, Standard 40,04%, Premium 14,69%.
* Định dạng thời gian: mọi `watch_timestamp` và `timestamp` trong `data/` đều là dạng ngây thơ (không có offset) và parse được (0 giá trị không parse được). Pipeline vẫn xử lý được chuỗi ISO có offset (ví dụ `2026-01-03T12:00:00+07:00`), được chứng minh bằng một dòng của bộ fixture trong `tests/conftest.py`, nhưng dữ liệu hiện tại **chưa có** định dạng lẫn lộn.

### 4.4 Cold start (người dùng chưa có lịch sử)

* 168 user (0,34%) không có lượt xem nào; 1.413 user chưa đánh giá phim nào; 21 phim chưa được đánh giá.
* Hệ quả ở BR3 (tính từ bảng `user_recommendations` bằng Pandas và đối chiếu với Cassandra ở `br_effectiveness.md`): 49.832 user (99,66%) có danh sách theo thể loại yêu thích; 168 user cold start nhận 10 phim xem nhiều nhất (1.680 dòng `popular`); ALS chỉ phục vụ 49.832 user có lịch sử.

## 5. Hệ quả cho thiết kế kỹ thuật dữ liệu

Mỗi dòng nêu phát hiện, quyết định thiết kế bị nó chi phối và nơi quyết định đó nằm trong mã.

| # | Phát hiện (nguồn) | Quyết định thiết kế | Vị trí trong mã / tài liệu |
|---|---|---|---|
| 1 | Lượt xem lệch: 1% phim chiếm 33,24%, Action chiếm 28,61% (`data_profile.md`) | Tính trước top-N và đặt trong bảng đã sắp xếp sẵn (`views DESC`) để đọc bằng `LIMIT`, không quét lúc đọc; khoá phân vùng theo `genre` hoặc `bucket` thay vì theo `movie_id` để một truy vấn chỉ chạm một partition | `sql/cassandra_schema.cql` (`movie_performance`, `top_viewed_movies`, `movies_by_genre_rating`) |
| 2 | Phân vùng theo thể loại chia đều theo số phim (2.496-2.505 dòng mỗi partition) nhưng tải đọc lệch về Action (28,61% lượt xem) (`data_profile.md`) | Chấp nhận nguy cơ nóng một partition ở quy mô này vì partition nhỏ (2.505 dòng; BR3 giờ đọc bảng `user_recommendations`, còn `movies_by_genre_rating` phục vụ bộ lọc thể loại của BR4) và dashboard cache truy vấn 60 s; ở quy mô lớn cần tách bucket theo thể loại/tháng (đã nêu trong ADR) | `app.py` (`cql`, `ttl=60`), `docs/storage_decision.md` mục 5 |
| 3 | Partition `bucket='all'` chứa ≈ 20.000 dòng (bằng số phim, `data_profile.md`) | Tin vào việc dữ liệu hiện nhỏ; đây là điểm sẽ phải đổi khi danh mục lớn lên (chia bucket hoặc chỉ giữ top-K) | `docs/storage_decision.md` mục 5 |
| 4 | Dữ liệu trải 180 ngày liên tục, mỗi ngày ≈ 5,5 nghìn sự kiện (`data_profile.md`) | `curated.watch_events` phân vùng theo `watch_date` để truy vấn theo khoảng ngày chỉ đọc các partition cần; truy vấn ad hoc một tuần chạy 485 ms ở p50 (`storage_latency.md`) | `src/spark_batch_job.py` (`run`, `partition_by="watch_date"`), `src/dashboard_queries.py` (`ADHOC_SQL`) |
| 5 | Sự kiện stream có timestamp riêng; giờ cao điểm 17-21 giờ chiếm ≈ 56,1% (`data_profile.md`) | Cửa sổ tumbling 5 phút gắn với timestamp bản ghi Kafka do producer đặt lúc gửi (thực chất gần với ingestion time) và watermark độ trễ 0 s, nên dữ liệu đến muộn chưa được xử lý; event time thật từ `source_timestamp` với độ lệch thứ tự có giới hạn là cải tiến dự kiến (task #19); bảng `live_trending` phân vùng theo ngày, sắp `window_start DESC` để `LIMIT 10` trả cửa sổ mới nhất | `flink/flink_trending_sql_job.py`, `sql/cassandra_schema.cql` (`live_trending`) |
| 6 | Nguồn có thể trộn định dạng thời gian (naive và ISO có offset); fixture có cả hai (`tests/conftest.py`) | Giữ timestamp là chuỗi ở vùng raw và parse sang UTC khi làm sạch; session Spark đặt múi giờ UTC | `src/spark_batch_job.py` (`RAW_SCHEMAS`, `curate`, `create_spark`), `src/batch_processing.py` (`parse_timestamps`) |
| 7 | Ít đánh giá mỗi phim: trung vị 13, nhỏ nhất 1 (`data_profile.md`) | Xếp hạng bằng điểm trung bình có trọng số kiểu Bayesian (kéo về trung bình toàn cục, trọng số trước m = trung vị số đánh giá) để phim có 1-2 đánh giá không đứng đầu; kết quả đo: phim top-10 có ít nhất 169 đánh giá (`br_effectiveness.md`) | `src/spark_batch_job.py` (`build_tables`, biến `min_votes`), `src/batch_processing.py` |
| 8 | 21 phim chưa có đánh giá (`data_profile.md`) | `weighted_rating` là null và loại khỏi các bảng xếp hạng đánh giá (`top_rated_movies`, `movies_by_genre_rating`) nhưng vẫn nằm trong `movie_performance` | `src/spark_batch_job.py` (`serving_frames`, biến `rated`) |
| 9 | 168 user (0,34%) chưa có lượt xem (`data_profile.md`) | Giữ mọi user ở `user_engagement` bằng phép join trái và đưa vào phân khúc Inactive; `user_recommendations` bù cho user không có sở thích (cold start) bằng 10 phim xem nhiều nhất (cột `source = 'popular'`), còn ALS không có dòng nào cho họ và dashboard chuyển về danh sách theo thể loại | `src/spark_batch_job.py` (`build_tables`), `src/recommendations.py` (`recommend_spark`), `app.py` (`viewer_page`) |
| 10 | Phân phối lượt xem mỗi user kéo dài: p50 12, p90 45; 42,18% user ≤ 10 lượt (`data_profile.md`) | Phân khúc theo phân vị 25 và 75 của lượt xem mỗi user (ngưỡng tính trong batch, lưu ở `segment_thresholds`), thay cho hai hằng số 10 và 35; ma trận đối chiếu với `activity_profile` đo mức khớp | `src/spark_batch_job.py` (`user_segment_tables`), `src/batch_processing.py` (`build_tables`), `src/business_rules.py` |
| 11 | Dữ liệu thật thường bẩn còn dữ liệu sinh ra sạch (0 dòng bị loại, `data_quality_real.txt`) | Tách bước `curated` có luật chất lượng đếm từng loại lỗi và lưu vào `curated.quality_report`; chứng minh luật bằng dữ liệu bẩn có chủ đích | `src/data_quality.py`, `generate_data.py` (`inject_dirty_rows`) |
| 12 | Truy vấn theo khoá cần 1-2 ms trên Cassandra nhưng 240-650 ms trên Spark SQL; quét 50.000 dòng thì hoà (382 ms so với 383 ms) (`storage_latency.md`) | Dùng hai lớp: Cassandra phục vụ truy vấn theo khoá/top-N, Hive/Parquet cho lưu trữ lịch sử, tính lại và truy vấn ad hoc | `docs/storage_decision.md` |
| 13 | Parquet nhỏ hơn CSV khoảng 2,6 lần (74,3 MB so với 28,8 MB, tính từ `storage_footprint.md`) | Lưu vùng raw/curated dạng Parquet cột nén thay vì giữ CSV | `src/spark_batch_job.py` (`_save_hive`) |
| 14 | Pandas nhanh hơn Spark ở ≤ 5M dòng nhưng RAM tăng tuyến tính (592 → 2.192 MB, `batch_engines.md`) | Giữ cả hai pipeline cho cùng một đầu ra (Pandas là bản đối chứng nhỏ), dùng Spark làm đường chính vì nó ghi Hive và Cassandra và RAM của nó bị chặn bởi heap (suy luận từ phép đo tới 5M dòng, chưa đo vượt RAM) | `src/batch_processing.py`, `src/spark_batch_job.py`, `tests/test_parity.py` |
