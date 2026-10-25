# Bối cảnh nghiệp vụ, nhóm người dùng và yêu cầu nghiệp vụ (mục 1 của báo cáo kỹ thuật)

Tài liệu này trả lời ba câu hỏi: nền tảng xem phim cần gì từ dữ liệu, ai dùng kết quả, và với từng yêu cầu nghiệp vụ (BR1-BR10) thì giải pháp
kỹ thuật dữ liệu được chọn vì lý do gì. Số liệu được ghi nguồn ngay cạnh số; mọi nguồn nằm trong `benchmarks/results/`
(`br_effectiveness.md`, `data_profile.md`, `storage_latency.md`, `stream_latency.md`, `load_times.md`, `data_quality_real.txt`, `data_quality_demo.txt`).
Nguồn và đặc điểm của dữ liệu nằm ở `docs/data_characteristics.md`; lý do chọn Cassandra và Hive/Parquet ở `docs/storage_decision.md`;
đánh giá công nghệ ở `docs/evaluation.md`.

Dữ liệu là dữ liệu tổng hợp (sinh bởi `generate_data.py`, seed 42), nên kết quả hiển thị phản ánh bộ sinh dữ liệu, không phải hành vi người xem thật.

## 1. Bối cảnh nghiệp vụ

Một nền tảng xem phim theo yêu cầu (streaming) sống bằng thuê bao: doanh thu đến từ việc người dùng đăng ký và tiếp tục sử dụng, còn chi phí lớn
nằm ở việc mua hoặc sản xuất nội dung. Ba mục tiêu kinh doanh được dùng xuyên suốt trong tài liệu này:

| Mã | Mục tiêu | Ý nghĩa |
|---|---|---|
| **O1** | Tăng mức độ tương tác (engagement) | Người dùng tìm được phim muốn xem nhanh và xem nhiều hơn |
| **O2** | Giữ chân người dùng (retention) | Nhận ra người dùng ít hoạt động, cá nhân hoá để họ quay lại |
| **O3** | Tối ưu đầu tư nội dung | Biết thể loại và tựa phim nào hiệu quả, tựa nào không đáng gia hạn bản quyền |

Vì sao cần kỹ thuật dữ liệu (data engineering): dữ liệu thô là sự kiện xem phim và đánh giá, sinh liên tục và rất nhiều so với bảng người dùng và danh mục
(1.000.000 lượt xem và 500.000 đánh giá so với 50.000 người dùng và 20.000 phim, `data_profile.md`). Để từ đó trả lời các câu hỏi như "phim nào đang hot",
"gợi ý gì cho người này" hay "người dùng nào sắp rời đi", dữ liệu phải được thu thập từ nhiều nguồn và định dạng (CSV, sự kiện JSON qua Kafka), làm sạch
(7 luật chất lượng, `src/data_quality.py`), tổng hợp trước theo từng câu hỏi, và phục vụ với độ trễ phù hợp từng nhóm: vài mili giây cho màn hình tương tác
(truy vấn theo khoá trên Cassandra mất 1-2 ms, trong khi cùng truy vấn trên Spark SQL mất 240-650 ms, `storage_latency.md`) và gần thời gian thực cho xu hướng đang diễn ra.
Mỗi nhóm người dùng cần một tốc độ làm mới khác nhau nên hệ thống kết hợp xử lý theo lô (Spark) và xử lý luồng (Kafka + Flink).

## 2. Các bên liên quan (nhóm người dùng)

Mỗi nhóm có sáu mục: vai trò, mục tiêu, quyết định, nhu cầu dữ liệu, độ tươi cần thiết và trang dashboard. "Độ tươi cần thiết" là yêu cầu mà nhóm đặt ra cho thiết kế
(không phải số đo); "hiện thực" là điều mã hiện làm.

### 2.1 Viewer (người xem)

| Mục | Nội dung |
|---|---|
| Vai trò | Người dùng cuối của nền tảng, chọn phim để xem |
| Mục tiêu | Tìm đúng phim nhanh, biết phim nào đang được xem nhiều, nhận gợi ý hợp gu |
| Quyết định | Xem phim nào tiếp theo |
| Nhu cầu dữ liệu | Danh mục phim (tên, thể loại, năm, thời lượng, lượt xem, điểm trung bình); xếp hạng phổ biến; thể loại ưa thích của chính mình; xu hướng 5 phút gần nhất |
| Độ tươi cần thiết | Xu hướng: gần thời gian thực (cửa sổ 5 phút, panel làm mới mỗi 10 s). Danh mục, gợi ý: theo ngày là đủ. Hiện thực: batch chạy khi khởi động hoặc theo yêu cầu (mất 249,6 s cho 1M lượt xem, `load_times.md`) |
| Trang dashboard | `Viewer` (BR1, BR2, BR3, kèm panel live của BR2) |

### 2.2 Analyst (chuyên viên phân tích)

| Mục | Nội dung |
|---|---|
| Vai trò | Phân tích hiệu quả nội dung và hành vi người dùng |
| Mục tiêu | Hiểu phim nào được đánh giá cao đáng tin, thể loại nào được xem nhiều, người dùng tương tác ra sao, tựa nào hiệu quả |
| Quyết định | Đề xuất mua thêm thể loại nào, nên quảng bá tựa nào, giờ nào phát nội dung |
| Nhu cầu dữ liệu | Điểm đánh giá có xét số lượng đánh giá; lượt xem và người xem duy nhất theo thể loại; phân phối lượt xem theo người dùng, gói thuê bao, giờ trong ngày; lượt xem, điểm, tỉ lệ hoàn thành và giờ xem của từng phim |
| Độ tươi cần thiết | Theo ngày là đủ (phân tích, không ra quyết định theo phút). Hiện thực: batch theo yêu cầu |
| Trang dashboard | `Analyst` (BR4 đến BR7) |

### 2.3 Manager (quản lý)

| Mục | Nội dung |
|---|---|
| Vai trò | Quản lý nội dung và tăng trưởng người dùng |
| Mục tiêu | Phát hiện nội dung kém hiệu quả, biết cơ cấu người dùng theo mức hoạt động, theo dõi xu hướng theo thời gian |
| Quyết định | Gỡ hoặc không gia hạn tựa ít xem, chạy chiến dịch kéo người dùng Inactive quay lại |
| Nhu cầu dữ liệu | Danh sách phim ít xem nhất; số người dùng theo phân khúc Inactive / Casual / Active; chuỗi lượt xem theo ngày |
| Độ tươi cần thiết | Theo ngày hoặc theo tuần. Hiện thực: batch theo yêu cầu |
| Trang dashboard | `Manager` (BR8, BR9, BR10) |

### 2.4 Đội Kỹ thuật dữ liệu / Nền tảng (vận hành đường ống)

| Mục | Nội dung |
|---|---|
| Vai trò | Vận hành đường ống dữ liệu: batch Spark, luồng Kafka/Flink, Cassandra, Hive |
| Mục tiêu | Dữ liệu đúng, làm mới đúng hạn, hệ thống chạy lặp lại được và có thể đo |
| Quyết định | Khi nào chạy lại batch, luật chất lượng nào thêm, có mở rộng cụm hay đổi thiết kế partition không, chọn Flink SQL hay Spark Streaming |
| Nhu cầu dữ liệu | Báo cáo dòng bị loại theo luật (`curated.quality_report`, `data_quality_real.txt`); thời gian từng giai đoạn batch (`load_times.md`); độ trễ stream (`stream_latency.md`); số dòng Cassandra so với đầu ra batch |
| Độ tươi cần thiết | Gần thời gian thực về trạng thái pipeline (batch đang chạy, job Flink còn sống) |
| Trang dashboard | Không có trang riêng. Thanh bên của dashboard có nút "Refresh batch (Spark)" và trạng thái batch; Flink có giao diện riêng ở cổng 8081; báo cáo chất lượng in ra log Spark và lưu ở `curated.quality_report` |

## 3. Ma trận nhóm người dùng × BR

● = nhóm dùng trực tiếp ở dashboard; ○ = nhóm vận hành và giám sát đường xử lý của BR đó (không xem màn hình BR).

| BR | Yêu cầu | Viewer | Analyst | Manager | Kỹ thuật dữ liệu | Mục tiêu |
|---|---|---|---|---|---|---|
| BR1 | Tìm kiếm và lọc phim | ● | | | ○ | O1 |
| BR2 | Phát hiện phim thịnh hành | ● | | | ○ | O1 |
| BR3 | Gợi ý cá nhân hoá | ● | | | ○ | O1, O2 |
| BR4 | Phân tích đánh giá phim | | ● | | ○ | O3 |
| BR5 | Phân tích độ phổ biến thể loại | | ● | | ○ | O3 |
| BR6 | Phân tích tương tác người dùng | | ● | | ○ | O1, O2 |
| BR7 | Phân tích hiệu quả phim | | ● | | ○ | O3 |
| BR8 | Phát hiện phim ít tương tác | | | ● | ○ | O3 |
| BR9 | Phân khúc người dùng | | | ● | ○ | O2 |
| BR10 | Xu hướng theo thời gian | | | ● | ○ | O1, O3 |

Mười BR đáp ứng yêu cầu của đề bài là ít nhất 2×n yêu cầu nghiệp vụ cho một nhóm n thành viên, với n ≤ 5.

## 4. Chi tiết từng yêu cầu nghiệp vụ

Quy ước chung: bảng phục vụ nằm trong keyspace `movie_analytics` của Cassandra (`sql/cassandra_schema.cql`). Mọi bảng batch do Spark tính (`src/spark_batch_job.py`,
`build_tables` rồi `serving_frames`, ghi bằng `write_cassandra`); bản Pandas tương đương ở `src/batch_processing.py` (`build_tables`) cho cùng đầu ra. Truy vấn dashboard nằm trong `app.py`
(bản CQL/SQL để so sánh ở `src/dashboard_queries.py`). Cột "kết quả" lấy từ `br_effectiveness.md`; thời gian truy vấn ở đó đo trên máy đang chạy cả stack nên dao động, thời gian khi máy rảnh lấy từ `storage_latency.md`.

### BR1 - Tìm kiếm và lọc phim

* **Nhóm người dùng:** Viewer.
* **Câu hỏi nghiệp vụ:** Tôi tìm phim theo tên hoặc thể loại thế nào?
* **Mục tiêu kinh doanh hỗ trợ:** O1 (giảm thời gian tìm phim, tăng lượt xem).
* **Dữ liệu dùng:** `movies` (`title`, `genre`, `release_year`, `duration`) và `watch_history` (đếm lượt xem), thêm `ratings` (điểm trung bình) và `watch_duration` (tỉ lệ hoàn thành).
* **Đường xử lý:** batch Spark. `src/spark_batch_job.py`, hàm `build_tables` (bảng `movie_performance`), `serving_frames`.
* **Bảng phục vụ và truy vấn:** `movie_performance`, khoá phân vùng `genre`, clustering `views DESC, movie_id`. Truy vấn: `SELECT movie_id, title, genre, release_year, duration, views, avg_rating, completion_rate FROM movie_performance WHERE genre = ?`; chọn "All" thì đọc lần lượt 8 partition (`app.py`, hàm `catalog_for`).
* **Màn hình hiển thị:** `Viewer › BR1`: ô nhập tên, chọn thể loại, bảng phim sắp theo lượt xem giảm dần. Việc lọc theo tên phim chạy trong ứng dụng bằng `str.contains`, không ở Cassandra.
* **Lý do chọn giải pháp:** *Vì sao batch:* danh mục và lượt xem lũy kế chỉ cần cập nhật theo ngày; không có yêu cầu thời gian thực. *Vì sao thiết kế bảng này:* phần lớn lượt đọc là "danh mục của một thể loại", nên đặt `genre` làm khoá phân vùng để một truy vấn chạm một partition (2.496-2.505 phim mỗi thể loại, `data_profile.md`) và sắp sẵn theo lượt xem để không phải sắp xếp lúc đọc. *Vì sao lọc tên ở ứng dụng:* Cassandra không có `LIKE`/tìm chuỗi con (`docs/storage_decision.md` mục 5). *Phương án đã cân nhắc:* chỉ mục SAI của Cassandra 5 để tìm chuỗi con (nêu trong ADR, chưa dùng); tìm kiếm toàn văn bằng Elasticsearch (chưa thử, vượt phạm vi); đọc thẳng Hive/Parquet bằng Spark SQL (chậm: 656 ms so với 20 ms p50, `storage_latency.md`).
* **Tiêu chí thành công và kết quả đo:** duyệt toàn bộ trả đủ phim: **20.000/20.000 phim, đạt**; tìm "spider" thấy các tựa Spider-Man: **2 tựa, đạt** (`br_effectiveness.md`). Độ trễ p50 của truy vấn một thể loại là 20,15 ms (p95 30,1 ms) trên Cassandra (`storage_latency.md`).
* **Giới hạn hiện tại:** tìm kiếm là khớp chuỗi con theo tên, không có sửa lỗi chính tả, không tìm theo diễn viên hay mô tả (dữ liệu không có các trường đó); chọn "All" phải đọc 8 partition rồi sắp lại ở ứng dụng; lượt xem lũy kế là toàn thời gian.

### BR2 - Phát hiện phim thịnh hành

* **Nhóm người dùng:** Viewer.
* **Câu hỏi nghiệp vụ:** Phim nào đang được xem nhiều nhất, tổng thể và trong 5 phút vừa qua?
* **Mục tiêu kinh doanh hỗ trợ:** O1 (đưa phim đang hot lên trước người xem).
* **Dữ liệu dùng:** batch: `watch_history` ⋈ `movies` (`movie_id`, số lượt xem). Stream: sự kiện `watch-events` (`movie_id`, `user_id`, `title`, `genre`, event time).
* **Đường xử lý:** *Batch:* Spark, `build_tables` (`movie_performance`) rồi `serving_frames` (`top_viewed_movies`). *Stream:* Kafka, Flink SQL job `flink/flink_trending_sql_job.py` (hàm `main`, cửa sổ `TUMBLE`), topic `trending-results`, rồi `src/trending_materializer.py` (`consume_results`, `rank_movies`, `write_snapshot`). Nguồn demo: `src/kafka_event_producer.py` (`send_events`).
* **Bảng phục vụ và truy vấn:** batch `top_viewed_movies`, khoá phân vùng `bucket` (hằng `'all'`), clustering `views DESC, movie_id`: `SELECT title, genre, views, avg_rating FROM top_viewed_movies WHERE bucket = 'all' LIMIT 10`. Live: `live_trending`, khoá phân vùng `day`, clustering `window_start DESC, rank`, TTL 3 ngày: `SELECT window_start, rank, title, genre, live_views, unique_viewers FROM live_trending WHERE day = ? LIMIT 10`.
* **Màn hình hiển thị:** `Viewer › BR2`: bảng top 10 lượt xem toàn thời gian, và panel "Latest 5-minute event window" làm mới mỗi 10 s (hàm `render_live_trending`), hiện "waiting" nếu kết quả cũ hơn 10 phút.
* **Lý do chọn giải pháp:** *Vì sao có cả batch và stream:* "thịnh hành" có hai nghĩa: toàn thời gian (tính theo lô, rẻ) và đang diễn ra (cần cửa sổ ngắn, nên dùng stream). *Vì sao Flink SQL:* cửa sổ chạy trong JVM; "event time" hiện là timestamp bản ghi Kafka do producer đặt lúc gửi (thực chất gần với ingestion time) với watermark độ trễ 0 s, dữ liệu đến muộn chưa được xử lý, còn event time thật từ `source_timestamp` với độ lệch thứ tự có giới hạn là cải tiến dự kiến (task #19); độ trễ p50 0,53 s và p95 0,77 s ở 500 sự kiện/s, đúng ở mọi cửa sổ; job PyFlink DataStream trước đó chỉ chịu ~330-500 sự kiện/s (độ trễ 20,68 s ở 500 sự kiện/s) nên không theo kịp producer demo (`stream_latency.md`). *Vì sao bảng stream thiết kế vậy:* mỗi cửa sổ ghi 10 dòng, `LIMIT 10` trên clustering `window_start DESC` trả ngay cửa sổ mới nhất; TTL tự xoá dữ liệu cũ; ghi lại cùng khoá là idempotent. *Phương án đã cân nhắc:* Spark Structured Streaming (độ trễ ổn định 2,65-2,96 s, vận hành đơn giản hơn, không cần materializer; có sẵn trong `src/spark_streaming_trending.py`, ghi vào `live_trending_spark`); PyFlink DataStream (giữ để so sánh).
* **Tiêu chí thành công và kết quả đo:** top 10 toàn thời gian bằng xếp hạng Pandas độc lập: **True, đạt**; cửa sổ live có rank 1..10 và mới hơn 10 phút: **10 dòng, cửa sổ mới nhất cũ 8,1 phút, đạt** (`br_effectiveness.md`). Độ đúng của top-10 mỗi cửa sổ ở Flink SQL là 3/3 đến 5/5 cửa sổ ở mọi tải, 7.297 sự kiện/s khi xử lý dồn (`stream_latency.md`).
* **Giới hạn hiện tại:** *batch BR2 là tổng lượt xem toàn thời gian, không phải tăng trưởng gần đây*: một phim hot từ lâu luôn đứng đầu. Cửa sổ dùng timestamp bản ghi Kafka (gần với ingestion time, watermark độ trễ 0 s) nên dữ liệu đến muộn không được xử lý; event time thật từ `source_timestamp` là cải tiến dự kiến (task #19). Nguồn stream là dữ liệu lịch sử được phát lại với timestamp hiện tại (không phải lưu lượng thật). Mỗi lần khởi động producer phát lại từ đầu file; bảng live chỉ giữ top 10 mỗi cửa sổ và TTL 3 ngày; một broker, một partition, parallelism 1 (`docs/evaluation.md` mục 7).

### BR3 - Gợi ý phim cá nhân hoá

* **Nhóm người dùng:** Viewer.
* **Câu hỏi nghiệp vụ:** Tôi nên xem phim gì tiếp theo?
* **Mục tiêu kinh doanh hỗ trợ:** O1 và O2 (cá nhân hoá để người dùng quay lại).
* **Dữ liệu dùng:** `watch_history` ⋈ `movies` (thể loại mỗi lượt xem của user), `ratings` (điểm trung bình có trọng số của phim).
* **Đường xử lý:** batch Spark. `build_tables`: `user_preferences` (đếm lượt xem theo (user, genre), `row_number` lấy 2 thể loại xem nhiều nhất) và `rating_stats` (điểm Bayesian); `serving_frames` tạo `movies_by_genre_rating`.
* **Bảng phục vụ và truy vấn:** hai bước: `SELECT favorite_genres FROM user_preferences WHERE user_id = ?` (khoá phân vùng `user_id`), rồi cho mỗi thể loại `SELECT title, genre, weighted_rating, avg_rating, rating_count, views FROM movies_by_genre_rating WHERE genre = ? LIMIT 8` (khoá phân vùng `genre`, clustering `weighted_rating DESC`); ứng dụng lấy 8 phim điểm cao nhất của hai thể loại (`app.py`).
* **Màn hình hiển thị:** `Viewer › BR3`: chọn user, bảng 8 phim đề xuất với điểm có trọng số, điểm trung bình, số đánh giá, lượt xem. User không có lịch sử thì hiện lời nhắn "No viewing history yet" và 8 phim phổ biến.
* **Lý do chọn giải pháp:** *Vì sao batch:* sở thích của một người thay đổi chậm, tính theo lô là đủ. *Vì sao hai bước:* Cassandra không có join, nên tách thành một tra cứu theo user và một truy vấn theo thể loại, đều là truy vấn theo khoá một partition (1,2-1,4 ms mỗi bước, `storage_latency.md`). *Vì sao dùng điểm Bayesian:* mỗi phim có ít đánh giá (trung vị 13, nhỏ nhất 1, `data_profile.md`), điểm trung bình thuần của phim 1-2 đánh giá sẽ lên đầu bảng; điểm có trọng số `(n·avg + m·C)/(n + m)` kéo về trung bình toàn cục C với m là trung vị số đánh giá (`src/spark_batch_job.py`, biến `min_votes`). *Vì sao hai thể loại:* quy ước đơn giản, vẫn đa dạng hơn một thể loại. *Phương án đã cân nhắc:* lọc cộng tác (collaborative filtering, ví dụ ALS của Spark MLlib) cho gợi ý cá nhân sâu hơn (chưa làm: cần ma trận user-item thưa, phức tạp hơn nhiều so với mục tiêu của đồ án); chỉ sắp theo điểm trung bình thuần (bị thiên lệch bởi số đánh giá ít).
* **Tiêu chí thành công và kết quả đo:** gợi ý nằm trong 2 thể loại hàng đầu của user (200 user ngẫu nhiên): **199/199 user có lịch sử đạt**; tỉ lệ user nhận gợi ý cá nhân hoá: **99,66%**, phần còn lại là cold start (168 user không có lượt xem, 0,34%) được hiển thị phim phổ biến (`br_effectiveness.md`, `data_profile.md`).
* **Giới hạn hiện tại:** *gợi ý không loại các phim người dùng đã xem rồi*; chỉ dựa trên số lượt xem theo thể loại và điểm đánh giá chung, không dùng hành vi người dùng tương tự; thể loại ngang bằng số lượt xem sắp theo tên thể loại; không đánh giá chất lượng gợi ý bằng độ chính xác dự đoán (chưa có dữ liệu thật để kiểm chứng), chỉ kiểm tra gợi ý nằm đúng thể loại.

### BR4 - Phân tích đánh giá phim

* **Nhóm người dùng:** Analyst.
* **Câu hỏi nghiệp vụ:** Phim nào được đánh giá cao một cách đáng tin cậy?
* **Mục tiêu kinh doanh hỗ trợ:** O3 (nhận ra tựa chất lượng để đầu tư tiếp).
* **Dữ liệu dùng:** `ratings` (`movie_id`, `rating`) ⋈ `movies` (tiêu đề, thể loại).
* **Đường xử lý:** batch Spark. `build_tables` (`rating_stats`, điểm trung bình thuần, số đánh giá và điểm Bayesian, rồi gộp vào `movie_performance`); `serving_frames` lọc phim có điểm (`rated`) tạo `top_rated_movies`.
* **Bảng phục vụ và truy vấn:** `top_rated_movies`, khoá phân vùng `bucket`, clustering `weighted_rating DESC, rating_count DESC, movie_id`: `SELECT title, genre, weighted_rating, avg_rating, rating_count FROM top_rated_movies WHERE bucket = 'all' LIMIT 10`. Chỉ số tổng `summary_metrics` cung cấp điểm trung bình toàn cục.
* **Màn hình hiển thị:** `Analyst › BR4`: bảng top 10 theo điểm có trọng số cùng điểm trung bình và số đánh giá; ô chỉ số "Average rating" ở đầu trang.
* **Lý do chọn giải pháp:** *Vì sao batch:* điểm đánh giá là chỉ số tích luỹ chậm. *Vì sao chỉ số Bayesian:* như BR3, mỗi phim có ít đánh giá (trung vị 13, `data_profile.md`) và 21 phim chưa có đánh giá nên điểm trung bình thuần sẽ cho phim 1 đánh giá 5 sao đứng đầu; chỉ số Bayesian đảm bảo mỗi tựa trong top phải dựa trên đủ đánh giá. *Vì sao một partition `bucket`:* danh sách xếp hạng toàn cục chỉ cần `LIMIT 10`, đã sắp sẵn. *Phương án đã cân nhắc:* đặt ngưỡng cứng "chỉ phim có ≥ N đánh giá" (đơn giản, nhưng bỏ hẳn phim dưới ngưỡng và ngưỡng N chọn tuỳ ý); khoảng tin cậy Wilson (phù hợp hơn cho điểm nhị phân, không áp trực tiếp cho thang 1-5).
* **Tiêu chí thành công và kết quả đo:** top 10 theo điểm có trọng số bằng xếp hạng Pandas độc lập: **True, đạt**; không tựa nào trong top 10 dựa trên quá ít đánh giá: **số đánh giá nhỏ nhất trong top 10 là 169, đạt** (`br_effectiveness.md`).
* **Giới hạn hiện tại:** trọng số trước m là trung vị số đánh giá (13) được tính từ dữ liệu hiện có, không được tinh chỉnh bằng thực nghiệm; điểm Bayesian che đi sự khác biệt giữa phim ít đánh giá và phim bình thường; chưa xét độ tươi (đánh giá cũ cùng trọng số đánh giá mới).

### BR5 - Phân tích độ phổ biến thể loại

* **Nhóm người dùng:** Analyst.
* **Câu hỏi nghiệp vụ:** Thể loại nào được xem nhiều nhất và bởi bao nhiêu người?
* **Mục tiêu kinh doanh hỗ trợ:** O3 (quyết định đầu tư vào thể loại nào).
* **Dữ liệu dùng:** `watch_history` ⋈ `movies` (`genre`, `user_id`).
* **Đường xử lý:** batch Spark. `build_tables`, bảng `genre_popularity` (`count` lượt xem và `countDistinct` người xem theo thể loại).
* **Bảng phục vụ và truy vấn:** `genre_popularity`, khoá chính `genre`: `SELECT genre, views, unique_viewers FROM genre_popularity` (8 dòng).
* **Màn hình hiển thị:** `Analyst › BR5`: biểu đồ cột lượt xem theo thể loại. Số người xem duy nhất (`unique_viewers`) có trong bảng phục vụ nhưng biểu đồ chỉ vẽ lượt xem.
* **Lý do chọn giải pháp:** *Vì sao batch:* tổng hợp toàn bộ lịch sử, dữ liệu nhỏ sau tổng hợp. *Vì sao bảng 8 dòng:* tính trước nên đọc chỉ 1-2 ms (1,87 ms p50, `storage_latency.md`) và Cassandra không có tổng hợp tuỳ ý lúc đọc nên số người xem duy nhất phải được tính trước (`docs/storage_decision.md` mục 5). *Phương án đã cân nhắc:* tính trực tiếp từ Parquet bằng Spark SQL (435,9 ms p50 so với 1,9 ms, `storage_latency.md`).
* **Tiêu chí thành công và kết quả đo:** tổng lượt xem các thể loại bằng tổng lượt xem: **1.000.000 so với 1.000.000, đạt** (`br_effectiveness.md`). Hồ sơ dữ liệu: Action 28,61% lượt xem, Thriller 4,80% (`data_profile.md`).
* **Giới hạn hiện tại:** chỉ tổng toàn thời gian, không theo thời gian (không biết thể loại nào đang lên); mỗi phim có đúng một thể loại nên không phản ánh phim đa thể loại; biểu đồ chỉ hiển thị lượt xem, không hiển thị người xem duy nhất.

### BR6 - Phân tích tương tác người dùng

* **Nhóm người dùng:** Analyst.
* **Câu hỏi nghiệp vụ:** Người dùng tương tác ra sao: số lượt xem, giờ xem, theo gói thuê bao?
* **Mục tiêu kinh doanh hỗ trợ:** O1 và O2 (hiểu mức độ gắn bó và khung giờ cao điểm).
* **Dữ liệu dùng:** `watch_history` ⋈ `users` (`user_id`, `watch_duration`, `watch_timestamp`, `subscription_plan`, `region`).
* **Đường xử lý:** batch Spark. `build_tables`: `user_engagement` (lượt xem, tổng phút xem, số ngày hoạt động mỗi user), `subscription_views`, `hourly_views`.
* **Bảng phục vụ và truy vấn:** `user_engagement` (khoá `user_id`): `SELECT views FROM user_engagement` (quét toàn bảng); `subscription_views` (khoá `subscription_plan`): `SELECT subscription_plan, views FROM subscription_views`; `hourly_views` (khoá `hour`): `SELECT hour, views FROM hourly_views`.
* **Màn hình hiển thị:** `Analyst › BR6`: biểu đồ phân phối lượt xem theo người dùng, biểu đồ tròn lượt xem theo gói thuê bao, biểu đồ cột lượt xem theo giờ trong ngày. Bảng `user_engagement` còn có số phút xem và số ngày hoạt động nhưng dashboard chưa hiển thị hai cột đó.
* **Lý do chọn giải pháp:** *Vì sao batch:* mức tương tác là thống kê tích luỹ. *Vì sao ba bảng:* mỗi biểu đồ một truy vấn đơn giản; hai bảng nhỏ (3 và 24 dòng) đọc 1,4 ms, còn phân phối lượt xem cần quét 50.000 dòng người dùng. *Vì sao chấp nhận quét:* quét 50.000 dòng trên Cassandra mất 381,6 ms p50 và trên Hive/Parquet mất 383,5 ms: hoà (`storage_latency.md`), nên không có lý do dựng cấu trúc riêng. *Phương án đã cân nhắc:* tính sẵn histogram thành bảng bin (nêu trong ADR, chưa làm); đọc trực tiếp từ Parquet.
* **Tiêu chí thành công và kết quả đo:** phân phối bao đủ người dùng và lượt xem cộng đúng: **50.000 user, 1.000.000 lượt xem, đạt** (`br_effectiveness.md`); thời gian truy vấn 3.795,5 ms khi máy đang chạy cả stack streaming (dao động, `docs/evaluation.md` mục 4), 381,6 ms p50 khi máy rảnh (`storage_latency.md`). Giờ cao điểm: giờ 20 có 116.720 lượt xem, giờ 01 có 7.780 (`data_profile.md`).
* **Giới hạn hiện tại:** các cột `watch_minutes` và `active_days` đã tính nhưng chưa hiển thị; chưa lọc theo vùng hay gói; phân phối chỉ có lượt xem, không có thời gian xem; thời gian truy vấn của màn hình này là chậm nhất trong các màn hình vì quét 50.000 dòng.

### BR7 - Phân tích hiệu quả phim

* **Nhóm người dùng:** Analyst.
* **Câu hỏi nghiệp vụ:** Phim nào hiệu quả, xét cả lượt xem, điểm, tỉ lệ hoàn thành và giờ xem?
* **Mục tiêu kinh doanh hỗ trợ:** O3 (đánh giá tựa hiệu quả trước khi gia hạn hoặc mua thêm).
* **Dữ liệu dùng:** `watch_history` (`watch_duration`), `movies` (`duration`), `ratings` (điểm trung bình).
* **Đường xử lý:** batch Spark. `build_tables`, bảng `movie_performance` (cột `views`, `unique_viewers`, `avg_rating`, `watch_hours`, `completion_rate`); `serving_frames` tạo `top_viewed_movies`.
* **Bảng phục vụ và truy vấn:** `top_viewed_movies`, khoá phân vùng `bucket`: `SELECT title, genre, views, avg_rating, completion_rate, watch_hours FROM top_viewed_movies WHERE bucket = 'all' LIMIT ?` với `?` bằng `ceil(30% × số phim)` (hằng `TOP_SHARE = 0.30` trong `app.py`).
* **Màn hình hiển thị:** `Analyst › BR7`: biểu đồ phân tán lượt xem (trục log) so với điểm trung bình, màu theo thể loại, hover hiện tỉ lệ hoàn thành và giờ xem, cho 30% phim xem nhiều nhất.
* **Lý do chọn giải pháp:** *Vì sao batch:* các chỉ số hiệu quả được tích luỹ theo phim. *Vì sao dùng chung bảng với BR2:* top 30% theo lượt xem là một phần đầu của cùng danh sách xếp theo lượt xem, nên không cần bảng riêng; đọc 6.000 dòng mất 32 ms p50 (`storage_latency.md`). *Vì sao 30%:* ngưỡng cố định trong `app.py` (`TOP_SHARE`), không rút ra từ phân phối. Lượt xem lệch nhiều (p50 26, p99 353, `data_profile.md`) nên trục log là cần thiết để đọc biểu đồ. *Phương án đã cân nhắc:* hiển thị mọi 20.000 phim (biểu đồ rối); phân bậc theo phân vị tính từ dữ liệu.
* **Tiêu chí thành công và kết quả đo:** trả đủ top 30% phim theo lượt xem và lượt xem nhỏ nhất vượt phân vị 70: **6.000 phim, lượt xem nhỏ nhất 37, đạt** (`br_effectiveness.md`).
* **Giới hạn hiện tại:** ngưỡng 30% là quy ước cố định; tỉ lệ hoàn thành cắt ở 100% và tính từ trung bình thời lượng xem trên thời lượng phim; "hiệu quả" chỉ gồm các số đo có sẵn, chưa có doanh thu hay chi phí bản quyền để tính lợi nhuận trên mỗi tựa; không tính theo thời gian.

### BR8 - Phát hiện phim ít tương tác

* **Nhóm người dùng:** Manager.
* **Câu hỏi nghiệp vụ:** Phim nào ít người xem nhất?
* **Mục tiêu kinh doanh hỗ trợ:** O3 (cân nhắc không gia hạn tựa ít xem).
* **Dữ liệu dùng:** `movies` (kể cả phim chưa có lượt xem) và `watch_history` (số lượt xem, `completion_rate`).
* **Đường xử lý:** batch Spark. `build_tables`, bảng `low_engagement_movies` (từ `movie_performance` sắp theo `views ASC, title ASC`; phim không có lượt xem vẫn có mặt nhờ phép join trái); `serving_frames`.
* **Bảng phục vụ và truy vấn:** `low_engagement_movies`, khoá phân vùng `bucket`, clustering `views ASC, title ASC, movie_id ASC`: `SELECT movie_id, title, genre, views, completion_rate FROM low_engagement_movies WHERE bucket = 'all' LIMIT 10`.
* **Màn hình hiển thị:** `Manager › BR8`: bảng 10 phim ít lượt xem nhất kèm thể loại và tỉ lệ hoàn thành.
* **Lý do chọn giải pháp:** *Vì sao batch:* quyết định gỡ nội dung không cần thời gian thực. *Vì sao cluster ngược:* sắp `views ASC` để `LIMIT 10` lấy ngay các phim ít xem nhất mà không quét 20.000 dòng (1,0 ms p50, nhanh nhất trong các truy vấn, `storage_latency.md`). *Vì sao giữ cả phim 0 lượt xem:* phim chưa ai xem là ứng viên rõ nhất, nên bảng được dựng từ danh mục phim chứ không từ lịch sử xem; `completion_rate` của phim đó là null chứ không phải 100% (một lỗi của Spark đã được sửa, `docs/evaluation.md` mục 2). *Phương án đã cân nhắc:* ngưỡng "dưới X lượt xem" thay vì top 10; sắp theo tỉ lệ hoàn thành.
* **Tiêu chí thành công và kết quả đo:** 10 phim ít xem nhất bằng danh sách Pandas độc lập: **lượt xem [1, 1, 2, 2, 2, 2, 2, 2, 2, 2], đạt** (`br_effectiveness.md`). Hồ sơ dữ liệu: 0 phim không có lượt xem (`data_profile.md`).
* **Giới hạn hiện tại:** *chỉ là 10 phim ít xem nhất*, không có ngưỡng thế nào là "ít"; không chuẩn hoá theo tuổi phim (phim mới ra ít lượt là bình thường) hay theo thể loại; hoà giải theo tên phim chỉ để cho thứ tự ổn định; không có trường hợp phim 0 lượt xem trong dữ liệu hiện tại nên nhánh đó chỉ được kiểm bằng bộ fixture (`tests/test_fixture_tables.py`).

### BR9 - Phân khúc người dùng

* **Nhóm người dùng:** Manager.
* **Câu hỏi nghiệp vụ:** Có bao nhiêu người dùng Inactive, Casual, Active?
* **Mục tiêu kinh doanh hỗ trợ:** O2 (xác định nhóm cần kéo quay lại).
* **Dữ liệu dùng:** `users` (mọi người dùng, kể cả chưa xem) và `watch_history` (số lượt xem mỗi user).
* **Đường xử lý:** batch Spark. `build_tables`: `user_engagement` (cột `segment`: ≤ 10 lượt là Inactive, ≤ 35 là Casual, còn lại Active) rồi `user_segments`.
* **Bảng phục vụ và truy vấn:** `user_segments`, khoá chính `segment` (3 dòng): `SELECT segment, users FROM user_segments`.
* **Màn hình hiển thị:** `Manager › BR9`: biểu đồ tròn số người dùng theo 3 phân khúc.
* **Lý do chọn giải pháp:** *Vì sao batch:* phân khúc phụ thuộc lượt xem tích luỹ. *Vì sao phân khúc theo ngưỡng số lượt:* quy tắc đơn giản, giải thích được cho quản lý. *Vì sao luôn có đủ 3 phân khúc:* bảng được ghép với danh sách phân khúc cố định, nên phân khúc rỗng vẫn hiện với 0 người dùng (một lỗi đã phát hiện nhờ dữ liệu thưa, `docs/evaluation.md` mục 2). *Phương án đã cân nhắc:* phân cụm K-means trên (lượt xem, phút xem, số ngày hoạt động), RFM (gần đây, tần suất, giá trị); chưa làm, vì khó giải thích bằng bộ dữ liệu chỉ có lượt xem.
* **Tiêu chí thành công và kết quả đo:** các phân khúc phủ đúng một lần mọi người dùng: **50.000 user trong 3 phân khúc, đạt** (`br_effectiveness.md`). Hồ sơ: 42,18% user có ≤ 10 lượt, 71,86% có ≤ 35 lượt (`data_profile.md`), nghĩa là khoảng 29,7% Casual và 28,1% Active (tính từ hiệu hai tỉ lệ đó).
* **Giới hạn hiện tại:** *ngưỡng 10 và 35 là cố định*, không rút ra từ dữ liệu; chỉ dựa trên số lượt xem, không xét độ gần đây hay thời gian xem; 168 user chưa xem lượt nào được gộp vào Inactive cùng người có 1-10 lượt, không tách riêng; phân khúc theo toàn bộ 180 ngày, không theo cửa sổ trượt.

### BR10 - Phân tích xu hướng theo thời gian

* **Nhóm người dùng:** Manager.
* **Câu hỏi nghiệp vụ:** Lượt xem thay đổi thế nào theo ngày?
* **Mục tiêu kinh doanh hỗ trợ:** O1 và O3 (theo dõi sức khoẻ tổng thể của nền tảng).
* **Dữ liệu dùng:** `watch_history` (`watch_timestamp`, `user_id`).
* **Đường xử lý:** batch Spark. `build_tables`, bảng `daily_views` (lượt xem và người xem duy nhất theo ngày UTC); bảng `hourly_views` (phân tích theo giờ) thuộc BR6.
* **Bảng phục vụ và truy vấn:** `daily_views`, khoá phân vùng `bucket`, clustering `day ASC`: `SELECT day, views FROM daily_views WHERE bucket = 'all'` (180 dòng, một partition nhỏ).
* **Màn hình hiển thị:** `Manager › BR10`: biểu đồ đường lượt xem theo ngày. Phân tích theo giờ (`hourly_views`) hiển thị ở trang Analyst, mục BR6.
* **Lý do chọn giải pháp:** *Vì sao batch:* xu hướng theo ngày chỉ cần cập nhật theo ngày. *Vì sao một partition:* chuỗi thời gian 180 điểm nhỏ, đọc hết bằng một truy vấn, sắp xếp sẵn theo ngày (1,67 ms p50, `storage_latency.md`); ở quy mô nhiều năm sẽ chia theo tháng hoặc năm. *Vì sao ngày UTC:* timestamp được chuẩn hoá về UTC khi làm sạch. *Phương án đã cân nhắc:* truy vấn ad hoc trực tiếp từ `curated.watch_events` (phân vùng theo `watch_date`, 485 ms ở p50 cho một tuần, `storage_latency.md`), linh hoạt hơn nhưng chậm hơn.
* **Tiêu chí thành công và kết quả đo:** một điểm mỗi ngày và cộng đúng tổng lượt xem: **180 ngày, 1.000.000 lượt xem, đạt** (`br_effectiveness.md`).
* **Giới hạn hiện tại:** *chỉ có tổng xu hướng toàn nền tảng*, không tách theo thể loại, phim, vùng hay gói thuê bao; không có phát hiện bất thường hay dự báo; dữ liệu sinh ra gần như phẳng theo ngày (5.318-5.731 sự kiện mỗi ngày, `data_profile.md`) nên biểu đồ không kể được xu hướng thật; chưa so sánh tuần với tuần.

## 5. Bảng truy vết BR → mã → bảng → kiểm thử / benchmark

Mọi BR batch dùng chung các kiểm thử `tests/test_fixture_tables.py` (giá trị từng ô của 10 bảng tính tay, trên cả Pandas và Spark), `tests/test_parity.py` (Pandas và Spark cho 10 bảng giống hệt),
`tests/test_cassandra_counts.py` (số dòng Cassandra khớp batch) và `tests/test_storage_equivalence.py` (12 truy vấn dashboard cho cùng kết quả trên Cassandra và Hive/Parquet); cột kiểm thử dưới đây chỉ nêu phần riêng của từng BR.
`br_effectiveness.md` là kết quả của `python -m benchmarks.br_effectiveness` chạy trên cả 10 BR.

| BR | Mã (file: hàm) | Bảng Cassandra | Kiểm thử / benchmark riêng |
|---|---|---|---|
| BR1 | `src/spark_batch_job.py: build_tables`; `app.py: catalog_for` | `movie_performance` | `br_effectiveness.md` (BR1, 2 tiêu chí); `storage_latency.md` (`genre_catalog`) |
| BR2 batch | `src/spark_batch_job.py: build_tables, serving_frames` | `top_viewed_movies` | `br_effectiveness.md` (BR2); `storage_latency.md` (`top_viewed`) |
| BR2 live | `src/kafka_event_producer.py: send_events`; `flink/flink_trending_sql_job.py: main`; `src/trending_materializer.py: consume_results, write_snapshot`; `app.py: render_live_trending` | `live_trending` (`live_trending_spark` cho Spark Streaming) | `tests/test_cassandra_counts.py::test_live_trending_tables_keep_ranks_one_to_ten`; `stream_latency.md` |
| BR3 | `src/spark_batch_job.py: build_tables, serving_frames` (`user_preferences`, `rating_stats`) | `user_preferences`, `movies_by_genre_rating` | `br_effectiveness.md` (BR3, 2 tiêu chí); `storage_latency.md` (`user_favorite_genres`, `genre_top_rated`) |
| BR4 | `src/spark_batch_job.py: build_tables` (`rating_stats`), `serving_frames` | `top_rated_movies`, `summary_metrics` | `br_effectiveness.md` (BR4, 2 tiêu chí); `storage_latency.md` (`top_rated`) |
| BR5 | `src/spark_batch_job.py: build_tables` (`genre_popularity`) | `genre_popularity` | `br_effectiveness.md` (BR5); `storage_latency.md` (`genre_popularity`) |
| BR6 | `src/spark_batch_job.py: build_tables` (`user_engagement`, `subscription_views`, `hourly_views`) | `user_engagement`, `subscription_views`, `hourly_views` | `br_effectiveness.md` (BR6); `storage_latency.md` (`user_views_scan`, `hourly_views`) |
| BR7 | `src/spark_batch_job.py: build_tables` (`movie_performance`), `serving_frames` | `top_viewed_movies` | `br_effectiveness.md` (BR7); `storage_latency.md` (`top_share_performance`) |
| BR8 | `src/spark_batch_job.py: build_tables` (`low_engagement_movies`) | `low_engagement_movies` | `br_effectiveness.md` (BR8); `storage_latency.md` (`low_engagement`) |
| BR9 | `src/spark_batch_job.py: build_tables` (`user_engagement.segment`, `user_segments`) | `user_segments` | `br_effectiveness.md` (BR9); `tests/test_parity.py` trên dữ liệu thưa (đủ 3 phân khúc); `storage_latency.md` (`user_segments`) |
| BR10 | `src/spark_batch_job.py: build_tables` (`daily_views`) | `daily_views` | `br_effectiveness.md` (BR10); `storage_latency.md` (`daily_views`) |
| Chất lượng (mọi BR) | `src/data_quality.py: clean_pandas, clean_spark`; `generate_data.py: inject_dirty_rows` | `curated.quality_report` (Hive) | `tests/test_data_quality.py`; `data_quality_demo.txt`; `data_quality_real.txt` |
| Hồ sơ dữ liệu | `src/profile_data.py: profile` | không | `tests/test_profile_data.py`; `data_profile.md` |
