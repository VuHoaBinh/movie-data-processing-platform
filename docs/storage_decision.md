# ADR-001: Chọn công nghệ quản lý dữ liệu — Apache Cassandra + Hive/Parquet

- Trạng thái: Đã chấp nhận
- Phạm vi: lớp lưu trữ của Movie Streaming Analytics Platform (BR1–BR10)

## 1. Bối cảnh

Dự án có hai luồng dữ liệu:

| Luồng | Đặc điểm | Truy vấn điển hình |
|---|---|---|
| Batch (users 50k, movies 20k, ratings 500k, watch_history 1M, lớn dần) | Ghi theo lô, đọc toàn bộ để tổng hợp | `GROUP BY` toàn bảng, join, xếp hạng |
| Stream (Kafka → Flink / Spark Streaming) | Append liên tục, mỗi movie 1 dòng mỗi cửa sổ 5 phút | "Top 10 của cửa sổ mới nhất" |
| Dashboard (Streamlit) | Đọc nhiều, độ trễ thấp, truy vấn theo khoá | theo `movie_id`, `user_id`, `genre`, `day` |

Trước đây mọi thứ lưu thành CSV cục bộ: không có schema, không có truy vấn theo khoá, không có
lưu trữ cho dữ liệu stream, và đề bài yêu cầu một công nghệ *quản lý dữ liệu* thực sự.

## 2. Quyết định

Dùng **hai lớp lưu trữ bổ trợ nhau**:

1. **Serving layer = Apache Cassandra 5.0** (keyspace `movie_analytics`, `sql/cassandra_schema.cql`).
   Mỗi bảng được thiết kế theo một truy vấn của dashboard (query-first). Dashboard và kết quả stream
   chỉ đọc/ghi ở đây.
2. **Analytical/lake layer = Hive tables trên Parquet** (Spark SQL `enableHiveSupport()`, metastore
   Derby nhúng, warehouse ở `data/warehouse/`) gồm 3 vùng:
   - `raw.*`: bản sao Parquet của CSV, schema tường minh, chưa làm sạch.
   - `curated.*`: dữ liệu đã chuẩn hoá (timestamp UTC, bỏ trùng/thiếu); `curated.watch_events`
     **phân vùng theo `watch_date`**.
   - `analytics.*`: 19 bảng tổng hợp của dashboard dùng chung với Pandas (kể cả `user_recommendations`) và, nếu batch chạy ALS, `user_recommendations_als`; nguồn để nạp vào Cassandra (24 bảng phục vụ sau khi tách theo truy vấn).

Luồng: `CSV → raw → curated → analytics → Cassandra`. Spark luôn tính lại từ lớp lake; Cassandra
chỉ là bản "phục vụ" có thể dựng lại bất cứ lúc nào.

## 3. Vì sao Cassandra cho serving

- **Truy vấn theo khoá phân vùng**: dashboard chỉ cần "lấy theo `genre`", "lấy theo `user_id`",
  "lấy N dòng đầu của bảng đã sắp xếp sẵn" — đúng thế mạnh của partition key + clustering order.
- **Ghi nhiều, append-only cho stream**: bảng `live_trending` (phân vùng theo ngày, clustering
  `window_start DESC, rank`) ghi 10 dòng mỗi cửa sổ; `LIMIT 10` trả ngay cửa sổ mới nhất mà không
  cần sắp xếp lúc đọc. Có `default_time_to_live` 3 ngày để dữ liệu live tự hết hạn.
- **Mở rộng ngang**: thêm node là tăng dung lượng và thông lượng ghi; story Big Data rõ ràng.
- **Connector chính thức**: `spark-cassandra-connector` (Spark 3.5.x, Scala 2.12) và
  `cassandra-driver` (Python), chạy local bằng Docker `cassandra:5.0` cho demo.

### Ánh xạ BR → bảng → truy vấn

| BR | Bảng Cassandra | Truy vấn dashboard |
|---|---|---|
| BR1 Tìm kiếm / lọc | `movie_performance` PK `((genre), views DESC, movie_id)` | `WHERE genre = ?` (lọc tiêu đề ở ứng dụng) |
| BR2 Trending | batch toàn thời gian: `top_viewed_movies`; batch đang lên: `trending_growth` PK `((bucket), growth DESC, movie_id)`; live: `live_trending` | `WHERE bucket='all' LIMIT 10` (cả hai bảng batch); `WHERE day = ? LIMIT 10` |
| BR3 Gợi ý cá nhân | `user_recommendations` PK `((user_id), rank)`; phương án ALS: `user_recommendations_als` (cùng cấu trúc); `user_preferences` (hiển thị thể loại ưa thích) | `WHERE user_id = ?` (một partition, 10 dòng đã xếp hạng). `movies_by_genre_rating` vẫn dùng cho bộ lọc thể loại của BR4 |
| BR4 Rating | `top_rated_movies` | `WHERE bucket='all' LIMIT 10` |
| BR5 Genre | `genre_popularity` | đọc 8 dòng (`views`, `unique_viewers`) |
| BR6 Engagement | `user_engagement`, `subscription_views`, `hourly_views` | một lần quét `user_engagement` lấy `views`, `watch_minutes`, `active_days` (cùng lượt đọc); đọc bảng nhỏ |
| BR7 Performance | `top_viewed_movies` | `WHERE bucket='all' LIMIT ceil(30% × movies)` |
| BR8 Low engagement | `low_engagement_movies` PK `((bucket), priority, views, movie_id)`; `low_engagement_by_genre` PK `((genre), priority, views, movie_id)` (cùng dòng, đọc theo thể loại); `genre_engagement_peers` (8 dòng); `lowest_viewed_movies` (đối chứng) | `WHERE bucket='all' LIMIT 10`; `WHERE genre = ? LIMIT 20`; đọc 8 dòng |
| BR9 Segment | `user_segments`, `segment_thresholds` PK `((bucket), percentile)`, `segment_agreement` PK `((bucket), activity_profile, segment)` | đọc 3 dòng; `WHERE bucket='all'` (2 và 9 dòng) |
| BR10 Xu hướng | `daily_views` PK `((bucket), day)`; `genre_daily_views` PK `((bucket), day, genre)` (1.440 dòng); `movie_daily_views` PK `((movie_id), day)`; `weekday_weekend_views` PK `((bucket), genre, day_type)` | `WHERE bucket='all'`; `WHERE movie_id = ?` |
| Tổng quan | `summary_metrics` | `WHERE bucket='all'` |
| Trang Data Engineering | `batch_runs` PK `((bucket), finished_at DESC)`, `batch_table_rows`, `quality_report` PK `((table_name), reason)`; `live_trending` | `WHERE bucket='all' LIMIT 5`; đọc toàn bảng nhỏ; `WHERE day = ? LIMIT 10` |

Cùng một dữ liệu có thể xuất hiện ở nhiều bảng (denormalization) — đây là đánh đổi chủ động của
mô hình query-first: ghi nhiều hơn một chút để mỗi truy vấn đọc chỉ chạm một partition.

## 4. Vì sao Hive/Parquet cho lake

- Lưu cột (columnar) + nén: rẻ, quét toàn bộ lịch sử nhanh, phù hợp tính lại và truy vấn ad-hoc.
- Metastore giữ schema/định nghĩa bảng; `curated.watch_events` phân vùng theo ngày cho phép cắt
  tỉa partition khi truy vấn theo khoảng thời gian.
- Spark SQL đọc/ghi trực tiếp, không cần cụm riêng. Đây cũng là "giải pháp thay thế" để so sánh
  với Cassandra khi phục vụ cùng các truy vấn dashboard (xem `docs/evaluation.md`).

## 5. Đánh đổi (trade-offs)

| Vấn đề | Hệ quả | Cách xử lý |
|---|---|---|
| Không có `LIKE`/tìm chuỗi con, không `JOIN`, `GROUP BY` hạn chế | BR1 tìm theo tiêu đề phải lọc ở ứng dụng; mọi tổng hợp phải tính trước | Spark tính trước, Cassandra chỉ phục vụ; (tuỳ chọn) SAI index của Cassandra 5 |
| Mô hình theo truy vấn → thêm truy vấn mới = thêm bảng | Trùng lặp dữ liệu | Bảng dẫn xuất sinh từ cùng DataFrame trong `serving_frames()` |
| Partition lớn (`bucket='all'` chứa ~20k dòng) | Nóng một partition nếu tải đọc rất lớn | Ở quy mô lớn chia bucket theo genre/tháng hoặc chỉ giữ top-K |
| Làm mới batch = truncate + insert (`write_cassandra`) | Trong lúc làm mới, dashboard có thể thấy bảng rỗng hoặc dở dang trong thời gian ngắn | Chấp nhận cho demo; production dùng bảng có phiên bản (ghi bảng mới rồi đổi con trỏ/alias) hoặc hoán đổi |
| Quét toàn bộ (BR6 histogram) kém hơn Parquet | Chậm hơn khi đọc cả bảng | Benchmark trong `docs/evaluation.md`; có thể tính histogram trước |
| Derby nhúng chỉ cho 1 tiến trình Spark tại một thời điểm | Không chạy hai job Hive đồng thời | Production dùng Hive Metastore Service + Postgres/MySQL |
| Mỗi lần `TRUNCATE` Cassandra chụp snapshot (`auto_snapshot`) | Mỗi lần batch để lại snapshot của dữ liệu cũ (khoảng 11-13 MB cho mỗi bảng gợi ý, 274 MiB sau khoảng chục lần chạy khi phát triển), đĩa đầy dần | `nodetool clearsnapshot --all` (đã dùng trước khi đo dung lượng); production tắt `auto_snapshot` hoặc đổi sang ghi bảng mới rồi hoán đổi |
| Cassandra nhất quán cuối cùng (eventual) | Đọc ngay sau ghi có thể cũ | RF=1 trong demo; production RF=3 + `LOCAL_QUORUM` |

## 6. Phương án đã loại

| Phương án | Lý do không chọn |
|---|---|
| **BigQuery** | Cần tài khoản/billing đám mây, không chạy offline để demo; phù hợp lake/warehouse nhưng không có phục vụ độ trễ thấp theo khoá. `sql/bigquery_schema.sql` cũ đã bị xoá. |
| **Redshift** | Cũng cần AWS; là kho dữ liệu cột, độ trễ truy vấn điểm và ghi stream liên tục kém Cassandra; chi phí cố định cao cho tải nhỏ. |
| **Neo4j** | Mạnh ở truy vấn đồ thị (quan hệ nhiều bước). Dữ liệu ở đây là bảng tổng hợp theo khoá, không có bài toán duyệt đồ thị; ghi ngang quy mô lớn khó hơn. |
| **MongoDB** | Linh hoạt, có truy vấn phong phú, nhưng ghi time-series append-only và mở rộng ngang cho khối lượng ghi lớn kém tự nhiên hơn; không có clustering order sẵn cho "top-N mới nhất". Là phương án hợp lý thứ hai. |
| **PostgreSQL / MySQL** | Đơn giản, đủ cho quy mô hiện tại, nhưng không minh hoạ được mô hình phân tán/mở rộng ngang mà đề bài hướng tới. |
| **Chỉ Parquet/Hive (không Cassandra)** | Độ trễ mỗi truy vấn qua Spark SQL cỡ giây, không phù hợp dashboard tương tác — được đo cụ thể trong phần benchmark. |
| **Chỉ Cassandra (không lake)** | Không có vùng raw/curated để tính lại, quét lịch sử đắt, không có metastore schema. |

## 7. Hệ quả

- Cần vận hành thêm Cassandra (heap 1 GB trong demo) và Derby metastore cho Hive.
- Mọi bảng phục vụ có thể dựng lại từ lake bằng `python -m src.spark_batch_job`.
- Kiểm thử tính đúng: số dòng Cassandra phải khớp đầu ra Spark (`tests/test_cassandra_counts.py`).
- `user_recommendations` có 10 dòng cho mỗi user (500.000 dòng, 50.000 partition nhỏ, 11,4 MB trên đĩa): đổi lấy việc dashboard chỉ đọc một partition thay vì ghép hai truy vấn lúc đọc, và loại được phim đã xem ngay ở batch (Cassandra không có anti-join lúc đọc). Bảng ALS thêm 498.320 dòng nữa (49.832 user có lịch sử, 12,8 MB).
