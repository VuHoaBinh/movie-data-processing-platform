# ADR-001: Chọn công nghệ quản lý dữ liệu — Apache Cassandra + Hive/Parquet

- Trạng thái: Đã chấp nhận
- Phạm vi: lớp lưu trữ của Movie Streaming Analytics Platform (BR1–BR10)

## 1. Bối cảnh

Dự án có hai luồng dữ liệu:

| Luồng | Đặc điểm | Truy vấn điển hình |
|---|---|---|
| Batch (users 50k, movies 20k, ratings 500k, watch_history 1M, lớn dần) | Ghi theo lô, đọc toàn bộ để tổng hợp | `GROUP BY` toàn bảng, join, xếp hạng |
| Stream (Kafka → Spark Structured Streaming / Flink SQL) | Append liên tục: top 10 mỗi cửa sổ 5 phút (BR2); tổng cộng dồn theo (ngày, user) và (ngày, phim) (BR6, BR8) | "Top 10 của cửa sổ mới nhất", "tổng hôm nay của user/phim" |
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
   - `raw.*`: bản sao Parquet của CSV, schema tường minh, chưa làm sạch; `raw.watch_events_stream` là bản
     sao của archive sự kiện Kafka (`data/warehouse/landing/watch_events`, CSV do `stream_archiver` ghi).
   - `curated.*`: dữ liệu đã chuẩn hoá (timestamp UTC, bỏ trùng/thiếu); `curated.watch_events`
     **phân vùng theo `watch_date`**.
   - `analytics.*`: 14 bảng tổng hợp cho BR1–BR10 (nguồn để nạp vào Cassandra).

Luồng: `CSV (trừ ngày replay) + archive Kafka → raw → curated → analytics → Cassandra`. Spark luôn tính
lại từ lớp lake; Cassandra chỉ là bản "phục vụ" có thể dựng lại bất cứ lúc nào.

**Phiên bản batch.** Mỗi lần chạy, Spark ghi các bảng phục vụ dưới một phiên bản mới (cột `bucket`, nằm
trong partition key của các bảng có thứ tự theo chỉ số), rồi mới đổi con trỏ `serving_state` sang phiên bản
đó; phiên bản trước nữa bị xoá. Dashboard luôn đọc theo phiên bản đã công bố nên không thấy bảng rỗng
hay ghi dở. `serving_state` cũng lưu `replay_day` và `batch_cutoff`: dashboard cộng thêm các bảng live
cho các ngày `>= batch_cutoff` (gộp batch + speed theo kiến trúc Lambda).

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

`v` = phiên bản batch đang công bố (`serving_state.version`); `d` = một ngày của speed layer.
Các bảng batch tồn tại ở hai keyspace cùng schema: `movie_analytics` (nhánh Spark) và `movie_analytics_flink`
(nhánh Flink, nạp bằng `src/load_serving.py`); bảng live của cả hai engine nằm trong `movie_analytics`.

| BR | Bảng Cassandra (khoá chính) | Truy vấn dashboard |
|---|---|---|
| BR1 Tìm kiếm / lọc | `movie_performance` `((genre, bucket), views DESC, movie_id)` | `WHERE genre = ? AND bucket = v`; lọc tiêu đề, năm, ngôn ngữ, loại, độ tuổi ở ứng dụng |
| BR2 Trending | `live_trending` / `live_trending_spark` `((day), window_start DESC, rank)` | `WHERE day = d LIMIT 10` |
| BR3 Gợi ý cá nhân | `user_recommendations` `((user_id), rank)` | `WHERE user_id = ?` (bỏ dòng có `bucket` khác `v`) |
| BR4 Rating | `top_rated_movies` `((bucket), weighted_rating DESC, rating_count DESC, movie_id)`; `rating_distribution` `(movie_id)` | `WHERE bucket = v LIMIT 10`; `WHERE movie_id = ?` |
| BR5 Genre | `genre_popularity` `(genre)`; `genre_monthly` `((genre), month)` | đọc 8 dòng; `WHERE genre = ?` |
| BR6 Engagement | `user_engagement` `(user_id)`, `subscription_views`, `hourly_views`; live: `live_user_engagement[_spark]` `((day), user_id)` | quét cột `views`; `WHERE day = d [AND user_id = ?]` |
| BR7 Performance | `top_viewed_movies` `((bucket), views DESC, movie_id)` | `WHERE bucket = v LIMIT ceil(30% × movies)` |
| BR8 Low engagement | `low_engagement_movies` `((bucket), movie_id)`; live: `live_movie_engagement[_spark]` `((day), movie_id)` | `WHERE bucket = v` + `WHERE day = d`, gộp tổng rồi gắn cờ ở ứng dụng |
| BR9 Segment | `user_segments` `(segment)` | đọc 3 dòng |
| BR10 Xu hướng | `view_trends` `((scope, key, period), period_start)`; `daily_views` `((bucket), day)` | `WHERE scope = ? AND key = ? AND period = ?` |
| Tổng quan | `summary_metrics` `(bucket)`, `serving_state`, `replay_clock` | `WHERE bucket = v` |

Cùng một dữ liệu có thể xuất hiện ở nhiều bảng (denormalization) — đây là đánh đổi chủ động của
mô hình query-first: ghi nhiều hơn một chút để mỗi truy vấn đọc chỉ chạm một partition.

## 4. Vì sao Hive/Parquet cho lake

- Lưu cột (columnar) + nén: rẻ, quét toàn bộ lịch sử nhanh, phù hợp tính lại và truy vấn ad-hoc.
- Metastore giữ schema/định nghĩa bảng; `curated.watch_events` phân vùng theo ngày cho phép cắt
  tỉa partition khi truy vấn theo khoảng thời gian.
- Spark SQL đọc/ghi trực tiếp, không cần cụm riêng. Đây cũng là "giải pháp thay thế" để so sánh
  với Cassandra khi phục vụ cùng các truy vấn dashboard (`src/dashboard_queries.py`,
  `tests/test_storage_equivalence.py`).

## 5. Đánh đổi (trade-offs)

| Vấn đề | Hệ quả | Cách xử lý |
|---|---|---|
| Không có `LIKE`/tìm chuỗi con, không `JOIN`, `GROUP BY` hạn chế | BR1 tìm theo tiêu đề phải lọc ở ứng dụng; mọi tổng hợp phải tính trước | Spark tính trước, Cassandra chỉ phục vụ; (tuỳ chọn) SAI index của Cassandra 5 |
| Mô hình theo truy vấn → thêm truy vấn mới = thêm bảng | Trùng lặp dữ liệu | Bảng dẫn xuất sinh từ cùng DataFrame trong `serving_frames()` |
| Partition lớn (một `bucket` chứa ~20k dòng) | Nóng một partition nếu tải đọc rất lớn | Ở quy mô lớn chia bucket theo genre/tháng hoặc chỉ giữ top-K |
| Làm mới batch theo phiên bản | Mỗi lần chạy ghi lại toàn bộ dòng (không truncate); tồn tại song song 2 phiên bản | Con trỏ `serving_state` đổi sau khi ghi xong; xoá phiên bản trước nữa |
| Quét toàn bộ (BR6 histogram) kém hơn Parquet | Chậm hơn khi đọc cả bảng | Đo bằng `tests/test_storage_equivalence.py` / `benchmarks/`; có thể tính histogram trước |
| Derby nhúng chỉ cho 1 tiến trình Spark tại một thời điểm | Không chạy hai job Hive đồng thời | Production dùng Hive Metastore Service + Postgres/MySQL |
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
