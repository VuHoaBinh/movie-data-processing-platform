# Movie Streaming Analytics Platform

Demo scope: BR1–BR10 — search/filter, trending, recommendations, rating analytics, genre popularity, engagement, performance, low-engagement detection, segmentation, and time trends.

```powershell
python generate_data.py
python -m pip install -r requirements.txt
streamlit run app.py
```

Mặc định sẽ tạo **1.570.000 dòng dữ liệu**: 50.000 users, 20.000 movies, 500.000 ratings và 1.000.000 lượt xem. Dữ liệu có nhóm hoạt động, gói thuê bao, thể loại ưa thích, giờ xem buổi tối/cuối tuần, phim thịnh hành và phim mới phát hành để biểu đồ có xu hướng dễ quan sát. Có thể đổi quy mô, ví dụ: `python generate_data.py --users 2000 --movies 1000 --ratings 20000 --watches 50000`.

Pipeline: `CSV raw data → Spark (daily aggregates/recommendations) + Flink (live events) → BigQuery → Streamlit`. The demo reads CSV locally; replace the loader with BigQuery queries once tables exist.
