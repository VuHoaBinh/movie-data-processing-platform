# Business-requirement success criteria measured on the running stack

| br | user_group | criterion | measured | pass | query_ms |
|---|---|---|---|---|---|
| BR1 | Viewer | browse-all returns every movie (8 genre partitions) | 20,000 of 20,000 movies | yes | 134.6 |
| BR1 | Viewer | title search 'spider' finds the Spider-Man titles | 2 titles | yes | 134.6 |
| BR2 | Viewer | all-time top-10 equals the Pandas ranking | True | yes | 2.3 |
| BR2 | Viewer | live window has ranks 1..10 and is < 10 min old | 10 rows, newest window 9.4 min old | yes | 1.8 |
| BR3 | Viewer | genre-based: no watched title is recommended (200 random users) | 0 watched titles in 2,000 recommendations for 200 users with a list | yes | 2.3 |
| BR3 | Viewer | genre-based: favourite-genre rows stay in the user's top-2 genres and lists equal the Pandas lists | 0 users with a foreign genre; 200/200 lists identical | yes | 2.3 |
| BR3 | Viewer | ALS: no watched title is recommended (200 random users) | 0 watched titles in 1,990 recommendations for 199 users with a list | yes | 2.3 |
| BR3 | Viewer | share of users that get favourite-genre recommendations (the rest are cold-start, shown popular titles) | 99.66% genre-based; every user has a list: True | yes | 0.0 |
| BR3 | Viewer | ALS serves every user with watch history (cold-start users fall back to the genre-based list) | 49,832 of 49,832 users with history (99.66% of all users) | yes | 436.9 |
| BR4 | Analyst | top-10 by weighted rating equals the Pandas ranking | True | yes | 2.9 |
| BR4 | Analyst | no top-10 title rests on very few ratings | min rating_count in top-10 = 169 | yes | 2.9 |
| BR5 | Analyst | sum of genre views = total views | 1,000,000 vs 1,000,000 | yes | 2.2 |
| BR5 | Analyst | unique viewers per genre equal the Pandas distinct count and never exceed the user count | 8 genres, max 42,915 of 50,000 users | yes | 2.2 |
| BR6 | Analyst | engagement scan covers all users; views add up | 50,000 users, 1,000,000 views | yes | 485.0 |
| BR6 | Analyst | watch minutes and active days per user equal the Pandas values | 94,838,155 minutes, active days 0-58 (mean 18.3, median 12), median watch minutes 1141 | yes | 485.0 |
| BR7 | Analyst | top 30% of movies by views are returned, with the minimum views above the 70th percentile | 6,000 movies, min views 37 | yes | 28.1 |
| BR8 | Manager | 10 lowest-viewed titles equal the Pandas list | views [1, 1, 2, 2, 2, 2, 2, 2, 2, 2] | yes | 2.4 |
| BR9 | Manager | segments cover all users exactly once | 50,000 users in 3 segments | yes | 1.8 |
| BR10 | Manager | daily series has one point per day and adds up to total views | 180 days, 1,000,000 views | yes | 2.6 |

Each criterion is evaluated on Cassandra and compared with an independent Pandas computation from the raw CSVs (`query_ms`: client-side time of the Cassandra query/queries involved, 1M watch rows).
