# Business-requirement success criteria measured on the running stack

| br | user_group | criterion | measured | pass | query_ms |
|---|---|---|---|---|---|
| BR1 | Viewer | browse-all returns every movie (8 genre partitions) | 20,000 of 20,000 movies | yes | 818.4 |
| BR1 | Viewer | title search 'spider' finds the Spider-Man titles | 2 titles | yes | 818.4 |
| BR2 | Viewer | all-time top-10 equals the Pandas ranking | True | yes | 6.0 |
| BR2 | Viewer | live window has ranks 1..10 and is < 10 min old | 10 rows, newest window 8.1 min old | yes | 9.0 |
| BR3 | Viewer | recommendations stay in the user's top-2 genres (200 random users) | 199/199 users with history OK | yes | 14.9 |
| BR3 | Viewer | share of users that get personalised results | 99.66% (rest are cold-start, shown popular picks) | yes | 0.0 |
| BR4 | Analyst | top-10 by weighted rating equals the Pandas ranking | True | yes | 15.6 |
| BR4 | Analyst | no top-10 title rests on very few ratings | min rating_count in top-10 = 169 | yes | 15.6 |
| BR5 | Analyst | sum of genre views = total views | 1,000,000 vs 1,000,000 | yes | 20.5 |
| BR6 | Analyst | engagement histogram covers all users; views add up | 50,000 users, 1,000,000 views | yes | 3795.5 |
| BR7 | Analyst | top 30% of movies by views are returned, with the minimum views above the 70th percentile | 6,000 movies, min views 37 | yes | 254.5 |
| BR8 | Manager | 10 lowest-viewed titles equal the Pandas list | views [1, 1, 2, 2, 2, 2, 2, 2, 2, 2] | yes | 10.4 |
| BR9 | Manager | segments cover all users exactly once | 50,000 users in 3 segments | yes | 15.9 |
| BR10 | Manager | daily series has one point per day and adds up to total views | 180 days, 1,000,000 views | yes | 21.4 |

Each criterion is evaluated on Cassandra and compared with an independent Pandas computation from the raw CSVs (`query_ms`: client-side time of the Cassandra query/queries involved, 1M watch rows).
