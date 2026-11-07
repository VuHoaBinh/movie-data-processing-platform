# BR3 recommenders: offline evaluation on a time-based split

| method | users_evaluated | precision_at_10 | recall_at_10 | ndcg_at_10 | hit_rate_at_10 | catalogue_coverage | users_with_full_list | fit_seconds | score_seconds |
|---|---|---|---|---|---|---|---|---|---|
| Popularity baseline | 40865 | 0.0314 | 0.0815 | 0.0543 | 0.2637 | 0.0013 | 1.0 | 0.0 | 0.8 |
| Genre-based (deployed) | 40865 | 0.0147 | 0.0394 | 0.0299 | 0.1365 | 0.0048 | 1.0 | 0.0 | 3.8 |
| Genre-based, ranked by views (sensitivity) | 40865 | 0.0279 | 0.0726 | 0.0505 | 0.2374 | 0.0053 | 1.0 | 0.0 | 3.9 |
| ALS (deployed) | 40865 | 0.0262 | 0.0679 | 0.0472 | 0.2246 | 0.0267 | 0.9956 | 10.8 | 10.8 |
| ALS rank 16 | 40865 | 0.0292 | 0.0732 | 0.05 | 0.2448 | 0.0279 | 0.9956 | 5.4 | 5.7 |
| ALS rank 64 | 40865 | 0.0218 | 0.0579 | 0.0421 | 0.1938 | 0.0267 | 0.9956 | 18.1 | 13.5 |
| ALS alpha 10 | 40865 | 0.0273 | 0.0694 | 0.0494 | 0.2315 | 0.0267 | 0.9956 | 7.1 | 7.7 |

**Split.** Events before 2026-08-28 23:59 UTC are training data (834,438 views, 417,330 ratings); the last 30 days up to 2026-09-27 are the test period (165,562 views). No recommender sees anything from the test period.
**Evaluated users.** 40,865 users watched at least one title in the test period that they had not watched before; 181 of them have no training history (cold start). Held-out titles per user: mean 3.8. 1 of the 18,490 distinct held-out titles had no training view (new releases), which no recommender can know.
**Metrics (K = 10, averaged over evaluated users, a user without a list scores 0).** precision@10 = hits / 10; recall@10 = hits / number of held-out titles of the user; ndcg@10 = DCG of the hits with log2 discount / the best possible DCG (binary relevance); hit_rate@10 = share of users with at least one hit; catalogue_coverage = distinct recommended titles / 20,000 titles; users_with_full_list = share of evaluated users that got ten titles from the method itself (ALS cannot serve users without training history; the dashboard then shows the genre-based list).
**Candidates.** Every method excludes the titles the user watched before the cutoff, as the deployed tables do.
**Genre-based variants.** 'Genre-based (deployed)' ranks the unwatched titles of the user's two most-watched genres by Bayesian rating, as `user_recommendations` does; the 'ranked by views' row keeps the candidates and changes only the ranking key to training views (a sensitivity check, not deployed).
**ALS.** Implicit feedback, strength = number of watches of the title by the user; rank=32, regParam=0.1, alpha=40.0, maxIter=10, seed=42, 8 user and item blocks. The 'deployed' setting was fixed before the evaluation; the other ALS rows only show sensitivity.
**Times.** `fit_seconds`: ALS training (popularity: sorting the titles; genre-based: none, it reuses the favourite-genre and rating aggregates the batch already computes for BR1-BR10). `score_seconds`: building all lists (ALS: all users with history; popularity: evaluated users only; genre-based: all users). Pandas for the first two, Spark `local[*]` for ALS, so the times are indicative, not a like-for-like engine comparison.
Synthetic data: the generator draws 43% of a user's views from one preferred genre and the rest mostly from blockbusters and hits, so the numbers describe this dataset, not real viewers.
