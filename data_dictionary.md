# Data dictionary

| Dataset | Fields | Keys / notes |
|---|---|---|
| users | user_id, age, gender, region, registration_date, subscription_plan, activity_profile | `user_id` primary key; profile creates realistic engagement groups |
| movies | movie_id, title, genre, release_year, duration, content_type, language, maturity_rating | `movie_id` primary key; duration is minutes |
| ratings | rating_id, user_id, movie_id, rating, timestamp | FKs to users/movies; rating is 1–5 |
| watch_history | watch_id, user_id, movie_id, watch_timestamp, watch_duration, device | FKs to users/movies; duration is minutes |

Relationships: `users 1—N ratings`, `movies 1—N ratings`, `users 1—N watch_history`, and `movies 1—N watch_history`.
