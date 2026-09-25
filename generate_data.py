"""Create deterministic, relational demo data for the movie analytics project."""
from __future__ import annotations
import csv, random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
OUT = Path("data"); OUT.mkdir(exist_ok=True)
GENRES = ["Action", "Comedy", "Drama", "Horror", "Romance", "Sci-Fi", "Animation", "Thriller"]
REGIONS = ["Vietnam", "Singapore", "Thailand", "Japan", "Korea"]
DEVICES = ["Mobile", "TV", "Web", "Tablet"]
PREFIX = {"Action":"Shadow", "Comedy":"Happy", "Drama":"Silent", "Horror":"Dark", "Romance":"Love", "Sci-Fi":"Star", "Animation":"Magic", "Thriller":"Hidden"}

def write(name, fields, rows):
    with (OUT / name).open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)

def main():
    users = [{"user_id":f"U{i:04}", "age":random.randint(18,60), "gender":random.choice(["Male","Female","Other"]), "region":random.choice(REGIONS), "registration_date":(datetime(2025,1,1)+timedelta(days=random.randrange(600))).date()} for i in range(1,201)]
    movies=[]
    for i in range(1,101):
        g=GENRES[(i-1)%len(GENRES)]; movies.append({"movie_id":f"M{i:04}","title":f"{PREFIX[g]} {g} {i:02}","genre":g,"release_year":random.randint(1995,2026),"duration":random.randint(80,180)})
    by_id={m['movie_id']:m for m in movies}; pref={u['user_id']:random.choice(GENRES) for u in users}; start=datetime(2026,7,1); watches=[]
    for i in range(1,8001):
        u=random.choice(users); pool=[m for m in movies if m['genre']==pref[u['user_id']]]*5+movies+movies[:12]*8; m=random.choice(pool); ts=start+timedelta(minutes=random.randrange(90*1440)); dur=max(5,min(m['duration'],int(random.gauss(m['duration']*.78,18))))
        watches.append({"watch_id":f"W{i:06}","user_id":u['user_id'],"movie_id":m['movie_id'],"watch_timestamp":ts.strftime('%Y-%m-%d %H:%M:%S'),"watch_duration":dur,"device":random.choice(DEVICES)})
    ratings=[]
    for i,w in enumerate(random.sample(watches,4000),1):
        completion=w['watch_duration']/by_id[w['movie_id']]['duration']; rating=min(5,max(1,round(2.5+completion*2.4+random.gauss(0,.7))))
        ratings.append({"rating_id":f"R{i:06}","user_id":w['user_id'],"movie_id":w['movie_id'],"rating":rating,"timestamp":w['watch_timestamp']})
    write('users.csv',list(users[0]),users); write('movies.csv',list(movies[0]),movies); write('watch_history.csv',list(watches[0]),watches); write('ratings.csv',list(ratings[0]),ratings)
    print('Created users=200, movies=100, ratings=4000, watch_history=8000')
if __name__ == '__main__': main()
