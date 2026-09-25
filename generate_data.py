"""Create deterministic, relational demo data for the movie analytics project."""
from __future__ import annotations
import argparse
import csv, random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
OUT = Path("data"); OUT.mkdir(exist_ok=True)
GENRES = ["Action", "Comedy", "Drama", "Horror", "Romance", "Sci-Fi", "Animation", "Thriller"]
REGIONS = ["Vietnam", "Singapore", "Thailand", "Japan", "Korea"]
DEVICES = ["Mobile", "TV", "Web", "Tablet"]
PREFIX = {"Action":"Shadow", "Comedy":"Happy", "Drama":"Silent", "Horror":"Dark", "Romance":"Love", "Sci-Fi":"Star", "Animation":"Magic", "Thriller":"Hidden"}
# Familiar recent titles make the demo dashboard immediately understandable.
RECENT_TITLES = [
    ("Inside Out 2", "Animation", 2024), ("Deadpool & Wolverine", "Action", 2024),
    ("Dune: Part Two", "Sci-Fi", 2024), ("Moana 2", "Animation", 2024),
    ("Wicked", "Drama", 2024), ("The Wild Robot", "Animation", 2024),
    ("Godzilla x Kong: The New Empire", "Action", 2024), ("Kingdom of the Planet of the Apes", "Action", 2024),
    ("Alien: Romulus", "Horror", 2024), ("Gladiator II", "Drama", 2024),
    ("Furiosa: A Mad Max Saga", "Action", 2024), ("A Quiet Place: Day One", "Horror", 2024),
    ("Beetlejuice Beetlejuice", "Comedy", 2024), ("The Fall Guy", "Action", 2024),
    ("Captain America: Brave New World", "Action", 2025), ("Thunderbolts*", "Action", 2025),
    ("Mission: Impossible – The Final Reckoning", "Action", 2025), ("How to Train Your Dragon", "Animation", 2025),
    ("Jurassic World Rebirth", "Sci-Fi", 2025), ("Superman", "Action", 2025),
    ("The Fantastic Four: First Steps", "Sci-Fi", 2025), ("Lilo & Stitch", "Animation", 2025),
    ("Spider-Man: No Way Home", "Action", 2021), ("Spider-Man: Across the Spider-Verse", "Animation", 2023),
    ("Mai", "Drama", 2024), ("Lật Mặt 7: Một Điều Ước", "Drama", 2024),
    ("Đào, Phở và Piano", "Drama", 2024), ("Nhà Bà Nữ", "Comedy", 2023),
    ("Bố Già", "Comedy", 2021), ("Mắt Biếc", "Romance", 2019),
    ("Em và Trịnh", "Drama", 2022), ("Quỷ Cẩu", "Horror", 2023),
    ("Kẻ Ăn Hồn", "Horror", 2023), ("Cô Dâu Hào Môn", "Comedy", 2024),
]
VIETNAMESE_TITLES = {"Mai", "Lật Mặt 7: Một Điều Ước", "Đào, Phở và Piano", "Nhà Bà Nữ", "Bố Già", "Mắt Biếc", "Em và Trịnh", "Quỷ Cẩu", "Kẻ Ăn Hồn", "Cô Dâu Hào Môn"}

def write(name, fields, rows):
    with (OUT / name).open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)

def main():
    parser = argparse.ArgumentParser(description="Generate relational movie-platform CSV data.")
    parser.add_argument("--users", type=int, default=50_000)
    parser.add_argument("--movies", type=int, default=20_000)
    parser.add_argument("--ratings", type=int, default=500_000)
    parser.add_argument("--watches", type=int, default=1_000_000)
    args = parser.parse_args()
    if args.ratings > args.watches:
        parser.error("--ratings cannot exceed --watches")

    plans = ["Basic", "Standard", "Premium"]
    users = []
    for i in range(1, args.users + 1):
        activity = random.choices(["Active", "Casual", "Inactive"], weights=[35, 45, 20])[0]
        users.append({"user_id":f"U{i:05}", "age":random.randint(18,60), "gender":random.choice(["Male","Female","Other"]), "region":random.choice(REGIONS), "registration_date":(datetime(2025,1,1)+timedelta(days=random.randrange(600))).date(), "subscription_plan":random.choices(plans,weights=[45,40,15])[0], "activity_profile":activity})
    movies=[]
    adjectives = ["Last", "Neon", "Golden", "Broken", "Crimson", "Lost", "Midnight", "Silent", "Electric", "Wild"]
    nouns = ["Horizon", "Signal", "Journey", "Empire", "Echo", "Promise", "Storm", "Orbit", "Dream", "Secret"]
    for i in range(1,args.movies+1):
        if i <= len(RECENT_TITLES):
            title, g, year = RECENT_TITLES[i-1]
        else:
            g=GENRES[(i-1)%len(GENRES)]; year=random.choices(range(1995,2027),weights=[1]*20+[2]*8+[4]*4)[0]; title=f"{random.choice(adjectives)} {random.choice(nouns)} {i:04}"
        language="Vietnamese" if title in VIETNAMESE_TITLES else "English" if i <= len(RECENT_TITLES) else random.choice(["English","Korean","Japanese","Thai"])
        movies.append({"movie_id":f"M{i:05}","title":title,"genre":g,"release_year":year,"duration":random.randint(80,180),"content_type":random.choices(["Movie","Series"],weights=[85,15])[0],"language":language,"maturity_rating":random.choice(["P","T13","T16","T18"])})
    by_id={m['movie_id']:m for m in movies}
    genre_movies={g:[m for m in movies if m['genre']==g] for g in GENRES}
    # Unequal genre demand, title quality and user activity produce realistic non-flat charts.
    genre_weight = {"Action":30, "Drama":21, "Comedy":16, "Romance":12, "Sci-Fi":10, "Thriller":6, "Animation":3, "Horror":2}
    pref={u['user_id']:random.choices(GENRES,weights=[genre_weight[g] for g in GENRES])[0] for u in users}
    activity_users={kind:[u for u in users if u['activity_profile']==kind] for kind in ["Active","Casual","Inactive"]}
    recent_names={x[0] for x in RECENT_TITLES}
    quality={m['movie_id']:min(.98,random.betavariate(2.2,2.0)+(.45 if m['title'] in recent_names else 0)) for m in movies}
    ranked=sorted(movies,key=lambda m: (m['title'] in recent_names, quality[m['movie_id']]+({"Action":.12,"Drama":.07,"Comedy":.03}.get(m['genre'],0))),reverse=True)
    blockbusters=[m for m in movies if m['title'] in recent_names]
    hits=[m for m in ranked if m['title'] not in recent_names][:max(250,args.movies//40)]
    new_releases=[m for m in movies if m['release_year']>=2024]
    start=datetime(2026,4,1); watches=[]
    for i in range(1,args.watches+1):
        # Active viewers generate most events; inactive viewers create a visible long tail.
        profile=random.choices(["Active","Casual","Inactive"],weights=[72,24,4])[0]
        u=random.choice(activity_users[profile])
        days=random.randrange(180)
        day = start + timedelta(days=days)
        hour = random.choices(range(24), weights=[1]*7+[2]*5+[5]*5+[12]*5+[4]*2)[0]
        if day.weekday() >= 5 and random.random() < .45: hour = random.choice(range(18,24))
        roll=random.random()
        # Blockbusters lead the ranking; new titles accelerate in the latest 60 days.
        m=random.choice(genre_movies[pref[u['user_id']]] if roll<.43 else blockbusters if roll<.70 else hits if roll<.85 else new_releases if roll<.94 and days>120 else movies)
        ts=day+timedelta(hours=hour,minutes=random.randrange(60))
        completion=.50 + quality[m['movie_id']]*.35 + (.08 if u['subscription_plan']=='Premium' else 0)
        dur=max(5,min(m['duration'],int(random.gauss(m['duration']*completion,16))))
        watches.append({"watch_id":f"W{i:06}","user_id":u['user_id'],"movie_id":m['movie_id'],"watch_timestamp":ts.strftime('%Y-%m-%d %H:%M:%S'),"watch_duration":dur,"device":random.choice(DEVICES)})
    ratings=[]
    for i,w in enumerate(random.sample(watches,args.ratings),1):
        completion=w['watch_duration']/by_id[w['movie_id']]['duration']; q=quality[w['movie_id']]
        rating=min(5,max(1,round(1.5+q*2.4+completion*1.2+random.gauss(0,.55))))
        ratings.append({"rating_id":f"R{i:06}","user_id":w['user_id'],"movie_id":w['movie_id'],"rating":rating,"timestamp":w['watch_timestamp']})
    write('users.csv',list(users[0]),users); write('movies.csv',list(movies[0]),movies); write('watch_history.csv',list(watches[0]),watches); write('ratings.csv',list(ratings[0]),ratings)
    print(f'Created users={args.users:,}, movies={args.movies:,}, ratings={args.ratings:,}, watch_history={args.watches:,}')
if __name__ == '__main__': main()
