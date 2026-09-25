from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title='Movie Analytics Platform', page_icon='🎬', layout='wide')
DATA = Path('data')

@st.cache_data
def load():
    users=pd.read_csv(DATA/'users.csv'); movies=pd.read_csv(DATA/'movies.csv'); ratings=pd.read_csv(DATA/'ratings.csv'); watches=pd.read_csv(DATA/'watch_history.csv',parse_dates=['watch_timestamp'])
    joined=watches.merge(movies,on='movie_id').merge(users,on='user_id')
    avg=ratings.groupby('movie_id',as_index=False).rating.mean().rename(columns={'rating':'avg_rating'})
    perf=joined.groupby(['movie_id','title','genre','duration'],as_index=False).agg(views=('watch_id','count'),watch_hours=('watch_duration',lambda v:round(v.sum()/60,1)),completion=('watch_duration','mean'))
    perf['completion_rate']=(perf.completion/perf.duration*100).round(1)
    return users,movies,ratings,watches,joined,perf.merge(avg,on='movie_id',how='left')

users,movies,ratings,watches,joined,perf=load()
st.title('🎬 Movie Streaming Analytics Platform')
page=st.sidebar.radio('Workspace',['Viewer','Analyst','Manager'])

if page == 'Viewer':
    st.subheader('Discover movies')
    query=st.text_input('Search title'); genre=st.selectbox('Genre',['All']+sorted(movies.genre.unique()))
    found=movies[movies.title.str.contains(query,case=False,na=False)]
    if genre != 'All': found=found[found.genre==genre]
    st.dataframe(found.merge(perf[['movie_id','views','avg_rating']],on='movie_id',how='left').sort_values('views',ascending=False),hide_index=True,use_container_width=True)
    a,b=st.columns(2)
    with a:
        st.subheader('Trending movies'); st.dataframe(perf.nlargest(10,'views')[['title','genre','views','avg_rating']],hide_index=True,use_container_width=True)
    with b:
        uid=st.selectbox('Recommendations for',users.user_id)
        favourite=joined[joined.user_id==uid].genre.value_counts().head(2).index
        st.subheader('Personalized recommendations'); st.dataframe(perf[perf.genre.isin(favourite)].nlargest(8,'avg_rating')[['title','genre','avg_rating','views']],hide_index=True,use_container_width=True)
elif page == 'Analyst':
    st.subheader('Analyst dashboard')
    a,b,c,d=st.columns(4); a.metric('Users',len(users)); b.metric('Movies',len(movies)); c.metric('Views',f'{len(watches):,}'); d.metric('Average rating',f'{ratings.rating.mean():.2f} / 5')
    genre=joined.groupby('genre',as_index=False).watch_id.count().rename(columns={'watch_id':'views'}).sort_values('views',ascending=False)
    a,b=st.columns(2)
    a.plotly_chart(px.bar(genre,x='genre',y='views',color='genre',title='Genre popularity',template='plotly_white'),use_container_width=True)
    # A linear axis hides long-tail movies when blockbusters have thousands of views.
    performance=perf[perf.views >= perf.views.quantile(.70)]
    chart=px.scatter(performance,x='views',y='avg_rating',color='genre',hover_name='title',hover_data=['completion_rate','watch_hours'],log_x=True,opacity=.65,title='Movie performance — top 30% by views (log scale)',template='plotly_white')
    chart.update_layout(xaxis_title='Views (log scale)',yaxis_title='Average rating',legend_title='Genre')
    b.plotly_chart(chart,use_container_width=True)
    engagement=joined.groupby('user_id',as_index=False).watch_id.count().rename(columns={'watch_id':'views'})
    a,b=st.columns(2)
    a.plotly_chart(px.histogram(engagement,x='views',nbins=30,title='User engagement distribution'),use_container_width=True)
    by_plan=joined.groupby('subscription_plan',as_index=False).watch_id.count().rename(columns={'watch_id':'views'})
    b.plotly_chart(px.pie(by_plan,names='subscription_plan',values='views',title='Views by subscription plan'),use_container_width=True)
    hourly=joined.assign(hour=joined.watch_timestamp.dt.hour).groupby('hour',as_index=False).watch_id.count().rename(columns={'watch_id':'views'})
    st.plotly_chart(px.bar(hourly,x='hour',y='views',title='Viewing hours: evening peak'),use_container_width=True)
else:
    st.subheader('Manager dashboard')
    st.write('Low-engagement movies'); st.dataframe(perf.nsmallest(10,'views')[['title','genre','views','completion_rate']],hide_index=True,use_container_width=True)
    engagement=joined.groupby('user_id').watch_id.count(); segments=pd.cut(engagement,bins=[-1,10,35,float('inf')],labels=['Inactive','Casual','Active']).value_counts().reset_index(); segments.columns=['segment','users']
    a,b=st.columns(2); a.plotly_chart(px.pie(segments,names='segment',values='users',title='User segments'),use_container_width=True)
    trend=joined.assign(day=joined.watch_timestamp.dt.date).groupby('day',as_index=False).watch_id.count().rename(columns={'watch_id':'views'}); b.plotly_chart(px.line(trend,x='day',y='views',title='Daily viewing trend'),use_container_width=True)
