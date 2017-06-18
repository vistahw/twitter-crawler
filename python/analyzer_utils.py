from search_utils import tweet_time_2_epoch

from pymongo import MongoClient
import networkx as nx
import pandas as pd
import numpy as np

from matplotlib import pyplot as plt
from bokeh.plotting import figure, show
from bokeh.palettes import Category20

### mongodb ###

def get_coll(coll_name,mongo_port=27017,mongo_db="twitter-crawler"):
    client = MongoClient('mongodb://localhost:%i/' % mongo_port)
    db = client[mongo_db]
    return db[coll_name], db

def find_some_docs(coll,sort_params=[('id',-1)],limit=10):
    res = coll.find().sort(sort_params).limit(limit)
    for item in res:
        print(item["id"],item["created_at"])
        
        
### mention network ###

def get_mentions(coll,limit=None,use_only_tweets=True):
    res = coll.find().limit(limit) if limit != None else coll.find()
    num_tweets, num_retweets = 0, 0
    users = {}
    edges = []
    for item in res:
        if use_only_tweets and "RT " == item['text'][:3]:
            num_retweets += 1
            continue
        num_tweets += 1
        src_id, epoch = item['user']['id_str'], int(tweet_time_2_epoch(item['created_at']))
        users[src_id] = item['user']['name']
        if 'user_mentions' in item['entities']:
            for mention in item['entities']['user_mentions']:
                trg_id = mention['id_str']
                users[trg_id] = mention['name']
                msg = item["text"]
                edges.append((epoch,src_id,trg_id,msg))
    return edges, users, num_tweets, num_retweets

def show_frequent_items(df,user_names,col,k=10):
    val_counts = pd.DataFrame(df[col].value_counts()[:k])
    frequent_users = [user_names[u_id] for u_id in val_counts.index]
    res_df = pd.DataFrame()
    res_df["id"] = val_counts.index
    res_df["name"] = frequent_users
    res_df["count"] = val_counts.values
    return res_df

def get_graph_stats(df):
    edges = list(zip(df["src"],df["trg"]))
    G = nx.DiGraph()
    G.add_edges_from(edges)
    N = G.number_of_nodes()
    M = G.number_of_edges()
    wc_comp = nx.number_weakly_connected_components(G)
    sc_comp = nx.number_strongly_connected_components(G)
    return (N,M,wc_comp,sc_comp)

### visualization ###

def filter_for_support(popularity_df, min_times=0, max_times=30):
    name_counts = popularity_df["name"].value_counts()
    filtered_names = list(name_counts[(name_counts >= min_times) & (name_counts <= max_times)].index)
    f_popular_trg_df = popularity_df[popularity_df["name"].isin(filtered_names)]
    return f_popular_trg_df
    
def plot_user_popularity(df, day_list):
    fig = plt.figure(figsize=(10,10))
    ax = fig.add_subplot(111)
    names_in_pop_order = list(df["name"].value_counts().index)
    for name in names_in_pop_order:
        if name == "Roland-Garros":
            continue
        item = df[df["name"]==name]
        x, y = item["day_idx"], item["count"]
        ax.plot(x,y,marker='x',markersize=10,label=name)
    plt.xticks(range(len(day_list)),day_list,rotation='vertical')
    plt.xlabel("days")
    #plt.legend()
    handles, labels = ax.get_legend_handles_labels()
    lgd = ax.legend(handles, labels, loc='upper center', bbox_to_anchor=(1.2,1.0))
    ax.grid('on')
    plt.show()

def stacked(df, categories):
    areas = dict()
    last = np.zeros(len(df[categories[0]]))
    for cat in categories:
        next = last + df[cat]
        areas[cat] = np.hstack((last[::-1], next))
        last = next
    return areas

def create_pivot(very_pop_df):
    pop_pivot_df = pd.pivot_table(very_pop_df,values="dominance",index=["day_idx"],columns=["name"])
    pop_pivot_df = pop_pivot_df.fillna(0.0)
    if "Roland-Garros" in pop_pivot_df.columns:
        del pop_pivot_df["Roland-Garros"]
    return pop_pivot_df

def plot_user_dominance(df):
    pop_pivot_df = create_pivot(df)
    index = pop_pivot_df.index
    categories = list(pop_pivot_df.columns)
    areas = stacked(pop_pivot_df, categories)
    colors = Category20[len(areas)]
    x2 = np.hstack((index[::-1], index))
    p = figure()
    p.grid.minor_grid_line_color = '#eeeeee'
    p.patches([x2] * len(areas), [areas[cat] for cat in categories],
          color=colors, alpha=0.8, line_color=None)
    show(p)
    return show_colors_for_users(categories, colors)

def show_colors_for_users(categories,colors):
    def color(col):
        return ["background-color: %s" % val for val in col]
    legend_df = pd.DataFrame(list(zip(categories,colors)),columns=["name","color"])
    return legend_df.style.apply(color)