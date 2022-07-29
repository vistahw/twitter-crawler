import datetime
import time
import os
import requests
import traceback

from twittercrawler.crawlers import RecursiveCrawler
from twittercrawler.data_io import FileWriter, FileReader
from twittercrawler.search import get_time_termination, get_id_termination
from os.path import getmtime, isfile, join

def mkdir(path):
    path = path.strip()
    path = path.rstrip("\\")

    isExists = os.path.exists(path)

    if not isExists:
        os.makedirs(path)

        print (path + ' created')
        return True
    else:
        print (path + ' exist.')
        return False

def msg_push(content, key="SCT83383TVNPXK2INl9DAs04HNnFk9gxi",title="nftscan"):
    try:
        url = 'https://sc.ftqq.com/%s.send' % key
        requests.post(url, data={'text': title, 'desp': content})
    except Exception as e:
        traceback.format_exc()

# initialize
sender="1"
current_dir=os.path.abspath('.')
mkpath = join(current_dir, ".cache")
mkdir(mkpath)

FILE_PATH=os.path.join(current_dir,".cache","log_" +str(datetime.datetime.now().strftime("%m-%d")) + '.txt')
file_path = FILE_PATH
recursive = RecursiveCrawler()
recursive.authenticate("./api_key1.json")
keys = ["id_str","user","text"]
dellist = ['499672969','1347334157316321285','1336123093543215104','1514769101117304832','amazon']
recursive.connect_output([FileWriter(file_path, clear=False,  include_mask=keys, export_mask=dellist)])

# query
search_params = {
    "q": "nftgiveaway OR giveaway OR freemint OR whitelist OR Campaign OR nftdrop OR nftshill OR bounty OR grants OR airdrop OR oat OR earn OR gleam OR mintnow OR finalmint",
    "result_type": "recent",
    "lang": "en",
    "count": 100
}
recursive.set_search_arguments(search_args=search_params)

# termination (collect tweets from the last 5 minutes)
now = datetime.datetime.now()
time_str = (now - datetime.timedelta(seconds=300)).strftime("%a %b %d %H:%M:%S +0000 %Y")
print(time_str)
time_terminator = get_time_termination(time_str)

# run search - FIRST STAGE
success, max_id, latest_id, cnt = recursive.search(term_func=time_terminator)
print("\nFirst stage report:")
print(success, max_id, latest_id, cnt)

while True:
    # wait for 5 minutes
    print("\nSleeping for 5 minutes...")
    time.sleep(5 * 60)

    # NEW termination (collect only new tweets)
    id_terminator = get_id_termination(latest_id)

    # NEW search - SECOND STAGE
    success, new_max_id, new_latest_id, new_cnt = recursive.search(term_func=id_terminator)
    print("\nSecond stage report:")
    print(success, new_max_id, new_latest_id, new_cnt)
    latest_id= new_latest_id

    FILE_PATH = os.path.join(current_dir, ".cache", "log_" + str(datetime.datetime.now().strftime("%m-%d")) + '.txt')
    if file_path!=FILE_PATH:
        recursive.close()
        # load results
        results_df = FileReader(file_path).read()
        print("\nTotal hits:", len(results_df))
        print(results_df.loc[0])
        msg_push(str(results_df.loc[0]))

        file_path=FILE_PATH
        recursive.connect_output([FileWriter(file_path, clear=False, include_mask=keys)])
