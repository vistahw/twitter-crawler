import datetime
import time
import os
import requests
import traceback

from twittercrawler.crawlers import RecursiveCrawler
from twittercrawler.data_io import FileWriter, FileReader
from twittercrawler.search import get_time_termination, get_id_termination
from os.path import getmtime, isfile, join

_day = ""
sender = 1
maxsender = 5


def mkdir(path):
    path = path.strip()
    path = path.rstrip("\\")

    isExists = os.path.exists(path)

    if not isExists:
        os.makedirs(path)

        print(path + ' created')
        return True
    else:
        print(path + ' exist.')
        return False


def msg_push(content, key="SCT83383TVNPXK2INl9DAs04HNnFk9gxi", title="nftscan"):
    try:
        url = 'https://sc.ftqq.com/%s.send' % key
        requests.post(url, data={'text': title, 'desp': content})
    except Exception as e:
        traceback.format_exc()


def isnewday():
    global _day
    latest_day = str(datetime.datetime.now().strftime("%m-%d"))
    if _day != latest_day:
        print(f"Day changed, old day is {_day}, and latestday is {latest_day}")
        _day = latest_day
        return True
    else:
        return False


def search_start(file_path):
    recursive = RecursiveCrawler()
    recursive.authenticate("./api_key1.json")
    keys = ["id_str", "user", "text"]
    userdellist = ['499672969', '1347334157316321285', '1336123093543215104', '1514769101117304832', 'amazon']
    recursive.connect_output([FileWriter(file_path, clear=False, include_mask=keys, export_mask=userdellist)])

    # query
    search_params = {
        "q": "nftgiveaway OR giveaway OR freemint OR whitelist OR #Campaign OR nftdrop OR nftshill OR #bounty OR #grants OR airdrop OR oat OR #earn OR gleam OR mintnow OR finalmint",
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
    print("\n001-First stage report:")
    print(success, max_id, latest_id, cnt)

    while True:
        # wait for 5 minutes
        print("\n002-Sleeping for 5 minutes...")
        time.sleep(5 * 60)

        # NEW termination (collect only new tweets)
        id_terminator = get_id_termination(latest_id)

        # NEW search - SECOND STAGE
        success, new_max_id, new_latest_id, new_cnt = recursive.search(term_func=id_terminator)
        print("004-Second stage report:", success, new_max_id, new_latest_id, new_cnt)
        latest_id = new_latest_id
        changeSender(recursive)

        if isnewday():
            recursive.close()
            count = len(open(file_path, 'r').readlines())
            msg_push(f"{file_path} generated. {str(count)}, data recorded.")
            break


def changeSender(recursive):
    print("changeSender")
    recursive.twitter_api.client.close()
    global sender, maxsender
    current_time = time.time()
    _, wait_for = recursive._check_remaining_limit(recursive.twitter_api, current_time)
    if wait_for > 0:
        print("Sender=", sender, "RATE LIMIT RESET in %.1f seconds" % wait_for)

    sender += 1
    if sender > maxsender:
        if wait_for > 0:
            print("Send5 sleep for %.1f seconds" % wait_for)
            time.sleep(wait_for)
        sender = 1

    filename = f"./api_key{sender}.json"
    print(f'sender switched {filename}')
    recursive.authenticate(filename)
    return


if __name__ == '__main__':
    current_dir = os.path.abspath('.')
    mkpath = join(current_dir, ".cache")
    mkdir(mkpath)
    _day = str(datetime.datetime.now().strftime("%m-%d"))
    while True:
        FILE_PATH = os.path.join(current_dir, ".cache",
                                 "log_" + str(datetime.datetime.now().strftime("%m-%d")) + '.txt')
        print(f"New search started, as fileName={FILE_PATH}")
        search_start(FILE_PATH)
