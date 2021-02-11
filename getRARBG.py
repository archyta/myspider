#!/usr/bin/python3
# coding=utf-8
from random import random

import requests
from bs4 import BeautifulSoup
import os
import datetime, time
import pymongo
from bson.son import SON
import traceback
import sys
import threading

# domain = 'https://cl.cfbf.xyz'
from colorama import Fore

domain = 'https://rarbg.to'
# domamin = 'http://x1.pbnmdssb.xyz'

CODEPAGE = 'gbk'
winagent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36'
macagent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'

# http请求头
Hostreferer = {
    'Host': 'rarbg.to',
    'User-Agent': winagent,
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'Referer': 'https://rarbg.to/torrents.php?category=51&page=3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
}
proxies = {'http_proxy': 'http://192.168.31.98:2080',
           'https_proxy': 'https://192.168.31.98:2080',
           'http': 'http://192.168.31.98:2080',
           'https': 'http://192.168.31.98:2080',
           'ftp': 'http://192.168.31.98:2080',
           'socks5': 'http://192.168.31.98:1080',
           }

Cookies = {'tcc': "",
           'c_cookie': 'rgish6kxbw',
           'gaDts48g': 'q8h5pp9t',
           'aby': '2',
           "skt": "24XTOoxaj3",
           "gaDts48g": "q8h5pp9t"}

MONGODB_CONFIG = {
    'host': '192.168.31.98',
    'port': 32768,
    'db_name': 'rarbg',
    'username': None,
    'password': None
}

CATEGORY = {
    "4": "XXX (18+)",
    "14": "Movies/XVID",
    "48": "Movies/XVID/720",
    "17": "Movies/x264",
    "44": "Movies/x264/1080",
    "45": "Movies/x264/720",
    "47": "Movies/x264/3D",
    "42": "Movies/Full BD",
    "46": "Movies/BD Remux",
    "18": "TV Episodes",
    "41": "TV HD Episodes",
    "23": "Music/MP3",
    "25": "Music/FLAC",
    "27": "Games/PC ISO",
    "28": "Games/PC RIP",
    "40": "Games/PS3",
    "32": "Games/XBOX-360",
    "33": "Software/PC ISO",
    "35": "e-Books"
}


class Singleton(object):
    # 单例模式写法,参考：http://ghostfromheaven.iteye.com/blog/1562618
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            orig = super(Singleton, cls)
            cls._instance = orig.__new__(cls, *args, **kwargs)
        return cls._instance


class MongoConn(Singleton):
    def __init__(self):
        # connect db
        try:
            self.conn = pymongo.MongoClient(MONGODB_CONFIG['host'], MONGODB_CONFIG['port'])
            self.db = self.conn[MONGODB_CONFIG['db_name']]  # connect db
            self.username = MONGODB_CONFIG['username']
            self.password = MONGODB_CONFIG['password']
            if self.username and self.password:
                self.connected = self.db.authenticate(self.username, self.password)
            else:
                self.connected = True
        except Exception:
            print(traceback.format_exc())
            print('Connect Statics Database Fail.')
            sys.exit(1)


myclient = pymongo.MongoClient("mongodb://192.168.31.98:32768/")
db = myclient["novels"]

from queue import Queue
from threading import Thread

q = Queue()
num_of_workers = 1

counter = {}


def get_one_page():
    global domain, q, counter
    while True:
        d = q.get()

        added_time = d["added_time"]
        imdb_id = d["imdb_id"]
        title = d["title"]
        category = d["category"]
        url = domain + d["href"]
        page_html = requests.get(url, headers=Hostreferer, cookies=Cookies, proxies=proxies)
        bs = BeautifulSoup(page_html.text, "html.parser")
        infos = bs.find_all("tr")

        # content = bs.find_all("div", id="content")
        # if len(content) > 0 and len(content[0].contents) > 1:
        # t = content[0].contents[2]
        if len(bs) > 0:
            t = bs.text
            now = time.strftime("%y-%m-%d %H:%M:%S")
            if len(t) > 200:  # 小于200字的应该不是正文
                collection.find_one_and_update({"book": book, "sector": sector},
                                               update={"$set": {"book": book, "sector": sector,
                                                                "title": title, "url": domain + href,
                                                                "content": t, "updateTime": now}},
                                               upsert=True)
                print(
                    '\033[5;32;2m update %d \033[0m %s %s' % (title, threading.currentThread().getName(), url))
            else:
                print('%s TOO SHORT %d \033[0m back to q: %s %s' % (
                Fore.BLUE, title, threading.currentThread().getName(), url))
                counter[url] = counter.get(url, 0) + 1
                if counter[url] < 3:
                    q.put(d)
                else:
                    del (counter[url])
                time.sleep(random() * 8 + 7)
        else:
            print('%s Fail %d \033[0m back to q: %s %s' % (Fore.RED, sector, threading.currentThread().getName(), url))
            q.put(d)
            # 限流了，暂停片刻
            time.sleep(random() * 8 + 7)
        time.sleep(random() + 0.8)
    q.task_done()
    # time.sleep(11) if cnt == 40 else time.sleep(0.3)
    # cnt += 1


if __name__ == "__main__":
    myclient = pymongo.MongoClient("mongodb://192.168.31.98:32768/")
    movieShowDB = myclient["rarbg"]
    pages = movieShowDB["moviesShows"]
    last = movieShowDB["last"]

    for i in range(num_of_workers):
        worker = Thread(target=get_one_page)
        worker.setDaemon(True)
        worker.start()
    next_link = last.find_one({"lastest":"next_page"})
    start_url = domain + next_link["next_page"] if next_link else "https://rarbg.to/torrents.php"
    print("Page :%s" % start_url)
    start_html = requests.get(start_url, headers=Hostreferer, cookies=Cookies, proxies=proxies)
    start_html.encoding = CODEPAGE
    soup = BeautifulSoup(start_html.text, "html.parser")
    pager_links = soup.find_all("div", id='pager_links')
    next_link = pager_links[0].find_all("a")[-1].attrs["href"]
    movie_links = soup.find_all("tr", class_='lista2')

    while (movie_links):
        last.find_one_and_update({"lastest":"next_page"},
                                 update={"$set": {"next_page": next_link,
                                                                          "update_time": datetime.datetime.utcnow()}},
                                 upsert=True)
        for one in movie_links:
            achor = one.find_all("a")
            href = achor[0].attrs['href']
            category = href[href.rfind("=") + 1:]
            category = int(category) if category.isnumeric() else category
            uri = achor[1].attrs["href"]
            title = achor[1].attrs["title"]
            mouseover = achor[1].get("onmouseover", None)
            img_src = mouseover[mouseover.find("http"): mouseover.rfind("jpg") + 3] if mouseover else None
            imdb_link = achor[2].attrs["href"] if len(achor) > 2 else None
            imdb_id = imdb_link[imdb_link.rfind("=") + 1:] if imdb_link else None
            added_time_utc = one.contents[2].get_text()
            #added_time = time.mktime(time.strptime(added_time_utc, "%Y-%m-%d %H:%M:%S"))
            added_time = datetime.datetime.fromisoformat(added_time_utc) + datetime.timedelta(hours=-1)
            added_time_utc = added_time.strftime("%Y-%m-%d %H:%M:%S")
            added_time = added_time.timestamp()
            file_size = one.contents[3].get_text()
            seeders = one.contents[4].get_text()
            seeders = int(seeders) if seeders.isnumeric() else seeders
            leechers = one.contents[5].get_text()
            leechers = int(leechers) if leechers.isnumeric() else leechers
            uploader = one.contents[7].get_text()
            print("uri=%s, siz=%s, imdb=%s, <<%s>>\n" % (uri, file_size, imdb_id, title))
            #now = time.strftime("%Y-%m-%d %H:%M:%S")
            now = datetime.datetime.utcnow()
            pages.find_one_and_update({"uri": uri},
                                           update={"$set": {"uri": uri, "category": category,
                                                            "full_title": title, "thumbnail": img_src,"imdb_id":imdb_id,
                                                            "file_size":file_size,
                                                            "seeders":seeders,"leechers":leechers,"uploader":uploader,
                                                            "added_time": added_time,"added_time_utc":added_time_utc,
                                                            "update_time": now}},
                                           upsert=True)
            # q.put({"href": uri, "title": title,"category": category, "imdb_id": imdb_id, "added_time": added_time})
        if next_link:
            nextPage = requests.get(domain + next_link, headers=Hostreferer, cookies=Cookies, proxies=proxies)
            nextPage.encoding = CODEPAGE
            soup = BeautifulSoup(nextPage.text, "html.parser")
            pager_links = soup.find_all("div", id='pager_links')
            next_link = pager_links[0].find_all("a")[-1].attrs["href"]
            movie_links = soup.find_all("tr", class_='lista2')
            time.sleep(0.2)
        else:
            movie_links = None

    print('*** Main thread waiting')
    q.join()
    print('*** %s Done' % book)