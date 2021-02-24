#!/usr/bin/python3
# coding=utf-8
import datetime
import time
from random import random

import requests
from bs4 import BeautifulSoup

# domain = 'https://cl.cfbf.xyz'
from utils import *


def find_movie_info(movie):
    pages = movieShowDB["moviesShows"]
    anchor = movie.find_all("a")
    href = anchor[0].attrs['href'].strip()
    category = href[href.rfind("=") + 1:]
    category = int(category) if category.isnumeric() else category
    uri = anchor[1].attrs["href"]
    title = anchor[1].attrs["title"].strip()
    mouseover = anchor[1].get("onmouseover", None)
    img_src = mouseover[mouseover.find("http"): mouseover.rfind("jpg") + 3] if mouseover else None
    imdb_link = anchor[2].attrs["href"] if len(anchor) > 2 and anchor[2].attrs["href"].find("imdb=tt") > 0 else None
    tittle_id = imdb_link[imdb_link.rfind("imdb=tt") + 5:] if imdb_link and imdb_link.rfind(
        "imdb=tt") > 0 else None  # imdb=tt12345678
    if tittle_id and len(tittle_id) > 15:
        print("Error imdb=%s, link=%s, uri=%s" % (tittle_id, imdb_link, uri))
    tv_link = anchor[3].attrs["href"] if len(anchor) > 3 and anchor[3].attrs["href"].find("/tv/") > 0 else None
    tv_id = tv_link[tv_link.rfind("tv/") + 3:-1] if tv_link else None  # /tv/tt12345678/
    added_time_utc = movie.contents[2].get_text().strip()
    added_time_utc = datetime.datetime.fromisoformat(added_time_utc) + datetime.timedelta(hours=-1)  # 服务器应该在东一区
    added_time = int(added_time_utc.timestamp())
    file_size = convertSize2MB(movie.contents[3].get_text().strip())
    seeders = movie.contents[4].get_text().strip()
    seeders = int(seeders) if seeders.isnumeric() else seeders
    leechers = movie.contents[5].get_text().strip()
    leechers = int(leechers) if leechers.isnumeric() else leechers
    uploader = movie.contents[7].get_text().strip()
    print("imdb=%s, <<%s>>, cate=%s, size=%s, uri=%s\n" % (tittle_id, title, CATEGORY["%d" % category], file_size, uri))
    # now = time.strftime("%Y-%m-%d %H:%M:%S")
    now = datetime.datetime.utcnow()
    pages.find_one_and_update({"uri": uri},
                              update={"$set": {"uri": uri, "category": category,
                                               "file_name": title, "thumbnail": img_src, "imdb_id": tittle_id,
                                               "imdb_rarbg": imdb_link, "tv_link": tv_link, "tv_id": tv_id,
                                               "file_size(MB)": file_size,
                                               "seeders": seeders, "leechers": leechers, "uploader": uploader,
                                               "info_level": 10,
                                               "added_time": added_time, "added_time_utc": added_time_utc,
                                               "update_time": now}},
                              upsert=True)
    return tittle_id


def deal_imdb(tittle_id):
    if tittle_id:
        imdb = movieShowDB["imdb"]
        if not imdb.find_one({"imdb_id": tittle_id}):
            imdb.insert_one({"imdb_id": tittle_id, "update_time": datetime.datetime.utcnow(),
                             "info_level": 10,
                             "rarbg_link": "/torrents.php?imdb=%s" % tittle_id})


if __name__ == "__main__":
    # myclient = pymongo.MongoClient("mongodb://192.168.31.98:32768/")
    movieShowDB = myclient["rarbg"]
    last = movieShowDB["last"]

    next_link = last.find_one({"lastest": "next_page"})
    start_url = 'https://rarbg.to/torrents.php?category=4;14;48;17;44;45;47;50;51;52;42;46;54;18;41;49&search=&order=data&by=ASC' if not next_link \
        else DOMAIN + next_link["next_page"]
    # start_url = "https://rarbg.to/torrents.php?category=2%3B18%3B41%3B49&page=3"
    print("Page :%s" % start_url)
    start_html = requests.get(start_url, headers=Hostreferer, cookies=Cookies, proxies=PROXIES)
    start_html.encoding = CODEPAGE
    soup = BeautifulSoup(start_html.text, "html.parser")
    pager_links = soup.find_all("div", id='pager_links')
    if not pager_links:
        print(soup.text)
        exit(1)
    next_link = pager_links[0].find_all("a")[-1].attrs["href"]
    movie_links = soup.find_all("tr", class_='lista2')

    while movie_links:
        last.find_one_and_update({"lastest": "next_page"},
                                 update={"$set": {"next_page": next_link,
                                                  "update_time": datetime.datetime.utcnow()}},
                                 upsert=True)
        for one in movie_links:
            imdb_id = find_movie_info(one)
            deal_imdb(imdb_id)

            # q.put({"uri": uri, "title": title,"category": category, "imdb_id": imdb_id, "added_time": added_time})
        if next_link:
            nextPage = requests.get(DOMAIN + next_link, headers=Hostreferer, cookies=Cookies, proxies=PROXIES)
            nextPage.encoding = CODEPAGE
            soup = BeautifulSoup(nextPage.text, "html.parser")
            pager_links = soup.find_all("div", id='pager_links')
            next_link = pager_links[0].find_all("a")[-1].attrs["href"]
            movie_links = soup.find_all("tr", class_='lista2')
            time.sleep(1.3 + random())
        else:
            movie_links = None
