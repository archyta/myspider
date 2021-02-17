#!/usr/bin/python3
# coding=utf-8

import requests
from bs4 import BeautifulSoup
import datetime, time

# domain = 'https://cl.cfbf.xyz'
from utils import *


def dealMovieBasic(one):
    pages = movieShowDB["moviesShows"]
    anchor = one.find_all("a")
    href = anchor[0].attrs['href'].strip()
    category = href[href.rfind("=") + 1:]
    category = int(category) if category.isnumeric() else category
    uri = anchor[1].attrs["href"]
    title = anchor[1].attrs["title"].strip()
    mouseover = anchor[1].get("onmouseover", None)
    img_src = mouseover[mouseover.find("http"): mouseover.rfind("jpg") + 3] if mouseover else None
    imdb_link = anchor[2].attrs["href"] if len(anchor) > 2 and anchor[2].attrs["href"].find("imdb=tt") >0 else None
    imdb_id = imdb_link[imdb_link.rfind("imdb=tt") + 5:] if imdb_link and imdb_link.rfind("imdb=tt") >0 else None  # imdb=tt12345678
    if imdb_id and len(imdb_id) > 15:
        print("Error imdb=%s, link=%s, uri=%s" % (imdb_id, imdb_link, uri) )
    tv_link = anchor[3].attrs["href"] if len(anchor) > 3 and anchor[3].attrs["href"].find("/tv/") >0 else None
    tv_id = tv_link[tv_link.rfind("tv/") + 3:-1] if tv_link else None  # /tv/tt12345678/
    added_time_utc = one.contents[2].get_text().strip()
    added_time_utc = datetime.datetime.fromisoformat(added_time_utc) + datetime.timedelta(hours=-1) #服务器应该在东一区
    added_time = int(added_time_utc.timestamp())
    file_size = convertSize2MB(one.contents[3].get_text().strip())
    seeders = one.contents[4].get_text().strip()
    seeders = int(seeders) if seeders.isnumeric() else seeders
    leechers = one.contents[5].get_text().strip()
    leechers = int(leechers) if leechers.isnumeric() else leechers
    uploader = one.contents[7].get_text().strip()
    print("uri=%s, siz=%s, imdb=%s, <<%s>>\n" % (uri, file_size, imdb_id, title))
    # now = time.strftime("%Y-%m-%d %H:%M:%S")
    now = datetime.datetime.utcnow()
    pages.find_one_and_update({"uri": uri},
                              update={"$set": {"uri": uri, "category": category,
                                               "file_name": title, "thumbnail": img_src, "imdb_id": imdb_id,
                                               "imdb_rarbg": imdb_link, "tv_link": tv_link, "tv_id": tv_id,
                                               "file_size(MB)": file_size,
                                               "seeders": seeders, "leechers": leechers, "uploader": uploader,
                                               "info_level": 10,
                                               "added_time": added_time, "added_time_utc": added_time_utc,
                                               "update_time": now}},
                              upsert=True)
    return imdb_id


def dealImdbBase(imdb_id):
    if imdb_id:
        imdb = movieShowDB["imdb"]
        if not imdb.find_one({"imdb_id": imdb_id}):
            imdb.insert_one( {"imdb_id": imdb_id, "update_time": datetime.datetime.utcnow(),
                                                  "info_level": 10,
                                                  "rarbg_link": "/torrents.php?imdb=%s" % imdb_id})


if __name__ == "__main__":
    #myclient = pymongo.MongoClient("mongodb://192.168.31.98:32768/")
    movieShowDB = myclient["rarbg"]
    last = movieShowDB["last"]

    next_link = last.find_one({"lastest":"next_page"})
    start_url = DOMAIN + next_link["next_page"] if next_link else "https://rarbg.to/torrents.php?category=4;14;48;17;44;45;47;50;51;52;42;46;54;18;41;49&search=&order=data&by=ASC"
    #start_url = "https://rarbg.to/torrents.php?category=2%3B18%3B41%3B49&page=3"
    print("Page :%s" % start_url)
    start_html = requests.get(start_url, headers=Hostreferer, cookies=Cookies, proxies=PROXIES)
    start_html.encoding = CODEPAGE
    soup = BeautifulSoup(start_html.text, "html.parser")
    pager_links = soup.find_all("div", id='pager_links')
    if not  pager_links :
        print(soup.text)
        exit(1)
    next_link = pager_links[0].find_all("a")[-1].attrs["href"]
    movie_links = soup.find_all("tr", class_='lista2')

    while (movie_links):
        last.find_one_and_update({"lastest":"next_page"},
                                 update={"$set": {"next_page": next_link,
                                                                          "update_time": datetime.datetime.utcnow()}},
                                 upsert=True)
        for one in movie_links:
            imdb_id = dealMovieBasic(one)
            dealImdbBase(imdb_id)

            # q.put({"uri": uri, "title": title,"category": category, "imdb_id": imdb_id, "added_time": added_time})
        if next_link:
            nextPage = requests.get(DOMAIN + next_link, headers=Hostreferer, cookies=Cookies, proxies=PROXIES)
            nextPage.encoding = CODEPAGE
            soup = BeautifulSoup(nextPage.text, "html.parser")
            pager_links = soup.find_all("div", id='pager_links')
            next_link = pager_links[0].find_all("a")[-1].attrs["href"]
            movie_links = soup.find_all("tr", class_='lista2')
            time.sleep(0.8)
        else:
            movie_links = None

