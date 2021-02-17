import datetime
import time
from random import random

import requests
import re
from bs4 import BeautifulSoup

from utils import *

def get_one_page(uri):
    url = DOMAIN + uri
    page_html = requests.get(url, headers=Hostreferer, cookies=Cookies, proxies=PROXIES)
    text = page_html.text.replace("S.", "Seeders").replace("L.", "Leechers").replace("<span>", "").replace("</span>","")
    bs = BeautifulSoup(text, "html.parser")

    items = bs.find_all("td",align="right",class_="header2")
    mydict = {"uri":uri}
    for myitem in items:
        key = myitem.text.lower().replace(":", "").strip()
        value = ""
        if key == "torrent":
            tmp = myitem.find_next()
            if len(tmp.contents) > 2:
                mydict["torrent_rarbg"] = value = tmp.contents[2]["href"]
            if len(tmp.contents) > 4:
                mydict["magnet"] = value = tmp.contents[4]["href"]
        elif key == "poster":
            mydict[key] = value = myitem.nextSibling()[0]["src"]
        elif key == "vpn":
            key = "vpn"  # skip VPN
        elif key == "subsbeta":
            key = "subtitles"
            value = []
            for sub in  myitem.nextSibling.find_all("a"):
                href = sub["href"]
                over = sub["onmouseover"]
                if over and over.find("Download"):
                    subtitle_name = over[over.find("Download") + 9:over.rfind("\\") - 1].strip()
                    value.append("%s:%s"%(subtitle_name,href))
            mydict[key] = value
        elif key == "others":
            others = myitem.find_next().select("tbody")
            deal_others_table(others)  # Others字段, 可下载的其他文件列表
        elif key == "trailer":
            if len(tmp.contents) > 1:
                mydict["trailer_rarbg"] = value = tmp.contents[2]["href"]
        elif key == "category":
            mydict["category_name"] = value = myitem.find_next().contents[0].text.strip()
            href = myitem.find_next().contents[0]["href"]
            if href:
                mydict[key] = int(href[href.rfind("=")+1:])
        elif key == "" and myitem.find("img"):
            key = "imdb_link"
            mydict[key] = value = myitem.nextSibling()[0]["href"]
        elif key == "genres" or key == "actors" or key == "director" or key == "tags" :
            mydict[key] = value = ",".join([x.text for x in myitem.nextSibling()])
        elif key == "year":
            mydict[key] = value = int(myitem.find_next().text.strip())
        elif key == "imdb runtime":
            mydict["imdb_runtime"] = value = int(myitem.find_next().text)
        elif key == "imdb rating":
            value = myitem.find_next().text.lower().strip()
            if value and value.find("/"):
                mydict["imdb_rating"] = float(value[:value.find("/")])
                if value.find("user") > 0:
                    mydict["imdb_votes"] = int(value[value.find("from") + 5:value.find("users")])
                mydict["imdb_updated"] = datetime.datetime.fromisoformat(value[value.find(":") + 1:].strip())
        elif key == "hit&run":
            value = myitem.find_next().text.strip()
            key = "hit_run"
            mydict[key] = float(value[:value.find("%")].strip()) / 100
        elif key == "peers":
            value = myitem.find_next().text.strip()
            if value and value.find(":"):
                mydict["seeders"] = int(value[value.find(":")+2:value.find(",")-1])
                mydict["leechers"] = int(value[value.rfind(":")+2:value.rfind("=")-1])
        elif key == "release name":
            key = "file_name"
            mydict[key] = value = myitem.find_next().text.strip()
        elif key == "size":
            key = "file_size(MB)"
            mydict[key] = value = convertSize2MB(myitem.find_next().text.strip())
        elif key == "added":
            value = myitem.find_next().text
            added_time_utc = datetime.datetime.fromisoformat(value) + datetime.timedelta(hours=-1)
            added_time = int(added_time_utc.timestamp())
            mydict["added_time"] = added_time
            mydict["added_time_utc"] = added_time_utc
        else:
            mydict[key] = value = myitem.find_next().text.strip()
        print("%s = %s"%(key, value))
    now = datetime.datetime.utcnow()
    mydict["update_time"] = now
    mydict["info_level"] = 20

    movieShowDB = myclient["rarbg"]
    pages = movieShowDB["moviesShows"]
    pages.find_one_and_update({"uri": uri},
                          update={"$set": mydict},
                          upsert=True)


def deal_others_table(others):
    movies = []
    for row in others[0].contents:
        tx = []
        for col in row.contents:
            tx.append(col.text)
            if (col.text == "Name"):
                tx.append("uri")
            if (col.select('a[href]')):
                tx.append(col.contents[0]['href'])
        movies.append(tx)
    movieShowDB = myclient["rarbg"]
    pages = movieShowDB["moviesShows"]
    keys = [x.lower().strip() for x in movies[0]]
    media = {}
    for i in range(1, len(movies)):
        for j in range(len(movies[i])):
            media[keys[j]] = movies[i][j].strip()
        media["seeders"] = int(media.get("seeders", 0))
        media["leechers"] = int(media.get("leechers", 0))
        media["update_time"] = datetime.datetime.utcnow()
        media["file_size"] = convertSize2MB(media["size"])
        del (media["size"])
        media["file_name"] = media["name"]
        del (media["name"])

        pages.find_one_and_update({"uri": media["uri"]},
                                  update={"$set": media},
                                  upsert=True)


if __name__ == "__main__":
    #myclient = pymongo.MongoClient("mongodb://192.168.31.98:32768/")
    #media_url = "http://rarbgproxy.org/torrent/4zreh1yu6cxpbmf95v7jatsn2l3wqdgk8oixtyaozbp5c17k3i98jwsu4dnlgeqmhrvf62"
    media_url = "https://rarbgprx.org/torrent/ephbqo4"

    #get_one_page("/torrent/qpz6ysj")
    movieShowDB = myclient["rarbg"]
    pages = movieShowDB["moviesShows"]

    cursor  = pages.find({"info_level":{"$lt": 20},"imdb_id":{"$ne":None}})
    # cursor = pages.find({"uri": '/torrent/ezg13bd'})   # for test only.
    for item in cursor:
        print(item)
        get_one_page(item["uri"])
        time.sleep(random() + 0.3)

