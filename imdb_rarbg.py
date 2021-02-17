import datetime
import time
from random import random

import requests
import re
from bs4 import BeautifulSoup

from utils import *

def query_by_imdbid(uri):
    url = DOMAIN + uri
    page_html = requests.get(url, headers=Hostreferer, cookies=Cookies, proxies=PROXIES)
    text = page_html.text.replace("S.", "Seeders").replace("L.", "Leechers").replace("<br/>","").replace("<br />","")
    bs = BeautifulSoup(text, "html.parser")

    imdb_basic = bs.find_all("h1")[0].find_next().find_all("td")
    imdb_id = uri[uri.rfind("imdb=tt") + 5:] if uri else None  # imdb=tt12345678
    imdb_link = "https://www.imdb.com/title/%s/" % imdb_id
    mydict = {"uri":uri}
    mydict["imdb_link"] = imdb_link
    mydict["imdb_id"] = imdb_id

    key = value = ""
    if len(imdb_basic) > 1:  #应该有2列，poster 和 基本信息
        if imdb_basic[0].find("img"):
            mydict["poster"] = imdb_basic[0].find("img")["src"]
        if imdb_basic[1].select("b"):
            for x in imdb_basic[1].select("b"):
                key = x.text.lower().strip().strip(":")
                value = x.nextSibling.strip().strip(":")
                if key == "actors" or key == "genres" or key == "directed by":
                    key = "director" if key == "directed by" else key
                    guys = []
                    guy = x.find_next()
                    while guy and guy.get("href",None):
                        guys.append(guy.text.strip())
                        guy = guy.find_next()
                    value = ",".join(guys)
                    mydict[key] = value
                elif key == "year":
                    value = int(value)
                    mydict[key] = value
                elif key == "runtime":
                    key = "imdb_runtime"
                    value = float(value)
                    mydict[key] = value
                elif key == "imdb rating":
                    key = "imdb_rating"
                    value = float(value.split("/")[0])
                    mydict[key] = value
                elif key == "rottentomatoes":
                    key = "rotten_tomatoes_fresh"
                    value = float(x.find_next().nextSibling.strip().strip("%")) / 100
                    mydict[key] = value
                    key = "rotten_tomatoes_like"
                    value = float(x.find_next().find_next().nextSibling.strip().strip("%")) / 100
                    mydict[key] = value
                else:
                    mydict[key] = value


        # # soup解析有误，转换成字符串来解析
        # infos = imdb_basic[1].text.strip().replace("IMDB Rating","IMDB_Rating").replace("Directed by","director")
        # keys = [x for x in re.findall(r"\S\w+:",infos)]
        # for x in keys:
        #     infos = infos.replace(x, ":")
        # infos = infos.strip(":").strip()
        # values = [x.strip() for x in infos.split(":")]
        # for i in range(len(keys)):
        #     key = keys[i].lower().strip(":")
        #     value = ",".join([x.strip() for x in values[i].split(",")])
        #     if key == "year":
        #         value = int(value)
        #     elif key == "runtime":
        #         key = "imdb_runtime"
        #         value = int(value)
        #     elif key == "imdb_rating":
        #         value = float(value.split("/")[0])
        #     mydict[key] = value

    now = datetime.datetime.utcnow()
    mydict["update_time"] = now
    mydict["info_level"] = 20
    mydict["rarbg_link"] = "/torrents.php?imdb=%s" % imdb_id

    imdb = movieShowDB["imdb"]
    record = imdb.find_one_and_update({"imdb_id": imdb_id},
                          update={"$set": mydict},
                          upsert=True)
    print(mydict)


def deal_others_table(others):
    movies = []
    for row in others[0].contents:
        tx = []
        for col in row.contents:
            tx.append(col.text.strip())
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
            media[keys[j]] = movies[i][j]
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

    movieShowDB = myclient["rarbg"]
    imdb = movieShowDB["imdb"]
    #query_by_imdbid("/torrents.php?imdb=tt0061839")
    query_by_imdbid("/torrents.php?imdb=tt0146455")

    cursor  = imdb.find({"info_level":{"$lt": 20},"imdb_id":{"$ne":None}})
    # cursor = pages.find({"uri": '/torrent/ezg13bd'})   # for test only.
    for item in cursor:
        query_by_imdbid("/torrents.php?imdb=%s" % item["imdb_id"])
        time.sleep(random() + 0.3)

