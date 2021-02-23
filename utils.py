import sys
import traceback

import pymongo

DOMAIN = 'https://rarbg.to'
#DOMAIN = 'https://rarbgprx.org'

# domamin = 'http://x1.pbnmdssb.xyz'

CODEPAGE = 'gbk'
WINAGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36'
MACAGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'

# http请求头
Hostreferer = {
    'Host': 'rarbg.to',
    'User-Agent': WINAGENT,
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
PROXIES = {'http_proxy': 'http://192.168.31.98:2080',
           'https_proxy': 'https://192.168.31.98:2080',
           'http': 'http://192.168.31.98:2080',
           'https': 'http://192.168.31.98:2080',
           'ftp': 'http://192.168.31.98:2080',
           'socks5': 'http://192.168.31.98:1080',
           }

Cookies = {'tcc': "",
           'c_cookie': 'e9a1uz0o2l',
           'gaDts48g': 'q8h5pp9t',
           'aby': '2',
           "skt": "BPTZN5c84o",
           "gaDts48g": "q8h5pp9t",
           "expla":4}

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

INFO_LEVELS={
    "10","BASIC_INFO",
    "20","DETAIL_INFO",
    "25","MEDIA_INFO",
    "30","LINK_DOWNLOADED",
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


def convertSize2MB(file_size):
    size = None
    a2 = file_size.split(" ")
    if len(a2) > 1:
        size = a2[0]
        b = a2[-1].upper()
        if b == 'TB':
            size = 1024 * 1024 * float(size)
        elif b == 'GB':
            size = 1024 * float(size)
        elif b == 'MB':
            size = float(size)
        elif b == 'KB':
            size = float(size) / 1024
        elif b == 'B':
            size = float(size) / 1024 / 1024
    return size