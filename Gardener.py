import argparse
import pickle
import logging
import os.path
import requests
import re
import yaml
import sqlite3
import time
from datetime import datetime
from bs4 import BeautifulSoup as bs
from getpass import getpass
from urllib.parse import urljoin, unquote

class GardenerException(Exception): pass
class InvalidUserParameters(GardenerException): pass
class CannotWriteUserParameters(GardenerException): pass


class Torrent:

    name = "torrents"

    def __init__(self, torrent_id, torrent_ptid, torrent_title, torrent_file='', t_add=None,
                 pattern_id=0, t_start=None, t_complete=None, t_remove=None):
        self.torrent_id = torrent_id
        self.torrent_ptid = torrent_ptid
        self.torrent_title = torrent_title
        self.torrent_file = torrent_file
        self.t_add = datetime.now()
        self.pattern_id = pattern_id
        self.t_start = None
        self.t_complete = None
        self.t_remove = None


    @classmethod
    def load_db(cls, db_conn, schema, foreign_keys=dict()):
        c = db_conn.cursor()
        s_schema = ", ".join(["{} {}".format(col["name"], col["dbtype"]) for col in schema])
        if foreign_keys:
            s_schema += ","
            s_schema += ",".join("FOREIGN KEY ({}) REFERENCES {}".format(k, v) for k, v in foreign_keys.items())
        cmd = "CREATE TABLE IF NOT EXISTS {} ({})".format(cls.name, s_schema)
        logging.info(cmd)
        c.execute(cmd)
        db_conn.commit()
        db_conn.row_factory = sqlite3.Row
        db_cols = ",".join([col["name"] for col in schema])
        cmd = "SELECT {} FROM {}".format(db_cols, cls.name)
        logging.info(cmd)
        torrents = [Torrent(*r) for r in c.execute(cmd).fetchall()]
        return torrents


    def update_db(self, db_conn):
        cols = ("torrent_ptid", "torrent_title", "torrent_file", "t_add",
                "pattern_id", "t_start", "t_complete", "t_remove")
        c = db_conn.cursor()
        if self.torrent_id == 0:
            cmd = "INSERT INTO {name} ({schema}) VALUES ({qmarks})".format(
                name=self.name,
                schema=",".join(cols),
                qmarks=",".join(["?"]*len(cols)))
            args = (self.torrent_ptid, self.torrent_title, self.torrent_file, self.t_add,
                    self.pattern_id, self.t_start, self.t_complete, self.t_remove)
        else:
            cmd = "UPDATE {name} SET {schema} WHERE {key}=?".format(
                name=self.name,
                schema=",".join(["{}=?".format(c) for c in cols]),
                key="torrent_id")
            args = (self.torrent_ptid, self.torrent_title, self.torrent_file, self.t_add,
                    self.pattern_id, self.t_start, self.t_complete, self.t_remove, self.torrent_id)
        logging.info(cmd + ' ' + ' '.join(map(str, args)))
        c.execute(cmd, args)
        self.torrent_id = (self.torrent_id if self.torrent_id else c.lastrowid)
        db_conn.commit()


class Pattern:

    name = "patterns"

    def __init__(self, pattern_id, value, t_add, t_remove=None):
        self.pattern_id = pattern_id
        self.value = value
        self.t_add = t_add
        self.t_remove = t_remove


    @classmethod
    def load_db(cls, db_conn, schema, foreign_keys=dict()):
        c = db_conn.cursor()
        s_schema = ",".join(["{} {}".format(col["name"], col["dbtype"]) for col in schema])
        if foreign_keys:
            s_schema += ","
            s_schema += ",".join("FOREIGN KEY ({}) REFERENCES {}".format(k, v) for k, v in foreign_keys.items())
        cmd = "CREATE TABLE IF NOT EXISTS {} ({})".format(cls.name, s_schema)
        logging.info(cmd)
        c.execute(cmd)
        db_conn.commit()
        db_conn.row_factory = sqlite3.Row
        s_schema = ",".join([col["name"] for col in schema])
        cmd = "SELECT {} FROM {}".format(s_schema, cls.name)
        logging.info(cmd)
        patterns = [Pattern(*r) for r in c.execute(cmd).fetchall()]
        return patterns


    def update_db(self, db_conn):
        cols = ("value", "t_add", "t_remove")
        c = db_conn.cursor()
        if self.pattern_id == 0:
            cmd = "INSERT INTO {name} ({schema}) VALUES ({qmarks})".format(
                name=self.name,
                schema=",".join(cols),
                qmarks=",".join(["?"]*len(cols)))
            args = (self.value, self.t_add, self.t_remove)
        else:
            cmd = "UPDATE {name} SET {schema} WHERE {key}=?".format(
                name=self.name,
                schema=",".join(["{}=?".format(c) for c in cols]),
                key="pattern_id")
            args = (self.value, self.t_add, self.t_remove, self.pattern_id)
        logging.info(cmd + ' ' + ' '.join(map(str, args)))
        c.execute(cmd, args)
        self.pattern_id = (self.pattern_id if self.pattern_id else c.lastrowid)
        db_conn.commit()


    def match(self, torrent):
        return bool(re.search(self.value, torrent.torrent_title))


class Gardener(object):

    def __init__(self, interactive=False, config_file=''):
        self.base_url = "https://pt.sjtu.edu.cn"
        self.interactive = interactive
        self.user_info_file = "data/parms.pickle"
        self.user_info = dict()
        self.db_file = "/var/tmp/Gardener/gardener.db"
        self.db_conn = None
        self.session = None
        self.torrents_dir = "/var/tmp/Gardener"
        self.torrent_schema = list()
        self.pattern_schema = list()
        self.torrents = list()
        self.patterns = list()
        self.patterns_file = "data/patterns.txt"
        if config_file:
            self.load_config(config_file)


    @staticmethod
    def get_path(path):
        if not path.startswith("/"):
            return os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
        else:
            return path

    def load_config(self, fname):
        config = yaml.load(open(fname), Loader=yaml.Loader)
        self.base_url = config["base_url"]
        self.user_info_file = config["user_info_file"]
        self.db_file = config["db_file"]
        self.torrents_dir = config["torrents_dir"]
        self.torrent_schema = config["torrent_schema"]
        self.pattern_schema = config["pattern_schema"]
        self.patterns_file = config["patterns_file"]
        logging.info("Loaded config from {}\n{}".format(fname, yaml.dump(config)))


    def load_db(self):
        self.db_conn = sqlite3.connect(self.db_file, detect_types=sqlite3.PARSE_DECLTYPES)
        self.patterns = Pattern.load_db(self.db_conn, self.pattern_schema)
        self.torrents = Torrent.load_db(self.db_conn, self.torrent_schema)


    def update_patterns(self):
        input_patterns = open(self.patterns_file).read().splitlines()
        for pobj in self.get_effective_patterns():
            if pobj.value not in input_patterns:
                pobj.t_remove = datetime.now()
                pobj.update_db(self.db_conn)
        for pstr in input_patterns:
            if pstr not in [pobj.value for pobj in self.get_effective_patterns()]:
                pobj = Pattern(0, pstr, datetime.now(), None)
                pobj.update_db(self.db_conn)
                self.patterns.append(pobj)


    def get_effective_patterns(self):
        return [pobj for pobj in self.patterns if pobj.t_remove is None]


    def get_session(self):
        if not self.user_info:
            self.get_user_info(self.user_info_file)
        self.session = requests.Session()
        self.session.post(urljoin(self.base_url, "takelogin.php"), data=self.user_info) 


    def get_ratios(self):
        if not self.session: self.get_session()
        resp = self.session.get(urljoin(self.base_url, "torrents.php"))
        soup = bs(resp.text, "lxml")
        user_msg = soup.find(id="usermsglink")
        return tuple(user_msg.find_all("span")[1].stripped_strings)


    def get_new_torrents(self):
        if not self.session:
            self.get_session()
        resp = self.session.get(urljoin(self.base_url, "torrents.php"))
        soup = bs(resp.text, "lxml")
        is_torrent_td = lambda tag: \
            tag.name == u"td" and \
            tag.get("class") == ["embedded"] and \
            len(tag.find_all("a")) == 1 and \
            tag.a["href"].startswith("details.php")
        tds = soup.find_all(is_torrent_td)
        ptids = [re.match(r"details.php\?id=(\d+)&.*", t.find("a")["href"]).group(1) for t in tds]
        titles = [t.find("a")["title"] for t in tds]
        new_idxs = [i for i in range(len(ptids)) if ptids[i] not in [tobj.torrent_ptid for tobj in self.torrents]]
        new_torrents = [Torrent(0, ptids[i], titles[i], t_add=datetime.now()) for i in new_idxs]
        self.torrents.extend(new_torrents)
        for torrent in new_torrents:
            torrent.update_db(self.db_conn)
        return new_torrents


    def download_new_torrents(self):
        new_torrents = self.get_new_torrents()
        if new_torrents:
            self.download_matching_torrents(new_torrents)
        else:
            logging.info("No new torrents")


    def download_matching_torrents(self, torrents=list()):
        undownloaded_torrents = [tobj for tobj in (torrents if torrents else self.torrents)
                                 if not (tobj.torrent_file and os.path.isfile(tobj.torrent_file))]
        for tobj in undownloaded_torrents:
            for pattern in self.patterns:
                if pattern.match(tobj):
                    self.download_torrent(tobj, pattern)


    def download_torrent(self, torrent, pattern):
        if torrent.torrent_file and os.path.isfile(torrent.torrent_file):
            logging.info("Torrent {torrent_ptid} - {torrent_title} is already downloaded".format(**torrent.__dict__))
            return
        resp = self.session.get(urljoin(self.base_url, "download.php"), params={"id":torrent.torrent_ptid})
        torrent_name = unquote(re.search(r"filename=(.*)", resp.headers['Content-Disposition']).group(1))
        torrent.torrent_file = os.path.join(self.torrents_dir, torrent_name)
        logging.info("Download {torrent_ptid} - {torrent_title} to {torrent_file}".format(**torrent.__dict__))
        open(torrent.torrent_file, "wb").write(resp.content)
        torrent.t_start = datetime.now()
        torrent.pattern_id = pattern.pattern_id
        torrent.update_db(self.db_conn)


    def update_torrents(self):
        for tobj in self.torrents:
            tobj.update_db(self.db_conn)


    def get_user_info(self, user_info_file):
        user_info = dict()
        if not os.path.isfile(user_info_file):
            logging.error("Cannot read user info from {}".format(user_info_file))
        else:
            try:
                user_info = pickle.load(open(user_info_file, "rb"))
            except Exception as e:
                logging.error("Cannot read user info from {}\n{}".format(user_info_file, e))
                user_info = dict()
        if self.validate_user_info(user_info):
            self.user_info = user_info
        elif self.interactive:
            print("Cannot read user info from {} or recorded user info is invalid. Please input".format(user_info_file))
            user_info = self.input_user_info(user_info_file=user_info_file)
            if not user_info:
                raise InvalidUserParameters
            else:
                self.user_info = user_info
        else:
            raise InvalidUserParameters


    def input_user_info(self, max_retries=3, user_info_file=''):
        for _ in range(max_retries):
            username = input("username: ")
            password = getpass("password: ")
            checkcode = "XxXx"
            user_info = {"username": username, "password": password, "checkcode": checkcode}
            if self.validate_user_info(user_info):
                if user_info_file:
                    try:
                        pickle.dump(user_info, open(user_info_file, "wb"))
                    except Exception as e:
                        logging.error("Cannot write user info to {}\n{}".format(user_info_file, e))
                        raise CannotWriteUserParameters
                    logging.info("Saved user info to {}".format(user_info_file))
                return user_info
            else:
                logging.error("User info is invalid.")
                print("User info is invalid. Please retry.")
        else:
            logging.error("Cannot get valid user info after max retries (3)")
            return dict()


    def validate_user_info(self, user_info):
        if not user_info:
            return False
        resp = requests.post(urljoin(self.base_url, "takelogin.php"), data=user_info)
        if "登录失败" in resp.text:
            logging.error("Login failed, probably because CAPTCHA code is required.")
            return False
        elif "退出" in resp.text:
            logging.info("Login succeeded")
            return True
        else:
            logging.error("Login failed due to unknown reason.")
            return False


    def run(self, interval):
        if not self.db_conn:
            self.load_db()
        while True:
            self.update_patterns()
            self.download_new_torrents()
            time.sleep(interval)


def main():
    import logging
    logging.basicConfig(filename='example.log',
                        level=logging.DEBUG,
                        format="%(asctime)s %(levelname)s %(module)s %(message)s")
    gardener = Gardener(config_file="gardener_config.yaml")
    gardener.load_db()
    gardener.update_patterns()
    gardener.download_new_torrents()


if __name__ == "__main__":
    main()
