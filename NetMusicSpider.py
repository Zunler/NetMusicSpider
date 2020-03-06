import requests
from bs4 import BeautifulSoup
from pymysql import connect
import json


class NetMusicSpider(object):
    chinese: str = "https://music.163.com/discover/artist/cat?id=100"
    europe: str = "https://music.163.com/discover/artist/cat?id=200"
    japan: str = "https://music.163.com/discover/artist/cat?id=600"
    korea: str = "https://music.163.com/discover/artist/cat?id=700"
    headers: dict = {"Accept": "application/json, text/plain, */*",
                     "Accept-Encoding": "gzip, deflate",
                     "Accept-Language": "zh-HK,zh;q=0.9,en-HK;q=0.8,en;q=0.7,zh-CN;q=0.6,zh-TW;q=0.5,en-US;q=0.4",
                     "Content-Type": "application/json;charset=UTF-8",
                     "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36"}

    def __init__(self):

        self.session = requests.session()
        self.sqlData = []

    def write_to_mysql(self, host, user, password, db, sql, port, charset):
        # 创建连接
        conn = connect(host=host, port=port, db=db, user=user, password=password,
                       charset=charset)
        # 游标
        cur = conn.cursor()
        print("start to write to database.......")
        for item in self.sqlData:
            cur.execute(sql, item)
            print(item)

        conn.commit()
        # 关闭游标　
        cur.close()
        # 关闭连接
        conn.close()
        print('write to database success!')

    def transform_music_length(self, time_stamp):
        min = int(time_stamp / 1000 / 60)
        second = int(time_stamp / 1000 % 60)
        if (min < 10):
            min = "0" + str(min)
        if (second < 10):
            second = "0" + str(second)
        return str(min) + ":" + str(second)

    def get_hot_music(self):
        # 获取热门音乐
        url = "https://music.163.com/discover/toplist?id=3778678"
        re = self.session.get("https://music.163.com/discover/toplist?id=3778678", headers=self.headers)
        soup = BeautifulSoup(re.text, "lxml")
        li = soup.find('ul', class_="f-hide")
        text = soup.find("textarea")
        res = json.loads(text.string)
        print("start to get hot music......")
        for item in res:
            music_id = item['id']
            source_path = "http://music.163.com/song/media/outer/url?id=" + str(item["id"]) + ".mp3"
            music_name = item['name']
            artist_name = item['artists'][0]['name']
            artist_id = item['artists'][0]['id']
            pic_url = item['album']['picUrl']
            timeStamp = item['duration']
            music_length = self.transform_music_length(timeStamp)
            self.sqlData.append([music_id, music_name, music_length, artist_id, artist_name, pic_url, source_path])
            print([music_id, music_name, music_length, artist_id, artist_name, pic_url, source_path])

    print("fetch all music success")


def get_hot_artist(self, type):
    # 获取某类型的热门歌手
    if (type == 'chinese'):
        url = self.chinese
    elif (type == 'europe'):
        url = self.europe
    elif (type == 'japan'):
        url = self.japan
    elif (type == 'korea'):
        url = self.korea
    print("start to get hot artist...... ")
    for i in range(1, 4):
        req = self.session.get(url + str(i), headers=self.headers)
        soup = BeautifulSoup(req.text, "lxml")
        cover = soup.find_all("div", class_="u-cover u-cover-5")
        artistDetail = soup.find_all("li", class_="sml")
        # 封面歌手,直接含有封面
        for item in cover:
            src = item.find('img')['src']
            a = item.find('a')
            artist_id = str(a['href']).split("=")[1]
            artist_name = a['title'][:-3]
            self.sqlData.append([artist_id, artist_name, src])
            print([artist_id, artist_name, src])
        # 列表歌手，需要进入主页找封面
        for item in artistDetail:
            a = item.find('a', class_="nm nm-icn f-thide s-fc0")
            artist_id = str(a['href']).split("=")[1]
            artist_name = a.string
            home_url = "https://music.163.com/artist?id=" + artist_id
            home_req = self.session.get(home_url, headers=self.headers)
            home_soup = BeautifulSoup(home_req.text, 'lxml')
            script = json.loads(home_soup.find("script", type="application/ld+json").string)
            src = script['images'][0]
            self.sqlData.append([artist_id, artist_name, src])
            print([artist_id, artist_name, src])
        print("fetch all artist success!")


if __name__ == "__main__":
    NMS = NetMusicSpider()
    NMS.get_hot_music()
    NMS.write_to_mysql("172.16.29.107", "user", "pass", "music","INSERT INTO TB_SYS_MUSIC_INFO VALUES (%s,%s,%s,%s,%s,%s,%s)", 3306, "utf8")
