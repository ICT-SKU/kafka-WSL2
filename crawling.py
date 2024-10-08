import requests
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from confluent_kafka import Producer
import socket
import time
import re
import os #titleId만 저장할 것이기에 csv 보단 os가 더 적합하다고 판단.
import json

# Kafka Producer 설정
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}

producer = Producer(producer_conf)

# 메시지 전달 콜백 함수
def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {msg.value}: {err}")
    else:
        print(f"Message produced: {msg.value}")

LAST_TITLE_ID = "./last_titleId.txt"

# 마지막 게시글 id 저장
def saveTitleId(titleId):
        with open(LAST_TITLE_ID, "w") as i:
            i.write(titleId)
            print(f"마지막 게시글 ID {titleId} 저장 완료")

# 마지막 게시글 id 불러오기
def loadTitleId():
    if os.path.exists(LAST_TITLE_ID):
        with open(LAST_TITLE_ID, "r") as i:
            print("마지막 게시글 ID 불러오기 완료")
            return i.read().strip()

    return None


# 크롬 드라이버 기본 설정
options = webdriver.ChromeOptions()
options.add_argument('--headless') #브라우저 없이 실행
options.add_argument('--no-sandbox') #샌드박스 보안기능 비활성화
options.add_argument('--disable-dev-shm-usage')

# 크롬 드라이버 최신 버전 설정
service = Service(executable_path='/root/chromedriver-linux64/chromedriver')
# 크롬 드라이버
driver = webdriver.Chrome(service = service, options = options)

# 크롤링 함수
def crawl_and_produce():
    baseurl = 'https://cafe.naver.com/joonggonara/'
    page = 1 # 현재 크롤링 하는 페이지
    display = 50 # 한 페이지에 표시되는 게시물의 수
    last_titleId = loadTitleId() # 마지막으로 크롤링한 게시물의 ID
    save_last_titleId = False # 가장 최신 게시물의 titleId를 저장했는지 여부를 판단
    
    while True:
        driver.get(baseurl + 'ArticleList.nhn?search.clubid=10050146&search.page=' + str(page) + '&userDisplay=' + str(display))
        driver.switch_to.frame('cafe_main') #iframe 전환
        soup = bs(driver.page_source, 'html.parser')

        print(f"페이지 {page} 게시글 추출을 실행합니다.")
        soup = soup.find_all(class_='article-board m-tcol-c')[1]
        datas = soup.select("#main-area > div:nth-child(4) > table > tbody > tr")

        for idx, data in enumerate(datas):
            title = data.find(class_="article") # 제목과 url이 포함된 article 자체를 가져옴
            url = baseurl + title.get('href') # url
            title = ' '.join(title.get_text().split()) #bs4의 .strip()을 안쓴 이유 : 게시글 몇개는 특수 문자로 도배되어 있는데 strip 메소드는 해당 특수문자를 인식하지 못하여 공백을 제대로 제거하지 못함.
            print(f'\ntitle : {title}\nurl : {url}')

            category = data.find(class_= 'link_name').get_text().strip()
            date = data.find(class_= 'td_date').get_text().strip()
            print(f'category 이름 : {category}\ndate : {date}')

            # 저장된 url 에서 articleid만 추출
            match = re.search(r'articleid=(\d+)', url)
            titleId = match.group(1) if match else None
            print(f'게시글 고유 id : {titleId}\n')

            if last_titleId and int(titleId) <= int(last_titleId):
                print(f"이전에 크롤링 된 게시물 {last_titleId}에 도달하였습니다.")
                return
            
            if not save_last_titleId:
                print(f"기존 {last_titleId}를 {titleId}로 최신화 합니다")
                saveTitleId(titleId)
                save_last_titleId = True

            message_value = json.dumps({
                'title' : title,
                'url' : url,
                'category' : category,
                'date' : date
            })

            #메시지 전송
            producer.produce('naver_cafe_posts', key=titleId, value=message_value, callback=acked)

            if idx % 500 == 0:
                producer.poll()

        # 메시지 전송 대기
        producer.flush()
        page += 1

if __name__ == "__main__":
    try:
        while True:
            crawl_and_produce()
            time.sleep(60)
    except KeyboardInterrupt:
        print("작업 중단")
    finally:
        driver.quit()
