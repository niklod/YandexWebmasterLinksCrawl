#Отправка страниц на переобход в яндекс вебмастере. Очередь на переобход формируется из файла
#https://docs.google.com/spreadsheets/d/1BHk6V-GFLdBmQBnV9OUXXWVvdzipz6Gzzpbx6p8R4eQ/edit#gid=0
#Данный файл доступен только для учетки goods

import requests
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import json
import gspread
import telebot
from telebot import apihelper
import yaml
import os
import time

def get_curr_path():
    return os.getcwd()


def get_yaml_info(filename, param):
    curr_path = get_curr_path()
    return yaml.load( open(os.path.join( curr_path, filename ), 'r', encoding='utf-8'), Loader=yaml.FullLoader )[param]


def get_yandex_token():
    yandex_token = get_yaml_info('config.yaml', 'yandex_token')
    return yandex_token


def get_telegram_token():
    telegtam_token = get_yaml_info('config.yaml', 'bot_token')
    return telegtam_token


def send_message(message, chat_name):
    telegram_token = get_telegram_token()
    bot = telebot.TeleBot(telegram_token)

    userproxy = get_yaml_info('config.yaml', 'userproxy')
    password = get_yaml_info('config.yaml', 'password')
    proxy_address = get_yaml_info('config.yaml', 'proxy_address')
    port = get_yaml_info('config.yaml', 'port')
    chat_id = get_yaml_info('config.yaml', chat_name)
    apihelper.proxy = {'https':'socks5h://{}:{}@{}:{}'.format(userproxy, password, proxy_address, port)}

    bot.send_message(chat_id, message)


def auth():
    #Функция авторизации в API google
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(os.path.join(os.getcwd(), 'YandexWebmasterLinksCrawl.json'), scope)
    gc = gspread.authorize(credentials)
    return gc


def parse_limits():
    #возвращает остаток квоты на перебход из яндекс вебмастера
    #технический токен на аккаунт goodspr
    host_id = get_yaml_info('config.yaml', 'host_id')

    http_header = {'Authorization': 'OAuth ' + get_yandex_token()} #http заголовок для авторизации
    request_url = 'https://api.webmaster.yandex.net/v4/user/{}/hosts/{}/recrawl/quota/'
    
    try:
        r = requests.get('https://api.webmaster.yandex.net/v4/user/', headers=http_header, timeout=5.000)
        user_id = r.json()['user_id'] # получаем user_id
    except r.exceptions.RequestException as e:
        send_message('Ошибка в получении user_id, код ответа: {}'.format(e), 'my_chat_id')
    
    try:
        request = requests.get(request_url.format(user_id, host_id), headers=http_header, timeout=5.000)
        request = request.json()
        return int(request['quota_remainder'])
    except request.exceptions.RequestException as e:
        send_message('Ошибка в парсинге лимитов, код ответа: {}'.format(e), 'my_chat_id')


def get_webmasters_lists(auth):
    #Переводим урлы из sheet1 в словарь с разбивкой по веб-мастерам
    gc = auth
    webmaster_urls = {}
    urls_sheet = gc.open('python_test').sheet1.get_all_records()

    for row in urls_sheet:
        webmaster = row['Имя спеца'].strip().lower()
        url = row['URL'].strip()
        webmaster_urls.setdefault(webmaster, [])
        if url not in webmaster_urls[webmaster]:
            webmaster_urls[webmaster].append(url)
    return webmaster_urls


def log(key, ok_num, error_num, auth):
    #функция логирования, принимает 3 статуса: 1-все урлы загружены; 2-ошибки в загрузке; 3-нечего загружать
    gc = auth
    today = datetime.today()
    today = today.strftime('%d.%m.%Y')
    log_sheet = gc.open('python_test').worksheet('Логи')
    if key == 1:
        log_sheet.append_row([today, 'Все URL в рамках квоты отправлены на переобход'])
        send_message('Статус переобхода URL:\n{} - {} из {} URL в рамках квоты отправлены на переобход'.format(today, ok_num, ok_num + error_num), 'seo_chat_id')
    if key == 2:
        log_sheet.append_row([today, 'Error: Часть или все URL не были отправлены на переобход'])
        send_message('Статус переобхода URL:\n{} - Error: {} из {} URL не были отправлены на переобход'.format(today, error_num, ok_num + error_num), 'seo_chat_id')
    if key == 3:
        log_sheet.append_row([today, 'Нет URL для переобхода'])
        send_message('Статус переобхода URL:\n{} - Нет URL для переобхода'.format(today), 'my_chat_id')


def make_queue(dictionary):
    #Составляет очередь из урлов для отправки на переобход.
    #Возвращает лист URL для переобхода
    queue_list = []

    if dictionary.values():
        limit = int(parse_limits()/len(dictionary.values()))
    else:
        limit = int(parse_limits())
        
    
    #получаем доступные лимиты
    webmasters_limits = {}
    
    #считаем количество урлов у каждого мастера
    for key, value in dictionary.items():
        webmasters_limits.setdefault(key, {})
        webmasters_limits[key].setdefault('count', len(dictionary[key]))
        webmasters_limits[key].setdefault('limit', limit)
    
    #Получаем количество неиспользуемых лимитов
    spare = 0
    for key, value in webmasters_limits.items():
        webmasters_limits[key].setdefault('spare_limit', 0)
        url_count = webmasters_limits[key]['count']
        url_limit = webmasters_limits[key]['limit']
        
        if url_count < url_limit:
            webmasters_limits[key]['limit'] = url_count
        
        difference = url_limit - url_count
        
        if difference > 0:
            webmasters_limits[key]['spare_limit'] = difference
            spare += difference
        else:
            webmasters_limits[key]['spare_limit'] = 0
            spare += 0
    
    # получаем вебмастеров нуждающихся в доп лимитах
    need_limits = []
    for key, value in webmasters_limits.items():   
        if webmasters_limits[key]['spare_limit'] <= 0:
            need_limits.append(key)
    if need_limits:
        divided_spare = spare / len(need_limits)
    for value in need_limits:
        webmasters_limits[value]['limit'] += divided_spare
    
    #формируес лист URL
    for key, value in dictionary.items():
        quate = int(webmasters_limits[key]['limit'])
        for url in value[:quate]:
            queue_list.append(url)
    if not queue_list:
        log(3, 0, 0, auth())
        return False
    return queue_list


def delete_url(url, auth):
    #функция удаления строки с URL из листа sheet1
    gc = auth
    row_num = 2
    urls_sheet = gc.open('python_test').sheet1
    
    for row in urls_sheet.get_all_records():
        if url == row['URL']:
            urls_sheet.delete_row(row_num)
            return True
        else:
            row_num += 1
    return False


def send_request(urls):
    #отправка запроса на переобход страниц
    yandex_token = get_yandex_token() #технический токен на аккаунт goodspr
    config = yaml.load(open(os.path.join(os.getcwd(), 'config.yaml'), 'r', encoding='utf-8'), Loader=yaml.FullLoader)
    host_id = config['host_id']

    http_header = {'Authorization': 'OAuth ' + yandex_token, 'Content-type': 'application/json'} #http заголовок для авторизации
    r = requests.get('https://api.webmaster.yandex.net/v4/user/', headers=http_header, timeout=5.000)
    user_id = r.json()['user_id'] # получаем user_id
    request_url = 'https://api.webmaster.yandex.net/v4/user/{}/hosts/{}/recrawl/queue/'
    
    url_dict = {}
    url_dict.setdefault('url', '')
    
    headers = {} #словарь с ответами сервера
    headers.setdefault('ok', 0)
    headers.setdefault('error', 0)
    
    if urls == False:
        return print('Нет урлов для переобхода')

    for url in urls:
        url_dict['url'] = url
        r = json.dumps(url_dict)
        try:
            request = requests.post(request_url.format(user_id, host_id), headers=http_header, data=r)
            request.raise_for_status()
            headers['ok'] += 1
            delete_url(url, auth()) #удаляем URL из sheet
        except requests.exceptions.RequestException as e:
            print(e)
            headers['error'] += 1

        time.sleep(1) #Ждем секунду чтобы не нагружать API google
            
    if headers['ok'] == len(urls):
        log(1, headers['ok'], 0, auth())
    else:
        log(2, headers['ok'], headers['error'], auth())


def main():
    #основная функция
    queue = make_queue(get_webmasters_lists(auth()))
    send_request(queue)


if __name__ == "__main__":
    main()