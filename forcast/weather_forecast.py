# -*- coding:utf-8 -*-
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta
import json
import random
import time
import logging
import traceback

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

headers = {'Accept': '*/*',
           'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 '
                         'Safari/537.36',
           'Connection': 'keep-alive'}


def get_data(hour):
    """
	get the weather forecast data in the next eight hours

	Parameters
	----------
	hour: int
		current hour + how many hours between current hour and the target hour you want
		e.g. now is 11:00:00, you want 18:00:00, the variable hour should be 18
			 if you want 18:00:00 tomorrow, the variable hour should be 42

	Returns
	----------
	data: list
		each element contains one feature of weather data, like
			['温度', '16°', '17°', '18°', '19°', '20°', '19°', '18°', '17°']

	"""
    url = "https://www.accuweather.com/zh/gb/london/ec4a-2/hourly-weather-forecast/328328?hour=%s" % hour
    # url = 'https://www.accuweather.com/zh/cn/beijing/101924/hourly-weather-forecast/101924?hour=%s' % hour
    logger.debug('fetch url: %s' % url)
    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')
    data = [i for d in soup.find_all(class_='hourly-table') for i in d.stripped_strings]
    data = [data[i: i + 9] for i in range(len(data)) if i % 9 == 0][:-2]
    return data


def parse_data(data, timestamp):
    """
	parse the data got from the function get_data(hour)

	Parameters
	----------
	data: list
		list from function get_data(hour)

	timestamp: datetime.datetime
		the datetime of the first element in variable data

	Returns
	----------
	results: dict
		all weather forecast date from the timestamp as beginning to the eight hours later
	"""
    results = []
    t = [[i[index] for i in data[1:]] for index in range(1, 9)]
    columns = [i[0] for i in data[1:]]
    for index in range(len(t)):
        delta = timedelta(hours=index)
        r = dict(zip(columns, t[index]))
        r['时间'] = datetime.strftime(timestamp + delta, '%Y-%m-%d %H:00:00')
        del r['降水']
        del r['天空']
        results.append(r)
    return results


def get_batch_data(now):
    """
	get two days weather forecast data from now on
	Parameters
	----------
	now: datetime.datetime
		now time

	Returns
	----------
	data: str
		json string ends with '\n'
	"""
    global delta
    results = []
    for i in range(7):
        delta = timedelta(hours=i * 8)

        data = get_data(now.hour + i * 8)
        logger.info('got data of %s' % (datetime.strftime(now + delta, '%Y-%m-%d %H:00:00')))
        results.extend(parse_data(data, now + delta))

        time.sleep(random.uniform(0, 3))

    if len(results) != 56:
        logger.warning(
            'there maybe something wrong with data length of %s' % datetime.strftime(now + delta, '%Y-%m-%d %H:00:00'))

    data = json.dumps({'time': datetime.strftime(now, '%Y-%m-%d %H:00:00'), 'data': results}) + '\n'
    logger.info('got data successfully!')
    return data


def insert_data_into_file(data, filename='data_ld.txt'):
    """
	insert json string into file

	Parameters
	----------
	data: str
		the json string from the function get_batch_data(now)

	filename: str
        which file you want to save the data
	"""
    with open(filename, 'a') as f:
        f.write(data)
    logger.info('insert data info file successfully!')


def fetch():
    """
	fetch the data and save them into a file
	"""
    now = now = datetime.now()
    logger.info('start fetching weather data at %s' % (datetime.strftime(now, '%Y-%m-%d %H:%M:%S')))
    insert_data_into_file(get_batch_data(now))


if __name__ == '__main__':
    while True:
        retry_times = 0
        success = False
        while retry_times < 10:
            if success:
                break
            try:
                fetch()
                success = True
            except Exception as e:
                logger.error(e)
                traceback.print_exc(file=open('traceback_ld.txt', 'a'))
                retry_times += 1
                time.sleep(30)
        else:
            logger.error('program has fatal error!')
        time.sleep(3600)
