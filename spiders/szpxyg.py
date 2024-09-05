import re
import requests
from time import sleep
from queue import Queue
from pathlib import Path
from bs4 import BeautifulSoup
from functools import partial
from json import dumps, loads
from threading import Thread, Lock
from utils.retry import (
	HttpError,
	retry
)
from model.spider_ret import RetVideo


class szpxyg():
	URL = 'https://szpxyg.com/show/2-----------.html'
	# EXCEPT_CATEGORIES = ('伦理片', )

	szpxygProcessFile = 'szpxyg_process.json'

	def __init__(
		self,
		ret_queue,
		workers=10,
		root_sleep_dur=60*60*24,
		episode_sleep_dur=10,
		is_test=False,
		proxy=None
	):
		self.queue = Queue(10)
		self.workers = workers
		self.ret_queue = ret_queue
		self.root_sleep_dur = root_sleep_dur
		self.episode_sleep_dur = episode_sleep_dur
		self.szpxyg_process_file = Path(self.szpxygProcessFile)

		self.total = 0 # todo
		self.is_end = False

		self.process_lock = Lock()
		self.process_list = set()
		if not self.szpxyg_process_file.exists():
			self.write_process()
		else:
			self.read_process()

		self.proxy = {
			'http': proxy,
			'https': proxy,
		} if isinstance(proxy, str) else proxy

		if is_test:
			self.episode_sleep_dur = 0

	def write_process(self):
		with self.process_lock:
			with open(self.szpxyg_process_file, 'w') as f:
				if self.is_end and len(self.process_list) >= self.total:
					f.write(dumps([]))
					self.process_list = set()
				else:
					f.write(dumps(tuple(self.process_list)))

	def read_process(self):
		with self.process_lock:
			with open(self.szpxyg_process_file, 'r') as f:
				d = loads(f.read())
		self.process_list = set(d)

	@retry
	def get(self, url):
		response = requests.get(
			url,
			proxies=self.proxy
		)
		if response.status_code != 200:
			print(f'status code: {response.status_code}, url: {url}')
			raise HttpError(code=response.status_code)
		return response

	def get_html(self, url):
		return self.get(url).text

	def parse_root(self, url):
		html_doc = self.get_html(url)
		soup = BeautifulSoup(html_doc, 'html.parser')

		video = list(soup.select('a.stui-vodlist__thumb.lazyload'))
		print(f'got videos: {len(video)}')
		video_a = map(
			lambda x: {
				'title': x.attrs['title'],
				'href': x.attrs['href']
			},
			video
		)
		video_a = list(video_a) 
		# print(video_a)
		for i in video_a:
			self.total += 1
			self.queue.put(i)
			# self.parse_video(**i)
			# return # test

		pages = list(soup.select('.stui-page li a'))
		next_page = filter(lambda p: p.text.strip() == '下一页', pages)
		next_page = list(next_page)[0].attrs['href'].strip()
		# if next_page:
		if next_page == url:
			return
		return next_page

	def parse_video(self, title, href):
		if href in self.process_list:
			return

		html_doc = self.get_html(href)
		soup = BeautifulSoup(html_doc, 'html.parser')

		attr = list(soup.select('div.stui-content__detail'))
		assert len(attr) == 1, f'error video attr numbers, {href}'
		attr = attr[0]
		attrs = list(attr.select('p.data'))
		base, actor, director, desc = attrs
		# print(base)
		# keys = [i.text for i in base.select('p span.text-muted')]
		# values = [i.text for i in base.select('p a,.text-muted.hidden-xs')]
		# # print(keys, values)
		# assert all([
		# 	keys[0].startswith('类型'),
		# 	keys[1].startswith('地区'),
		# 	keys[2].startswith('年份')
		# ])
		category, area, year = [
			i.split('：', 1)[1].strip()
			for i in base.text.strip().split('\n')
		]
		video_attrs = ({
			'category': category,
			'area': area,
			'year': year,	
		})
		
		play_list = list(soup.select('.stui-pannel-box.b.playlist.mb ul.stui-content__playlist.clearfix'))
		# print(play_list)

		key_url_map = {}

		for index, i in enumerate(play_list):
			a = list(i.select('li a'))
			key_tmp = f'链接{index}'
			key_url_map[key_tmp] = {}
			for j in a:
				# if not (j.text in key_url_map[index]):
				# 	key_url_map[index][j.text] = []
				key_url_map[key_tmp][j.text] = j.attrs['href']

		key_m3u8_map = {
			key: {}
			for key, v in key_url_map.items()
		}
		for k, m in key_url_map.items():
			for key, v in m.items():
				# print(f'starting {title}-{k} m3u8 fetching length: {len(v)}')
				# for i in v:
				m3u8_url = self.fetch_m3u8(v)
				key_m3u8_map[k][key] = (m3u8_url)
				sleep(self.episode_sleep_dur)

		print(title, video_attrs, href, key_m3u8_map)
		self.ret_queue.put(
			RetVideo(
				title=title,
				category=category,
				area=area,
				year=year,
				key_m3u8_map=key_m3u8_map,
				platform='szpxyg'
			)
		)

		self.process_list.add(href)
		self.write_process()


	def fetch_m3u8(self, url):
		html_doc = self.get_html(url)
		s = r'"url":"https:\/\/'
		try:
			index = (html_doc.index(s))
		except ValueError as e:
			print(f'parse m3u8 html_doc: {html_doc}')
			raise
		index = index + len(s)
		code = html_doc[index: index + 128].split('"', 1)[0]
		return f'https://{code}'

	def run(self):
		def parse_root_loop():
			url = self.URL
			while 1:
				print(f'start parse root {url}')
				url = self.parse_root(url)
				if not url:
					url = self.URL
					print('complete parse root')
					self.is_end = True
					sleep(self.root_sleep_dur)
					while not self.queue.empty():
						sleep(self.root_sleep_dur)
					self.is_end = False

		def parse_video_loop():
			while 1:
				conf = self.queue.get()
				self.parse_video(**conf)

		ts = [
			Thread(target=parse_root_loop)
		] + [
			Thread(target=parse_video_loop)
			for _ in range(self.workers)
		]
		for i in ts: i.setDaemon(True)
		for i in ts: i.start()
		while 1:
			for t in ts:
				if not t.is_alive():
					exit()
			else:
				sleep(10)

SPIDER = partial(
	szpxyg, 
	workers=10,
	is_test=False,
)

if __name__ == '__main__':
	r = Queue()
	a = szpxyg(
		workers=10,
		ret_queue=r,
		is_test=True,
		proxy='http://127.0.0.1:7890'
	)
	a.run()