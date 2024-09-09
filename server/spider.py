import os
import curio
import resource
from time import sleep
from queue import Queue
from pathlib import Path
from random import randint
from threading import Thread
from yaml import load, FullLoader
from spiders.szpxyg import SPIDER as SPIDER_szpxyg
from model.models import (
	Category,
	Video,
	PlatformVideo,
	DATABASE
)
from model.spider_ret import RetVideo
from proxy_utl import get_server, multi_server

class ProxyManaGer():
	
	__instance = None
	def __new__(cls, *args, **kw):
		if cls.__instance:
			return cls.__instance
		cls.__instance = super().__new__(cls)
		return cls.__instance

	def __init__(self, path, proxy_start_port=10000):
		self.path = path
		self.proxy_start_port = proxy_start_port
		self.ts = []
		self.code = {}

	def parse(self):
		with open(self.path, encoding='utf-8') as f:
			code = f.read()
			self.code = load(code, Loader=FullLoader)
			print('yaml parse: ', self.code)
			self.proxies = self.code['proxies']

	def __getattr__(self, k):
		if k in self.code:
			return self.code[k]
		return super().__getattr__(k)

	def run_proxy(self, conf):
		resource.setrlimit(resource.RLIMIT_NOFILE, (50000, 50000))
		kernel = curio.Kernel()
		try:
			kernel.run(multi_server((*get_server(conf),)))
		except KeyboardInterrupt:
			kernel.run(shutdown=True)

	def run(self, proxy_limit=None):
		self.parse()
		p = self.proxy_start_port
		while 1:
			ts_length = len(self.ts)
			if proxy_limit and ts_length >= proxy_limit:
				break
			if ts_length >= len(self.proxies):
				break
			conf = self.proxies[ts_length]
			protocal = f'{conf["type"]}{conf["udp"] and "udp" or ""}'
			uri = f'http://127.0.0.1:{p+ts_length}?via={protocal}://{conf["cipher"]}:{conf["password"]}@{conf["server"]}:{conf["port"]}'
			# self.run_proxy('http://127.0.0.1:7890?via=ssudp://chacha20-ietf-poly1305:Zex8OQ@b97cccac.pnd6xm1ljcfpc3b-fbnode.6pzfwf.com:56001')
			t = Thread(target=self.run_proxy, args=(uri,))
			self.ts.append(t)
		for i in self.ts: i.setDaemon(True)
		for i in self.ts: i.start()
		print(f'proxy: {len(self.ts)}')
		while 1:
			for t in self.ts:
				if not t.is_alive():
					exit()
			else:
				sleep(10)


class SPIDER():
	def __init__(self, proxies=None):
		# proxy conf clash tml conf
		if isinstance(proxies, ProxyManaGer):
			t = Thread(target=proxies.run)
			t.setDaemon(True)
			t.start()
			self.proxies = [
				f'http://127.0.0.1:{proxies.proxy_start_port + i}'
				for i in range(len(proxies.ts))
			]
		else:
			self.proxies = proxies

		self.ret_queue = Queue(100)

	def run(self):
		spider_instance = SPIDER_szpxyg(
			ret_queue=self.ret_queue,
			proxy=self.proxies
		)

		ts = [
			Thread(target=self.gather_ret)
		] + [
			Thread(target=spider_instance.run)
		]
		for i in ts: i.setDaemon(True)
		for i in ts: i.start()
		while 1:
			for t in ts:
				if not t.is_alive():
					exit()
			else:
				sleep(10)

	def gather_ret(self):
		while 1:
			ret = self.ret_queue.get()
			assert isinstance(ret, RetVideo)
			title = ret.title
			category = ret.category
			area = ret.area
			year = ret.year
			key_m3u8_map = ret.key_m3u8_map
			platform = ret.platform

			# title = 'ret.title'
			# category = 'ret.category'
			# area = 'ret.area'
			# year = '2020'
			# key_m3u8_map = {1: 'ret.key_m3u8_map'}
			# platform = 'ret.platform'

			unique_key = f'{title}-{category}-{area}-{year}'
			with Category.manager.atomic():
				c = Category.get_or_none(Category.name==category)
				if not c:
					c = Category.create(
						name=category
					)
				v = Video.get_or_none(
					Video.unique_key==unique_key
				)
				if not v:
					v = Video.create(
						name=title,
						category=c,
						area=area,
						year=year,
						unique_key=unique_key
					)
				p = PlatformVideo.get_or_none(
					PlatformVideo.video==v,
					PlatformVideo.platform==platform
				)
				if p:
					for k, v in key_m3u8_map.items():
						if k in p.resources:
							p.resources[k] = (p.resources[k]) | (v)
						else:
							p.resources[k] = v
					p.save()
				else:
					p = PlatformVideo.create(
						video=v,
						platform=platform,
						resources=key_m3u8_map
					)

if __name__ == '__main__':
	pcp = ProxyManaGer('./proxies_conf.yaml')
	# pcp.run()
	# exit()
	s = SPIDER(
		proxies=pcp,
		# proxies='http://127.0.0.1:7890'

	)
	s.run()