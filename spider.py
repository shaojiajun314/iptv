from queue import Queue
from time import sleep
from threading import Thread
from spiders.szpxyg import SPIDER as SPIDER_szpxyg
from model.models import (
	Category,
	Video,
	PlatformVideo,
	DATABASE
)
from model.spider_ret import RetVideo


class SPIDER():
	def __init__(self, proxies=None):
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
	s = SPIDER(
		proxies='http://127.0.0.1:7890'
	)
	s.run()