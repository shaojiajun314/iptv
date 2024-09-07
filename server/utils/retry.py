import urllib3
import requests
from time import sleep

RETRY_NUMS = 20

class HttpError(Exception):
	def __init__(self, code=0):
		self.code = code

def retry(func):
	def foo(self, *args, **kw):
		err = None
		for i in range(RETRY_NUMS):
			try:
				ret = func(self, *args, **kw)
				return ret
			except StopIteration as e:
				err = e
				raise e
			except HttpError as e:
				err = e
				print(e, 'HttpError error') # TODO: 写进日志
				sleep(5)
			except requests.exceptions.SSLError as e:
				err = e
				print(e, 'requests.exceptions.SSLError error') # TODO: 写进日志
				sleep(5)
			except requests.exceptions.ProxyError as e:
				err = e
				print(e, 'requests.exceptions.ProxyError error') # TODO: 写进日志
				sleep(5)
			except urllib3.exceptions.InvalidChunkLength as e:
				err = e
				peinr(e, 'urllib3.exceptions.InvalidChunkLength')
				sleep(5)
			except Exception as e:
				err = e
				print(e, 'retry error') # TODO: 写进日志
				raise
		raise err
	return foo