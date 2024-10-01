import os
import m3u8
import requests
from time import sleep
from hashlib import md5
from pathlib import Path
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from utils.retry import (
	HttpError,
	retry
)
import urllib

class TemporaryDir():
    def __init__(self, dir='./tmp', remove_tmp=True):
        self.dir = Path(dir)
        self.remove_tmp = remove_tmp

    def __enter__(self):
        os.makedirs(self.dir, exist_ok=True)

    def __exit__(self, type, value, trace):
        self.remove_tmp and self.remove_folder(self.dir)
        self.dir = None

    def remove_folder(self, path):
	    if os.path.exists(path):
	        if os.path.isfile(path) or os.path.islink(path):
	            os.remove(path)
	        else:
	            for filename in os.listdir(path):
	                self.remove_folder(os.path.join(path, filename))
	            os.rmdir(path)


def UrlStrMap(s):
	shuffix = s.rsplit('.')[-1]
	return f'{md5(s.encode()).hexdigest()}.{shuffix}'


class M3U8():
	def __init__(self, url):
		self.url = url
		self.playlist = None

	def parseM3u8(self, m3u8Url):
	    playlist = m3u8.load(m3u8Url)
	    if playlist.is_variant:
	        playlists = playlist.playlists
	        if len(playlists) > 0 :
	            subPlaylist = playlists[0]   # 默认取第一个，有多个的情况，其实可以优化（应该是选择分辨率高的）
	            #subM3u8Url = subPlaylist.base_uri + subPlaylist.uri
	            subM3u8Url = subPlaylist.absolute_uri
	            return self.parseM3u8(subM3u8Url)
	        else :
	            return None
	    else :
	    	self.playlist = playlist

	def getFileUrlsAndPaths(self): 
		allUrls = []
		for seg in self.playlist.segments:
			if seg == None:
				break
			allUrls.append(seg.absolute_uri)
 
		for key in self.playlist.keys:
			if key == None:
				break
			allUrls.append(key.absolute_uri)

		return allUrls
	
	def getUrls(self):
		self.parseM3u8(self.url)
		allUrls = self.getFileUrlsAndPaths()
		return allUrls

	def newM3u8File(self, m3u8Path, path_transfer): 
	    for seg in self.playlist.segments:
	        if seg == None:
	            break
	        if not seg.absolute_uri:
	        	break
	        seg.uri = path_transfer(seg.absolute_uri)
	 
	    for key in self.playlist.keys:
	        if key == None:
	            break
	        if not key.absolute_uri:
	        	break
	        key.uri = path_transfer(key.absolute_uri)
	 
	    self.playlist.dump(m3u8Path)


	def toMp4(self, tmp, mp4_path, path_transfer):
		m3u8Path = tmp / f'tmp_m3u8.m3u8'
		self.newM3u8File(m3u8Path, path_transfer)
		ffmpeg_command = f"ffmpeg -allowed_extensions ALL -protocol_whitelist \"file,http,crypto,tcp\" -i {m3u8Path} -c copy {mp4_path}"  
		print('=== ffmpeg开始转码 ===')
		print(f'm3u8路径： {m3u8Path}')
		print(f'输出mp4路径： {mp4_path}')
		# 使用subprocess调用FFmpeg并捕获输出  
		res = os.system(ffmpeg_command)
		# process = subprocess.Popen(ffmpeg_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
		# stdout, stderr = process.communicate()  
		if res == 0:  
		    print("=== ffmpeg转码完成 ====")    
		    # print(stdout.decode())  
		    return True
		else:  
		    print("=== 转换失败！=== ")  
		    # print(stderr.decode())
		return False


class MultiFFMPEGWorker():

	def __init__(self, workers=1, is_test=False):
		self.workers = workers
		self.is_test = is_test

	def parseM3u8(self, m3u8Url):
	    playlist = m3u8.load(m3u8Url)
	    if playlist.is_variant:
	        playlists = playlist.playlists
	        if len(playlists) > 0 :
	            subPlaylist = playlists[0]   # 默认取第一个，有多个的情况，其实可以优化（应该是选择分辨率高的）
	            #subM3u8Url = subPlaylist.base_uri + subPlaylist.uri
	            subM3u8Url = subPlaylist.absolute_uri
	            return self.parseM3u8(subM3u8Url)
	        else :
	            return None
	    else :
	        return playlist

	def getFileUrlsAndPaths(self, playlist, tmp): 
		allUrls = []
		for seg in playlist.segments:
			if seg == None:
				break
			allUrls.append(seg.absolute_uri)
 
		for key in playlist.keys:
			if key == None:
				break
			allUrls.append(key.absolute_uri)

		return allUrls
	
	def m3u8Fetcher(self, url, mp4_path, queue=None):
		m3u = M3U8(url)
		tmp = Path('./tmp')
		try:
			allUrls = m3u.getUrls()
		except urllib.error.URLError as e:
			queue.put(e)
			raise
			return
		with TemporaryDir(dir=tmp, remove_tmp=(not self.is_test)):
			conf = {
				'lock': Lock(),
				'length': len(allUrls),
				'complete': 0
			}
			queue and queue.put(conf["length"])
			with ThreadPoolExecutor(max_workers=self.workers) as executor:
				list(executor.map(lambda x: self.download(*x), [
					(i, tmp, conf, queue)
					for i in allUrls
				]))
			queue and queue.put(0)
			ret = m3u.toMp4(tmp, mp4_path, UrlStrMap)
		return ret
	@retry
	def __download(self, url):
		response = requests.get(url)
		if response.status_code != 200:
			print(f'status code: {response.status_code}, url: {url}')
			raise HttpError()
		return response.content

	def _download(self, url, file_path):
		if os.path.exists(file_path):
			return
		data = self.__download(url)
		with open(file_path, 'wb') as f:
			f.write(data)

	def download(self, url, tmp, conf, queue):
		if not url:
			return
		file_path = tmp / UrlStrMap(url)
		self._download(url, file_path)
		with conf['lock']:
			conf['complete'] += 1
			print(f'{conf["complete"]} / {conf["length"]}')
			queue and queue.put(conf["complete"])
	SHUFFIX_MAP = {
		'm3u8': m3u8
	}


if __name__ == '__main__':
	url = 'https://ukzy.ukubf4.com/20230116/FA8P2t2A//2000kb/hls/index.m3u8'
	url = 'https://v.gsuus.com/play/9b6Z9VVb/index.m3u8'
	a = MultiFFMPEGWorker(
		workers=10,
		is_test=True,
	)
	a.m3u8Fetcher(url, '抓娃娃.mp4')



