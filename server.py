import os
import sys

import stat
import traceback
from io import StringIO
from pathlib import Path
from cheroot import wsgi
from queue import Queue
from wsgidav import util
from wsgidav.fs_dav_provider import (
        FolderResource as FolderResourceBackup,
        DAVCollection,
        FileResource,
        DAVError,
        BUFFER_SIZE,
        FilesystemProvider as FilesystemProviderBackup,
)
from wsgidav.dav_provider import DAVNonCollection
from wsgidav.wsgidav_app import WsgiDAVApp

from model.models import (
    Category,
    Video,
    PlatformVideo,
    DATABASE
)

import cv2
import numpy as np
from threading import Thread, Lock
from ffmpeg_transfer import MultiFFMPEGWorker

LIMIT = 3
CACHING_MAP = {
    
}
CACHING_MAP_LOCK = Lock()
def SET_CACHING_MAP(k, v, total=LIMIT):
    with CACHING_MAP_LOCK:
        if not (k in CACHING_MAP):
            if len(CACHING_MAP) >= total:
                return False
        CACHING_MAP[k] = v
    return True

DOWNLOADER = MultiFFMPEGWorker(
    workers=10,
    is_test=True,
)

def processBarGenerator(p, text_start_pos=None, file_path=None):
    width, height = 640, 480
    fps = 30 # 30  # 每秒帧数
    duration = 1 # 2  # 视频长度（秒）
     
    # 创建视频写入对象
    fourcc =  cv2.VideoWriter_fourcc(*'XVID')
    file_path = file_path or 'progress_video.mp4'
    out = cv2.VideoWriter(file_path, fourcc, fps, (width, height))

    font = cv2.FONT_HERSHEY_SIMPLEX
    text_start_pos = text_start_pos or (220, 240)
    text_color = (255, 255, 255)
    text_scale = 1
    text_thickness = 2

    for i in range(0, fps * duration):
        frame = np.zeros((height, width, 3), np.uint8)
        # progress_width = int((i / (fps * duration)) * width)
        text = f'{p} downloading...'
        # text = f'download: {i / (fps * duration):.0%}'
        cv2.putText(
            frame,
            text, 
            text_start_pos,
            font,
            text_scale,
            text_color,
            text_thickness
        )
        out.write(frame)
        # print(f'Progress: {i / (fps * duration):.0%}')
    out.release()


def path2ResourceTag(path, tmpl='category/area/name/platform/link/ep'):
    '''
        area/type/name/platform
        dir by year
        4
    '''
    keys = tmpl.split('/')
    keys_length = len(keys)

    path = path.strip().strip('/').split('/')
    path += [None for _ in range(keys_length - len(path))]
    category, area, video_name, platform, link, ep =  path[0:6]
    if platform:
        platform = platform.strip('播放平台:').strip()
    if ep:
        ep = ep.strip('.mp4').strip()
    path = category, area, video_name, platform, link, ep
    return path

def build_cache_file_path(
    root_folder_path,
    category,
    area,
    video_name,
    platform,
    link,
    ep
):
    cache_file_path = Path(root_folder_path) / category / area / video_name / platform / link / ep
    cache_file_path = f'{cache_file_path}.mp4'
    return Path(cache_file_path)    

def get_url_resource(
    category,
    area,
    video_name,
    platform,
    link,
    ep
):
    print(category,
    area,
    video_name,
    platform,
    ep)
    if ('.' in ep):
        return
    # ep = ep.strip('.mp4')
    category = Category.get_or_none(Category.name==category)
    v = Video.get_or_none(
        (
            Video.category==category
        )&(
            Video.area==area
        )&(
            Video.name==video_name
        )
    )
    p = PlatformVideo.get_or_none(
        PlatformVideo.video==v,
        PlatformVideo.platform==platform
    )
    print(p.resources, link, ep, 'eee')
    return p.resources[link][ep]

def is_leaf(name):
    return name.endswith('.mp4')

class StreamResource(DAVNonCollection):
    """Represents a single existing DAV resource instance.

    See also _DAVResource, DAVNonCollection, and FilesystemProvider.
    """

    PENDING_IMAGE = 'progress_video.mp4'
    LIMIT_VIDEO = 'limit_video.mp4'
    # /poster.jpg/
    # PENDING_IMAGE = 'aa.jpeg'

    def __init__(self, path: str, environ: dict, root_folder_path):
        # cache_file_path, m3u8_conf
        self.root_folder_path = root_folder_path
        category, area, video_name, platform, link, ep = path2ResourceTag(path)
        self.category, self.area, self.video_name, self.platform, self.link, self.ep =  category, area, video_name, platform, link, ep
        # r = path2ResourceTag(
        #         path
        #     )
            # category, area, video_name, platform, link, ep =  r[0:6]
        print(category, area, video_name, platform, link, ep, 'category, area, video_name, platform, link, ep')
        cache_file_path = build_cache_file_path(
            root_folder_path=self.root_folder_path,
            category=category,
            area=area,
            video_name=video_name,
            platform=platform,
            link=link,
            ep=ep
        )
# cache_file_path,
        super().__init__(path, environ)
        self.cache_file_path = cache_file_path
        # self.m3u8_conf = m3u8_conf
        self.cached = os.path.exists(self.cache_file_path)
        if self.cached:
            self.file_stat: os.stat_result = os.stat(self.cache_file_path)
        else:
            self.file_stat: os.stat_result = os.stat(self.PENDING_IMAGE)

    # Getter methods for standard live properties
    def get_content_length(self):
        return self.file_stat[stat.ST_SIZE]

    def get_content_type(self):
        return util.guess_mime_type(self.path)

    def get_creation_date(self):
        return self.file_stat[stat.ST_CTIME]

    def get_display_name(self):
        if self.cached:
            name, shuffix = self.name.rsplit('.', 1)
            return f'{name}(已下载).{shuffix}'
        d = CACHING_MAP.get(self.cache_file_path)
        if not d:
            return self.name
        name, shuffix = self.name.rsplit('.', 1)
        return f'{name}(下载中{d[0]}/{d[1]}).{shuffix}'

    def get_etag(self):
        if not self.cached:
            return util.get_file_etag(self.PENDING_IMAGE)
        return util.get_file_etag(self.cache_file_path.as_posix())

    def get_last_modified(self):
        return self.file_stat[stat.ST_MTIME]

    def is_link(self):
        return False

    def support_etag(self):
        return True

    def support_ranges(self):
        return True

    def get_content(self):
        assert not self.is_collection
        if self.cached:
            return open(self.cache_file_path, "rb", BUFFER_SIZE) 
        if not (self.cache_file_path in CACHING_MAP):
            self.m3u8_conf = get_url_resource(
                category=self.category,
                area=self.area,
                video_name=self.video_name,
                platform=self.platform,
                link=self.link,
                ep=self.ep
            )
            if not self.m3u8_conf:
                return
            os.makedirs(self.cache_file_path.as_posix().rsplit('/', 1)[0], exist_ok=True)
            q = Queue(10)
            # CACHING_MAP[self.cache_file_path] = [0, 1000]
            r = SET_CACHING_MAP(self.cache_file_path, (0, 1000))
            if not r:
                return open(self.LIMIT_VIDEO , "rb", BUFFER_SIZE)
            ts = [
                Thread(target=self.download, args=(q, )),
                Thread(target=self.write_process, args=(q, ))
            ]
            for t in ts: t.setDaemon(True)
            for t in ts: t.start()
        
        return open(self.PENDING_IMAGE, "rb", BUFFER_SIZE)

    def process_q_msg(self, msg):
        if isinstance(msg, int):
            return True
        print(msg, 'todo test')
        return False

    def write_process(self, q):
        total = q.get()
        if not self.process_q_msg(total):
            return
        # CACHING_MAP[self.cache_file_path][1] = total
        SET_CACHING_MAP(self.cache_file_path, (0, total))
        dur = int(total/10)
        while 1:
            cur = q.get()
            if not self.process_q_msg(cur):
                return
            # CACHING_MAP[self.cache_file_path][0] = cur
            SET_CACHING_MAP(self.cache_file_path, (cur, total))
            if cur == 0:
                break
            # print(f'test {cur}/{total}')
            if cur % dur == 0:
                processBarGenerator(f'{cur}/{total}')

    def download(self, q):
        url = self.m3u8_conf
        print(url, 'uuuuu')
        url = url.replace('\\/', '/')
        print(f'fetching url: {url}')
        try:
            ret = DOWNLOADER.m3u8Fetcher(url, self.cache_file_path, q)
            # 判断结果 清空 error 计数
            if not ret:
                processBarGenerator('something error1', file_path=self.cache_file_path.as_posix())
        except Exception as e:
            # todo error 计数
            q.put(0)
            f = StringIO()
            traceback.print_exc(file=f)
            value = f.getvalue()
            print(value)
            # text_start_pos=(0, 240), 
            processBarGenerator('something error2', file_path=self.cache_file_path.as_posix())
        del CACHING_MAP[self.cache_file_path]


class FolderResource(DAVCollection):
    def __init__(self, path: str, environ: dict, root_folder_path):
        super().__init__(path, environ)
        self.root_folder_path = root_folder_path
        self.path = path
        category, area, video_name, platform, link, ep = path2ResourceTag(path)
        self.category, self.area, self.video_name, self.platform, self.link, self.ep =  category, area, video_name, platform, link, ep
        # super().__init__(path, environ, file_path)

    def get_env_names(self):
        if not self.category:
            return [
                i.name
                for i in Category.select(Category.name).distinct()
            ]
        category = Category.get_or_none(Category.name==self.category)
        if not category:
            return []
        if not self.area:
            qs = Video.select(Video.area).where(Video.category==category).distinct()
            return [
                i.area
                for i in qs
            ]
        if not self.video_name:
            qs = Video.select(Video.name).where(
                (Video.category==category)&(Video.area==self.area)
            )
            return [
                i.name
                for i in qs
            ]
        v = Video.get_or_none(
            (
                Video.category==category
            )&(
                Video.area==self.area
            )&(
                Video.name==self.video_name
            )
        )
        if not v:
            return []
        if not self.platform:
            qs = PlatformVideo.select(PlatformVideo.platform).where(
                PlatformVideo.video==v
            ).distinct()
            return [
                f'播放平台: {i.platform}'
                for i in qs
            ]
        p = PlatformVideo.get_or_none(
            PlatformVideo.video==v,
            PlatformVideo.platform==self.platform
        )
        if not p:
            return []
        if not self.link:
            return [i for i in p.resources.keys()]
        leaf = p.resources.get(self.link)
        if not leaf:
            return []
        return [f'{i}.mp4'for i in leaf.keys()]

    def get_member_names(self):
        names = self.get_env_names()
        return names

    def get_member(self, name: str) -> FileResource:
        assert util.is_str(name), f"{name!r}"
        path = util.join_uri(self.path, name)
        res = None
        if is_leaf(name):
            res = StreamResource(
                path,
                self.environ,
                self.root_folder_path, 
                # self.category, self.area, self.video_name, self.platform, self.link, self.ep
            )
        else:
            # r = path2ResourceTag(
            #     path
            # )
            # category, area, video_name, platform, link, ep =  r[0:6]
            res = FolderResource(path, self.environ, self.root_folder_path)
        return res

class FilesystemProvider(FilesystemProviderBackup):
    def get_resource_inst(self, path: str, environ: dict) -> FileResource:
        self._count_get_resource_inst += 1
        
        path = path.rstrip('/')
        r = path2ResourceTag(
            path
        )
        category, area, video_name, platform, link, ep =  r[0:6]
        # if platform:
        #     platform = platform.strip('播放平台:').strip()
        if ep:
            # if ep.endswith('.mp4'):
            # ep = ep.strip('.mp4')
            # cache_file_path = build_cache_file_path(
            #     root_folder_path=self.root_folder_path,
            #     category=category,
            #     area=area,
            #     video_name=video_name,
            #     platform=platform,
            #     link=link,
            #     ep=ep
            # )
            return StreamResource(
                path,
                environ,
                self.root_folder_path,
                # category, area, video_name, platform, link, ep
                # cache_file_path,
                # get_url_resource(
                #     category=category,
                #     area=area,
                #     video_name=video_name,
                #     platform=platform,
                #     link=link,
                #     ep=ep
                # )
            )
            # else:
            #  # ep.endswith('.jpg'):
            #     # test
            #     return FileResource(path, environ, 'aa.jpeg')
        return FolderResource(
            path,
            environ,
            self.root_folder_path,
            # category, area, video_name, platform, link, ep
        )


def main():
    root_path = "./var"
    os.makedirs(root_path, exist_ok=True)

    provider = FilesystemProvider(root_path, readonly=True, fs_opts={})

    config = {
        "host": "0.0.0.0",
        "port": 8080,
        "provider_mapping": {"/": provider},
        "http_authenticator": {
            "domain_controller": None  # None: dc.simple_dc.SimpleDomainController(user_mapping)
        },
        "simple_dc": {"user_mapping": {"*": True}},  # anonymous access
        "verbose": 4,
        "logging": {
            "enable": True,
            "enable_loggers": [],
        },
        "property_manager": True,  # True: use property_manager.PropertyManager
        "lock_storage": True,  # True: use LockManager(lock_storage.LockStorageDict)
    }
    app = WsgiDAVApp(config)

    # For an example, use cheroot:
    version = (
        f"{util.public_wsgidav_info} {wsgi.Server.version} {util.public_python_info}"
    )

    server = wsgi.Server(
        bind_addr=(config["host"], config["port"]),
        wsgi_app=app,
        server_name=version,
        # "numthreads": 50,
    )

    app.logger.info(f"Running {version}")
    app.logger.info(f"Serving on http://{config['host']}:{config['port']}/ ...")
    try:
        server.start()
    except KeyboardInterrupt:
        app.logger.info("Received Ctrl-C: stopping...")
    finally:
        server.stop()


if __name__ == "__main__":
    processBarGenerator('0/1000')
    processBarGenerator(f'limit {LIMIT} video downloading', file_path='limit_video.mp4')
    main()
