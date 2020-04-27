import aioschedule
import asyncio
from ruamel.yaml import YAML
from os import listdir, path
import os
import re
from datetime import datetime, timedelta
import logging
from . import ffmpeg, m3u8
from lxml.builder import E, ElementMaker
from lxml import etree
from email.utils import formatdate as formatemaildate
from aiohttp import web
import signal
from uuid import uuid4
import json
import re
from collections import deque
from time import time
from typing import List, Tuple, Dict
import traceback

logging.basicConfig(level=os.environ.get('LOGLEVEL', 'WARNING').upper())
log = logging.getLogger(__name__)

CONFIG_FILE = os.environ.get('PYCAST_SHOWS', 'shows.yaml')
shows = None
recording_tasks = {}

RECORDING_WATCHDOG_TIME = 10
DATE_FORMAT = '%Y%m%d%H%M'
FILE_NAME = '{name}_{start}_{end}_{part:03}'
TMP_EXT = '.ts'
OUT_EXT = os.environ.get('PYCAST_EXT', '.m4a')
OUT_FMT = os.environ.get('PYCAST_FORMAT', 'aac')
OUT_BITRATE = os.environ.get('PYCAST_BITRATE', '128k')
RE_FILE_NAME = re.compile(r'(?P<name>.+)_(?P<start>\d{12})_(?P<end>\d{12})(_(?P<part>\d+))?')

# TODO
TEMP_DIR = os.environ.get('PYCAST_TEMP', '/tmp/recording')
OUT_DIR = os.environ.get('PYCAST_OUT', '/tmp/recordings')
HTTP_PORT = os.environ.get('PYCAST_PORT', 80)
HTTP_BASE = os.environ.get('PYCAST_HTTPBASE', 'http://localhost/files/')

sleeper: asyncio.Task = None
wakeup = False
parts_requested = []

class File:

    def __init__(self, filename):
        self.filename = filename
        self.basename = path.basename(self.filename)
        match = RE_FILE_NAME.search(self.basename)
        if not match or self.filename.endswith('.metadata') or self.filename.endswith('.playlist'):
            raise ValueError()
        self.name, start, end, self.part = match.group('name', 'start', 'end', 'part')
        self.part = int(self.part)
        self.start = datetime.strptime(start, DATE_FORMAT)
        self.end = datetime.strptime(end, DATE_FORMAT)

    @staticmethod
    async def try_parse(filename):
        try:
            return File(filename)
        except ValueError:
            return None

    async def populate_metadata(self, cache=True):
        # log.debug(f'populate_metadata({self.filename})')
        CACHE_FILE = self.filename + '.metadata'
        if path.isfile(CACHE_FILE) and cache:
            with open(CACHE_FILE, 'r') as f:
                cached = json.load(f)
                self.duration = cached['duration']
                self.size = cached['size']
                self.mtime = datetime.fromtimestamp(cached['mtime'])
        else:
            self.duration = await ffmpeg.get_duration(self.filename)
            stats = os.stat(self.filename)
            self.size = stats.st_size
            self.mtime = datetime.fromtimestamp(stats.st_mtime)
            if cache:
                with open(CACHE_FILE, 'w') as f:
                    json.dump({
                        'duration': self.duration,
                        'size': self.size,
                        'mtime': stats.st_mtime
                    }, f)

    def is_part(self, other):
        return other.name == self.name and other.start >= self.start and other.end <= self.end

class RecordingTask:
    proc_task: asyncio.Task = None
    output_file = ''
    last_output_file_size = 0
    last_output_file_time = datetime.now()

def get_filename(show_name, start, end, part, ext):
    start_f = datetime.strftime(start, DATE_FORMAT)
    end_f = datetime.strftime(end, DATE_FORMAT)
    return FILE_NAME.format(name=show_name, start=start_f, end=end_f, part=part) + ext
        
async def run_forever():
    global sleeper
    global wakeup
    idle_seconds = 0
    pending = set()
    while True:
        pending.add(asyncio.create_task(aioschedule.run_pending()))
        default_wait_time = RECORDING_WATCHDOG_TIME if recording_tasks else 3600
        if aioschedule.jobs:
            idle_seconds = max(aioschedule.idle_seconds(), 0)
        else:
            idle_seconds = default_wait_time
        sleeper = asyncio.create_task(asyncio.sleep(min(idle_seconds, default_wait_time)))
        pending.add(sleeper)
        try:
            wakeup = False
            while not sleeper.done():
                _, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            await monitor_recordings()                
        except asyncio.CancelledError:
            if not wakeup:
                raise
        except:
            log.error('Failure while running tasks. Exception follows')
            log.error(traceback.format_exc())
        sleeper = None

def wake_scheduler():
    global wakeup
    if sleeper:
        wakeup = True
        sleeper.cancel()

def get_show_by_name(name):
    for s in shows['shows']:
        if s['name'] == name:
            return s

async def read_show_files(files, name=None, condition=lambda _: True, cache_metadata=True):
    result = []
    for f in files:
        file = await File.try_parse(f)
        if file and (not name or file.name == name) and condition(file):
            await file.populate_metadata(cache_metadata)
            result.append(file)
    result.sort(key=lambda f: f.part)
    return result

async def get_show_files(dir_path, name=None, condition=lambda _: True, cache_metadata=True):
    return await read_show_files([path.join(dir_path, p) for p in listdir(dir_path)], name, condition, cache_metadata)

async def monitor_recordings():
    for name, task in recording_tasks.items():
        if (datetime.now() - task.last_output_file_time).seconds >= RECORDING_WATCHDOG_TIME:
            task.last_output_file_time = datetime.now()

            try:
                new_size = os.stat(task.output_file).st_size
                if new_size == task.last_output_file_size:
                    log.info(f'No change in filesize for {task.output_file}. Current recording process will be killed')
                    await end_recording(name)
                else:
                    task.last_output_file_size = new_size
            except FileNotFoundError:
                log.warning(f'File {task.output_file} has not appeared yet')

async def record(url, file_name, format, bitrate):
    with open(file_name + '.playlist', 'w') as metadata_f:

        # Small buffer before writing to file to catch out of order items
        BUFFER_LENGTH = 2
        recent_metadata = []

        # Recent metadata
        info_stream = deque(maxlen=100)
        # ffmpeg chunks awaiting tagging
        metadata_queue = deque(maxlen=10)

        media_start_time = time()

        def write_metadata(metadata_item):
            chunk_time, chunk_info = metadata_item
            metadata_f.write(f'{chunk_time} {json.dumps(chunk_info)}\n')
            metadata_f.flush()

        def match_metadata():
            nonlocal recent_metadata
            if len(metadata_queue) > 0:
                last_item = metadata_queue[-1]
                while True:
                    this_item = metadata_queue[0]
                    chunk_time, chunk_url = this_item
                    info = None
                    try:
                        info = next((x for x in info_stream if x.get('file', '') == chunk_url))
                        del info['file']
                    except StopIteration:
                        pass
                    if info:
                        # Now processed so remove from list
                        metadata_queue.popleft()
                        if len(recent_metadata) == 0 or (len(recent_metadata) > 0 and recent_metadata[-1][1] != info):
                            recent_metadata.append((chunk_time, info))
                            recent_metadata.sort()
                            if len(recent_metadata) > BUFFER_LENGTH:
                                log.debug('Write metadata')
                                write_metadata(recent_metadata[0])
                                recent_metadata = recent_metadata[1:]
                    else:
                        # No metadata found, try next chunk
                        metadata_queue.rotate()
                    if this_item == last_item:
                        break

        async def read_metadata():
            m3u8_file = m3u8.M3u8(url)
            async for info in m3u8_file.read_song_info():
                log.debug(f'Got info for {info["file"]}')
                info_stream.append(info)
                match_metadata()
        
        metadata_task = asyncio.create_task(read_metadata())

        try:
            re_file = re.compile(r"Opening '(.+)' for reading")
            re_time = re.compile(r'time=(\d+):(\d+):(\d+)(?:\.(\d+))?')
            async for err_line in ffmpeg.convert_with_stderr(url, file_name, OUT_FMT, OUT_BITRATE):
                file_match = re_file.search(err_line)
                if file_match:
                    chunk_url = file_match[1]
                    log.debug(f'Need metadata for {chunk_url}')
                    metadata_queue.append((time() - media_start_time, chunk_url))
                time_match = re_time.search(err_line)
                if time_match:
                    # Re-sync the start time based on current media time
                    h, m, s, ms = tuple(int(n) for n in time_match.groups('0'))
                    real_media_time = timedelta(hours=h, minutes=m, seconds=s, milliseconds=ms).total_seconds()
                    log.debug(f'Media time is {real_media_time}')
                    media_start_time = time() - real_media_time
                match_metadata()
        except asyncio.CancelledError:
            pass
        except:
            log.warning('Exception during recording')
            log.warning(traceback.format_exc())
            raise
        finally:
            metadata_task.cancel()
            # Mop up final metadata items
            for metadata_item in recent_metadata:
                write_metadata(metadata_item)
            try:
                await metadata_task
            except asyncio.CancelledError:
                pass

async def start_recording(show):
    log.debug(f'start_recording({show})')
    show_name = show['name']

    task = recording_tasks.get(show_name, None)
    if task:
        log.info('Already recording ' + show_name)
        return

    schedule = show['schedule']
    log.info('Start recording ' + show_name)

    existing_files = await get_show_files(TEMP_DIR, show_name, cache_metadata=False)
    if existing_files:
        part_num = existing_files[-1].part + 1
    else:
        part_num = 1

    start_time = datetime.strptime(schedule['start'], '%H:%M')
    end_time = datetime.strptime(schedule['end'], '%H:%M')

    start_date = datetime.now().replace(hour=start_time.hour, minute=start_time.minute)
    end_date = start_date.replace(hour=end_time.hour, minute=end_time.minute)
    if end_time.hour < start_time.hour:
        end_date = end_date + timedelta(days=1)

    file_name = path.join(TEMP_DIR, get_filename(show_name, start_date, end_date, part_num, TMP_EXT))

    proc_task = asyncio.create_task(record(show['stream'], file_name, OUT_FMT, OUT_BITRATE))
    recording_task = RecordingTask()
    recording_task.proc_task = proc_task
    recording_task.output_file = file_name
    recording_tasks[show['name']] = recording_task
    wake_scheduler()
    try:
        await proc_task
    except:
        # Can be expected on failure, or end of recording
        log.debug('proc_task exception')
        log.debug(traceback.format_exc())
    await end_recording(show)
    await finalise_recordings()

async def end_recording(show):
    log.debug(f'end_recording({show})')
    name = show['name']
    task = recording_tasks.get(name, None)
    if task:
        task.proc_task.cancel()
        del recording_tasks[name]

async def finalise_recordings(skip_long_running=False):
    req_id = str(uuid4())
    log.debug(f'{req_id} finalise_recordings({skip_long_running})')
    
    try:
        long_running_tasks = []
        lock = asyncio.Lock()
        log.debug(req_id + ' await lock')
        async with lock:
            log.debug(req_id + ' got lock')
            tmp_files = await get_show_files(TEMP_DIR, cache_metadata=False)
            out_files = [path.join(OUT_DIR, p) for p in listdir(OUT_DIR)]
            
            live_part_shows = []
            for show_name in parts_requested:
                show = get_show_by_name(show_name)
                if show and show.get('live_parts', False):
                    live_part_shows.append(show_name)
            parts_requested.clear()

            # Group files relating to the same recording
            groups = {}
            for f in tmp_files:
                k = f'{f.name}_{f.start}_{f.end}'

                group_files = groups.get(k, [])
                group_files.append(f)
                groups[k] = group_files
                
            for k, v in groups.items():
                log.debug(f'{req_id} Processing file group {k}')

                now = datetime.now()

                v.sort(key=lambda f: f.part)
                first_tmp = v[0]
                total_duration = sum(f.duration for f in v)

                # Resume the recording if it should be running
                if (not skip_long_running) and now > first_tmp.start and now < first_tmp.end and not recording_tasks.get(first_tmp.name, None):
                    # Use scheduler, but run once
                    show = get_show_by_name(first_tmp.name)
                    if show:
                        # This is long running, so we'll await this outside the lock
                        long_running_tasks.append(asyncio.create_task(start_recording(show)))

                # Create parts if ended or requested
                if (now > first_tmp.end) or (first_tmp.name in live_part_shows):

                    # Locate parts created so far
                    last_part_num = 0
                    parts_duration = 0
                    for pf in await read_show_files(out_files, condition=lambda f: first_tmp.is_part(f)):
                        if pf.part > last_part_num:
                            last_part_num = pf.part
                        parts_duration += pf.duration

                    # Create a new part - diff between tmp duration and total
                    part_length = total_duration - parts_duration
                    if part_length > 1: # seconds - 1 to account for rounding etc
                        metadata = []
                        try:
                            for f_info in v:
                                with open(f_info.filename + '.playlist', 'r') as f:
                                    while line := f.readline():
                                        time, info_json = tuple(line.split(maxsplit=1))
                                        metadata.append((float(time), json.loads(info_json)))
                                        metadata.sort()
                        except:
                            log.warning(f'No valid metadata for {first_tmp.name}')
                            metadata = []

                        part_num = last_part_num + 1
                        part_name = get_filename(first_tmp.name, first_tmp.start, first_tmp.end, part_num, OUT_EXT)
                        part_path = path.join(OUT_DIR, part_name)
                        print(req_id + ' create part')
                        await ffmpeg.create_part([f.filename for f in v], part_path, parts_duration, total_duration)
                        part_metadata = []
                        prev_item = None
                        in_bounds = False
                        for time, info in metadata:
                            if not in_bounds and time > parts_duration:
                                if prev_item:
                                    part_metadata.append(prev_item)
                                in_bounds = True
                            if in_bounds and time < total_duration:
                                part_metadata.append((time, info))
                            if time > total_duration:
                                break
                            prev_item = time, info
                        if part_metadata:
                            with open(part_path + '.playlist', 'w') as f:
                                for time, info in part_metadata:
                                    f.write(f'{time} {json.dumps(info)}\n')
                                    f.flush()
                        print(req_id + ' end create part')

                    if now > first_tmp.end:
                        for f in v:
                            os.remove(f.filename)

        if long_running_tasks:
            await asyncio.wait(long_running_tasks)

    except Exception as e:
        log.error('Failure during finalise_recordings. Exception follows')
        log.error(e)
                
def create_job(day, at):
    job = aioschedule.every()
    job.start_day = day
    job.unit = 'weeks'
    job.at(at)
    return job

async def get_show_as_podcast(name):
    files = await get_show_files(OUT_DIR, name, cache_metadata=True)

    IT_NS = 'http://www.itunes.com/dtds/podcast-1.0.dtd'
    NS_MAP = {'itunes': IT_NS}

    it = ElementMaker(namespace=IT_NS, nsmap=NS_MAP)
    root = ElementMaker(namespace=None, nsmap=NS_MAP)

    def file_to_item(f):
        return E.item(
            E.title(f'{name} - {datetime.strftime(f.start, "%Y-%m-%d")} (Part {f.part})'),
            it.author(name),
            it.summary(''),
            E.enclosure(
                '',
                url=(HTTP_BASE + f.basename),
                length=str(f.size),
                type='audio/mp4'
            ),
            E.guid(f.basename),
            E.pubDate(formatemaildate(f.mtime.timestamp())),
            it.duration(str(timedelta(seconds=f.duration)))
        )

    doc = (
        root.rss(
            E.channel(
                E.title(name),
                E.description(''),
                E.link(''),
                it.author(name),
                *[file_to_item(f) for f in files]
            ),
            version='2.0'
        )
    )

    return etree.tostring(doc, pretty_print=True)

async def get_feed_http(request):
    log.debug(f'get_feed_http')
    show_name = request.match_info.get('name', None)
    if show_name:
        parts_requested.append(show_name)
        await finalise_recordings(skip_long_running=True)
        rss = await get_show_as_podcast(show_name)
        return web.Response(body=rss, content_type='application/rss+xml')
    return web.Response(status=404)

async def run():
    log.debug('Debug logging enabled')

    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)

    global shows
    with open(CONFIG_FILE, 'r') as f:
        shows = YAML().load(f)

    for show in shows['shows']:
        # Make the show name file safe
        show['name'] = ''.join([c if c.isalpha() or c.isdigit() else '-' for c in show['name']])
        show_sched = show['schedule']
        for day in show_sched['every']:
            create_job(day, show_sched['start']).do(start_recording, show)
            create_job(day, show_sched['end']).do(end_recording, show)

    # Clean-up task is oneoff, but may result in recording being resumed
    cleanup_task = asyncio.create_task(finalise_recordings())
    scheduler_task = asyncio.create_task(run_forever())

    # HTTP server
    server = web.Application()
    server.add_routes([web.get('/shows/{name}', get_feed_http)])
    server.router.add_static('/files', OUT_DIR)
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', HTTP_PORT)
    site_task = asyncio.create_task(site.start())
    
    await asyncio.wait([cleanup_task, scheduler_task, site_task])

def main():
    try:
        try:
            os.setpgrp()
        except:
            pass # Not allowed e.g. in Docker
        asyncio.run(run())
    finally:
        try:
            # Ensure any child processes are also terminated
            os.killpg(0, signal.SIGINT)
        except:
            pass

if __name__ == "__main__":
    main()
