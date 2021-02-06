import asyncio
import json
import logging
import os
import re
import signal
import sys
from datetime import date, datetime, time, timedelta
from email.utils import formatdate as formatemaildate
from glob import glob
from os import listdir, path, read
from typing import List

from aiohttp import web
from lxml import etree
from lxml.builder import E, ElementMaker
from pydantic import AnyHttpUrl, BaseModel, validator
from ruamel.yaml import YAML

from . import ffmpeg

LAUNCH_TIME = datetime.now()

LOGLEVEL = os.environ.get('LOGLEVEL', 'WARNING').upper()
logging.basicConfig(level=LOGLEVEL)
log = logging.getLogger(__name__)
if DEBUG := os.environ.get('DEBUG', ''):
    for name in DEBUG.split(','):
        logging.getLogger(name).setLevel('DEBUG')

CONFIG_FILE = os.environ.get('PYCAST_SHOWS', 'shows.yaml')

RECORDING_WATCHDOG_TIME = 10
DATE_FORMAT = '%Y%m%d%H%M'
FILE_NAME = '{name}_{start}_{end}'
TMP_FORMAT = 'mpegts'
TMP_EXT = '.ts'
OUT_EXT = os.environ.get('PYCAST_EXT', '.m4a')
OUT_FORMAT = os.environ.get('PYCAST_FILE_FORMAT', 'ipod')
OUT_CODEC = os.environ.get('PYCAST_FORMAT', 'aac')
OUT_BITRATE = os.environ.get('PYCAST_BITRATE', '128k')
RE_FILE_NAME = re.compile(r'(?P<name>.+)_(?P<start>\d{12})_(?P<end>\d{12})')

# TODO
TEMP_DIR = os.environ.get('PYCAST_TEMP', '/tmp/recording')
OUT_DIR = os.environ.get('PYCAST_OUT', '/tmp/recordings')
HTTP_PORT = os.environ.get('PYCAST_PORT', 80)
HTTP_BASE = os.environ.get('PYCAST_HTTPBASE', 'http://localhost/files/')

# Build weekday names dynamically so they're in the right locale
SOME_MONDAY = datetime(2021, 1, 25)
DAYS_OF_WEEK = { ((SOME_MONDAY + timedelta(days=n)).strftime('%A').lower()): n for n in range(7) }

class File:

    def __init__(self, filename):
        self.filename = filename
        self.basename = path.basename(self.filename)
        match = RE_FILE_NAME.search(self.basename)
        if not match or self.filename.endswith('.metadata') or self.filename.endswith('.playlist'):
            raise ValueError()
        self.name, start, end = match.group('name', 'start', 'end')
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

class ShowSchedule(BaseModel):
    every: List[int]
    start: time
    end: time

    @validator('every', pre=True, each_item=True)
    def check_every(cls, value):
        try:
            return DAYS_OF_WEEK[value]
        except KeyError:
            raise ValueError(f'{value} is not a day of the week')

class Show(BaseModel):
    name: str
    stream: AnyHttpUrl
    schedule: ShowSchedule

    @property
    def slug(self) -> str:
        return ''.join([c if c.isalpha() or c.isdigit() else '-' for c in self.name])

class Config(BaseModel):
    shows: List[Show]
config: Config = None

class Recording:
    task: asyncio.Task = None
    output_file = ''
    last_output_file_size = 0
    last_output_file_time = datetime.now()

recording_tasks: dict[Show, Recording] = {}

def get_filename(show_name, start, end, ext):
    start_f = datetime.strftime(start, DATE_FORMAT)
    end_f = datetime.strftime(end, DATE_FORMAT)
    return FILE_NAME.format(name=show_name, start=start_f, end=end_f) + ext

async def read_show_files(files, name=None, condition=lambda _: True, cache_metadata=True):
    result: List[File] = []
    for f in files:
        file = await File.try_parse(f)
        if file and (not name or file.name == name) and condition(file):
            await file.populate_metadata(cache_metadata)
            result.append(file)
    result.sort(key=lambda f: f.start)
    return result

async def get_show_files(dir_path, name=None, condition=lambda _: True, cache_metadata=True):
    return await read_show_files([path.join(dir_path, p) for p in listdir(dir_path)], name, condition, cache_metadata)

async def monitor_recordings():
    for _, recording in recording_tasks.items():
        if recording.task.done():
            continue
        secs_since_last_update = (datetime.now() - recording.last_output_file_time).total_seconds()
        if secs_since_last_update >= RECORDING_WATCHDOG_TIME:
            try:
                new_size = os.stat(recording.output_file).st_size
                if new_size == recording.last_output_file_size:
                    log.warning(f'No change in filesize for {recording.output_file}. Current recording process will be killed')
                    recording.task.cancel()
                else:
                    recording.last_output_file_size = new_size
                recording.last_output_file_time = datetime.now()
            except FileNotFoundError:
                if secs_since_last_update >= (RECORDING_WATCHDOG_TIME * 2):
                    log.warning(f'File {recording.output_file} still has not appeared after {secs_since_last_update}s. Cancelling recording task.')
                    recording.task.cancel()
                else:
                    log.warning(f'File {recording.output_file} has not appeared yet')

async def record(show: Show, output_file: str, start_time: datetime, end_time: datetime):
    """Actually record. Runs as long as the recording does, completes when finished."""
    log.debug(f'record({show.slug}, {output_file}, {start_time}, {end_time}')
    try:
        if end_time > datetime.now():
            rec_seconds = (end_time - datetime.now()).total_seconds()
            try:
                log.info(f'Recording {show.name}')
                await asyncio.wait_for(ffmpeg.convert(show.stream, output_file, OUT_CODEC, OUT_BITRATE, TMP_FORMAT, append=True), timeout=rec_seconds)
            except asyncio.TimeoutError:
                pass
        else:
            log.info(f'{show.name} has already finished')
        log.info(f'Finished recording {show.name}')
        # Convert resulting file to its final form
        final_file = os.path.join(OUT_DIR, get_filename(show.slug, start_time, end_time, OUT_EXT))
        await ffmpeg.convert(output_file, final_file, 'copy', 0, OUT_FORMAT)
        os.remove(output_file)
    except asyncio.CancelledError:
        pass

async def start_recording(show, start_time: datetime, end_time: datetime) -> Recording:
    log.debug(f'start_recording({show})')

    if recording_tasks.get(show.slug, None):
        log.warning('Already recording ' + show.name)
        return

    output_file = path.join(TEMP_DIR, get_filename(show.slug, start_time, end_time, TMP_EXT))

    recording = Recording()
    recording.output_file = output_file
    recording.last_output_file_time = datetime.now()
    recording.task = asyncio.create_task(record(show, output_file, start_time, end_time))
    return recording

def get_show_by_slug(show_slug):
    for show in config.shows:
        if show.slug == show_slug:
            return show
    raise KeyError()

async def finalise_recordings():
    """Clean up stray files presumably left behind by failed recordings"""
    
    try:
        for stray in glob(os.path.join(TEMP_DIR, '*' + TMP_EXT)):
            if file := await File.try_parse(stray):
                show = None
                try:
                    show = get_show_by_slug(file.name)
                except KeyError:
                    log.warning(f'Temp file "{stray}" references an unknown show and will be deleted.')
                    os.remove(stray)
                    continue
                # Ignore if show is recording or end date is in the future
                if file.end > datetime.now() or recording_tasks.get(show.slug, None):
                    continue
                log.info(f'Temp file "{stray}" appears to have been left behind and will be recovered')
                recording_tasks[show.slug] = await start_recording(show, file.start, file.end)
            else:
                log.warning(f'Temp file "{stray}"\'s name is not parseable')
    except Exception as e:
        log.error('Failure during finalise_recordings. Exception follows')
        log.error(e)

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
        rss = await get_show_as_podcast(show_name)
        return web.Response(body=rss, content_type='application/rss+xml')
    return web.Response(status=404)

def get_live_shows():
    """Get shows that are currently live and their remaining times."""
    now = datetime.now()
    for show in config.shows:
        schedule = show.schedule
        show_days = set(sorted(schedule.every))

        abs_start = datetime.max
        abs_end = datetime.min
        for day in show_days:
            end_day = (day + 1) % 7 if schedule.end < schedule.start else day
            if day == end_day and day == now.weekday():
                # starts and ends today
                abs_start = datetime.combine(now, schedule.start)
                abs_end = datetime.combine(now, schedule.end)
            elif day != end_day and end_day == now.weekday():
                # started yesterday
                yesterday = now - timedelta(days=1)
                abs_start = datetime.combine(yesterday, schedule.start)
                abs_end = datetime.combine(now, schedule.end)
            elif day != end_day and day == now.weekday():
                # finishes tomorrow
                tomorrow = now + timedelta(days=1)
                abs_start = datetime.combine(now, schedule.start)
                abs_end = datetime.combine(tomorrow, schedule.end)
            if abs_start <= now <= abs_end:
                yield show, abs_start, abs_end
                break

def get_next_recording_time():
    now = datetime.now()
    next_start = datetime.max

    for show in config.shows:
        schedule = show.schedule
        show_days = set(sorted(schedule.every))
        for day in show_days:
            day_delta = day - now.weekday()
            if day_delta < 0:
                day_delta += 7
            start_date = datetime.combine(now + timedelta(days=day_delta), schedule.start)
            if start_date >= now:
                next_start = min(next_start, start_date)
    return next_start

async def check_and_start_recordings():
    """Create recording tasks for any shows that should currently have them, adding to the recording_tasks map."""
    next_end_time = datetime.max
    for show, start_time, end_time in get_live_shows():
        if show.slug not in recording_tasks.keys():
            log.info(f'Starting recording for "{show.name}"')
            if new_recording := await start_recording(show, start_time, end_time):
                recording_tasks[show.slug] = new_recording
            next_end_time = min(next_end_time, end_time)
    return next_end_time

def get_show_by_task(task):
    for slug, recording in recording_tasks.items():
        if recording.task == task:
            return get_show_by_slug(slug)

async def await_next_recording_end():
    tasks = [ r.task for _, r in recording_tasks.items() ]
    if tasks:
        done = set()
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            pass
        for task in done:
            show = get_show_by_task(task)
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as err:
                log.error(f'{show.name}\'s recording failed. It will be resumed shortly if needed. Exception follows.')
                log.error(err)        
            del recording_tasks[show.slug]
            return

def read_config():
    global config
    old_config = config
    with open(CONFIG_FILE, 'r') as f:
        config = Config(**YAML().load(f))
    if os.environ.get('PYCAST_TEST_MODE', False):
        config.shows.append(Show(
            name='test-test-test',
            stream='https://stream01.ungrounded.net/easylistening',
            schedule=ShowSchedule(
                every=['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'],
                start=(LAUNCH_TIME + timedelta(seconds=10)).time(),
                end=(LAUNCH_TIME + timedelta(minutes=1)).time())
        ))
    if old_config != config:
        log.debug(config)

async def run_tasks_forever():
    log.debug(f'run_tasks_forever()')

    pending = set()
    def schedule(coro):
        task = asyncio.create_task(coro)
        pending.add(task)

    while True:
        read_config()

        next_wakeup_time = datetime.now() + timedelta(days=1)
        def set_wakeup(new_time):
            nonlocal next_wakeup_time
            next_wakeup_time = min(next_wakeup_time, new_time)

        # Are existing recordings going ok?
        schedule(monitor_recordings())
        
        # Should anything be recording that isn't?
        set_wakeup(await check_and_start_recordings())

        # Any completed (but lost) recordings to be finalised?
        schedule(finalise_recordings())

        # When is the next recording?
        set_wakeup(get_next_recording_time())
        next_end_task = None
        if recording_tasks:
            set_wakeup(datetime.now() + timedelta(seconds=RECORDING_WATCHDOG_TIME))
            next_end_task = asyncio.create_task(await_next_recording_end())
            pending.add(next_end_task)

        sleep_seconds = max(0, (next_wakeup_time - datetime.now()).total_seconds())
        if sleep_seconds > RECORDING_WATCHDOG_TIME:
            log.info(f'Sleeping for {sleep_seconds} seconds...')
        sleep_task = asyncio.create_task(asyncio.sleep(sleep_seconds))
        pending.add(sleep_task)
        
        while True:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
            if sleep_task.done() or (next_end_task and next_end_task.done()):
                break
        if next_end_task and not next_end_task.done():
            next_end_task.cancel()
            try:
                await next_end_task
            except asyncio.CancelledError:
                pass

async def run():
    log.debug('Debug logging enabled')

    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)

    read_config()

    scheduler_task = asyncio.create_task(run_tasks_forever())

    # HTTP server
    server = web.Application()
    server.add_routes([web.get('/shows/{name}', get_feed_http)])
    server.router.add_static('/files', OUT_DIR)
    runner = web.AppRunner(server)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', HTTP_PORT)
    site_task = asyncio.create_task(site.start())
    
    try:
        for task in asyncio.as_completed([scheduler_task, site_task]):
            await task
    except:
        log.critical('Unexpected exception!! Termination imminent.')
        raise

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
