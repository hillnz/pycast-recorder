
import asyncio
import logging
import os
import socket
from contextlib import closing, contextmanager
from datetime import datetime, timedelta
from glob import glob
from shutil import rmtree
from tempfile import mkdtemp

import pytest

from pycast_recorder import ffmpeg, recorder
from pycast_recorder.recorder import Recorder

logging.getLogger('pycast_recorder.recorder').setLevel('DEBUG')

pytestmark = pytest.mark.anyio

@contextmanager
def make_temp_dir():
    d = mkdtemp()
    try:
        yield d
    finally:
        rmtree(d)

def get_random_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]        

async def schedule(test_seconds: int, wait=5):
    now = datetime.now()
    total = test_seconds + wait
    start = (now + timedelta(seconds=wait)).time()
    end = (now + timedelta(seconds=total)).time()
    TEST_NAME = 'test-test-test'
    config = recorder.Config(
        shows = [
            recorder.Show(
                name        = TEST_NAME,
                stream      = 'https://ais-nzme.streamguys1.com/nz_009/playlist.m3u8',
                schedule    = recorder.ShowSchedule(
                    every   = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'],
                    start   = start,
                    end     = end
                )
            )
        ]
    )
    with make_temp_dir() as rec_dir:
        config.recorder = recorder.RecorderConfig(out_dir=rec_dir)
        config.server = recorder.ServerConfig(http_port=get_random_port())
        instance = Recorder(config)
        recorder_task = asyncio.create_task(instance.run())
        await asyncio.sleep(total + 5)
        recorder_task.cancel()
        try:
            await recorder_task
        except asyncio.CancelledError:
            pass
        
        files = glob(os.path.join(rec_dir, '*.m4a'))
        assert len(files) == 1
        format = await ffmpeg.get_format(files[0])
        assert format.duration > 0
        assert abs(test_seconds - format.duration) < 20
        feed = str(await instance.get_show_as_podcast(TEST_NAME))
        assert str(config.server.http_base_url) in feed
        assert 'audio/mp4' in feed

async def test_basic():
    await schedule(10)

async def test_in_progress():
    await schedule(15, wait=-5)

async def test_longer():
    await schedule(600)
