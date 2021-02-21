import asyncio
import json
import logging
import os
import shlex
from asyncio.subprocess import DEVNULL, PIPE, Process
from dataclasses import dataclass
from io import StringIO
from subprocess import CalledProcessError
from typing import List, Tuple

log = logging.getLogger(__name__)

@dataclass
class FfmpegFormat:
    name: str
    duration: float
    probe_score: int

async def get_format(filepath):
    try:
        proc = await asyncio.create_subprocess_exec(
            'ffprobe', 
            '-v', 'error', '-show_format', '-of', 'json',
            filepath,
            stdout=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        result = stdout.decode()
        data = json.loads(result)['format']
        return FfmpegFormat(
            name=data['format_name'],
            duration=float(data['duration']),
            probe_score=data['probe_score']
        )
    except Exception as e:
        log.debug('get_format exception')
        log.debug(e)
        return None

def format_metadata(title: str, artist: str, duration: int, chapters: List[Tuple[int, str]]):
    def escape(s):
        return ''.join((
            f'\\{c}' if c in [ '=', ';', '#', '\\', '\n' ] else c
            for c in s
        ))
    text = StringIO()
    w = lambda s: text.write(escape(s))
    l = lambda s: text.write(escape(s) + '\n')
    tag = lambda k, v: text.write(f'{escape(k)}={escape(v)}\n')
    
    l(';FFMETADATA1')
    tag('title', title)
    tag('artist', artist)

    def around(iterable):
        iterable = iter(iterable)
        prev = next(iterable)
        curr = next(iterable)
        yield None, prev, curr
        for item in iter:
            yield prev, curr, item
        yield curr, item, None

    # Loop runs one behind
    chapters.append(0,'')
    timecode = -1
    name = ''
    for prev, this, after in around(chapters):
        
        if timecode >= 0:
            l('[CHAPTER]')
            tag('TIMEBASE', '1/1000')
            tag('START', int(timecode / 1000))
            tag('')
        timecode = next_timecode
        name = next_name

        
    


async def convert(source, dest, codec, bitrate, format, append=False):
    if source == '-' and dest == '-':
        raise Exception(f"Converting from stdin to stdout is not supported because I cba implementing it")

    proc: asyncio.Process = None
    try:
        log.info(f'convert {source} to {dest} ({codec}, {bitrate})')
        args = ['ffmpeg', '-i', source, '-acodec', codec ]
        if codec != 'copy':
            args += [ '-b:a', bitrate, '-flush_packets', '1' ]
        args += [ '-f', format, '-y' ]
        if append:
            args += [ '-' ]
        else:
            args += [ dest ]
        full_cmd = ' '.join((shlex.quote(s) for s in args))
        log.debug(full_cmd)
        proc: Process = await asyncio.create_subprocess_exec(*args, stdin=PIPE, stdout=PIPE if append else DEVNULL, stderr=PIPE, limit=64*1024)

        async def iter_out(chunk_size=64 * 1024):
            while True:
                chunk = await proc.stdout.read(chunk_size)
                if not chunk:
                    break
                yield chunk

        stdout_task = None
        if append and dest != '-':
            async def read_stdout():
                with open(dest, 'ab') as f:
                    async for chunk in iter_out():
                        f.write(chunk)
                        f.flush()
            stdout_task = asyncio.create_task(read_stdout())
        
        async def read_stderr():
            while not proc.stderr.at_eof():
                line = await proc.stderr.readline()
                if line:
                    log.debug(line)
        stderr_task = asyncio.create_task(read_stderr())

        try:
            if source == '-': # if stdin source
                while proc.returncode is None:
                    chunk = yield
                    if chunk:
                        proc.stdin.write(chunk)
                        await proc.stdin.drain()
        except GeneratorExit:
            proc.stdin.write_eof()
            await proc.stdin.drain()

        if dest == '-': # stdout dest
            async for chunk in iter_out():
                yield chunk

        if await proc.wait() != 0:
            log.error('ffmpeg process failed')
            try:
                if os.stat(dest).st_size == 0:
                    os.remove(dest)
            except:
                pass
            raise CalledProcessError(proc.returncode, full_cmd, '', '')
        if append:
            await stdout_task
        await stderr_task        
        log.info('convert finished')
    except:
        if proc:
            try:
                proc.terminate()
            except ProcessLookupError: # already dead
                pass
        raise
    finally:
        log.info('convert completed')

async def convert_file(source, dest, codec, bitrate, format, append=False):
    async for _ in convert(source, dest, codec, bitrate, format, append):
        pass

if __name__ == '__main__':
    print(asyncio.run(get_format('/tmp/9j1HwAveS3Ap-322571921-4969.aac')))
