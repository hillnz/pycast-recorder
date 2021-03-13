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
    codec: str
    duration: float
    probe_score: int
    sample_rate: int

async def get_format(filepath):
    try:
        proc = await asyncio.create_subprocess_exec(
            'ffprobe', 
            '-v', 'error', '-show_format', '-show_streams', '-of', 'json',
            filepath,
            stdout=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        result = stdout.decode()
        data = json.loads(result)
        format = data['format']
        stream = data['streams'][0]
        return FfmpegFormat(
            name=format['format_name'],
            codec=stream['codec_name'],
            duration=float(format['duration']),
            probe_score=format['probe_score'],
            sample_rate=int(stream['sample_rate'])
        )
    except Exception as e:
        log.debug('get_format exception')
        log.debug(e)
        return None

def format_metadata(title: str, artist: str, duration: int, chapters: List[Tuple[int, str]]):
    def escape(s):
        return ''.join((
            f'\\{c}' if c in [ '=', ';', '#', '\\', '\n' ] else c
            for c in str(s)
        ))
    text = StringIO()
    line = lambda s: text.write(escape(s) + '\n')
    tag = lambda k, v: text.write(f'{escape(k)}={escape(v)}\n')
    
    text.write(';FFMETADATA1\n')
    tag('title', title)
    tag('artist', artist)

    def this_and_next(iterable):
        iterable = iter(iterable)
        curr = next(iterable)
        for item in iterable:
            yield curr, item
            curr = item
        yield curr, None

    for this, after in this_and_next(chapters):
        start, title = this
        if after:
            end, _ = after
        else:
            end = duration

        line('[CHAPTER]')
        tag('TIMEBASE', '1/1000')
        tag('START', int(start * 1000))
        tag('END', int(end * 1000))
        tag('title', title)

    return text.getvalue()

async def convert(source, dest, codec, bitrate, format, metadata=None, in_format=None):
    proc: asyncio.Process = None
    try:
        log.info(f'convert {source} to {dest} ({codec}, {bitrate})')
        args = ['ffmpeg']
        if in_format:
            args += [ '-f', in_format ]
        args += [ '-i', source ]
        if metadata:
            args += [ '-i', metadata, '-map_metadata', '1' ]
        args += [ '-acodec', codec ]
        if codec != 'copy':
            args += [ '-b:a', bitrate, '-flush_packets', '1' ]
        args += [ '-f', format, '-y', dest ]
        full_cmd = ' '.join((shlex.quote(s) for s in args))
        log.debug(full_cmd)
        proc: Process = await asyncio.create_subprocess_exec(*args, stdout=PIPE, stderr=PIPE, limit=64*1024)

        async def iter_out(chunk_size=64 * 1024):
            while True:
                chunk = await proc.stdout.read(chunk_size)
                if not chunk:
                    break
                yield chunk
        
        async def read_stderr():
            while not proc.stderr.at_eof():
                line = await proc.stderr.readline()
                if line:
                    log.debug(line)
        stderr_task = asyncio.create_task(read_stderr())

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

async def convert_file(source, dest, codec, bitrate, format, metadata=None, in_format=None):
    async for _ in convert(source, dest, codec, bitrate, format, metadata=metadata, in_format=None):
        pass

if __name__ == '__main__':
    print(format_metadata('hello', 'world', 60, [
        (0, 'chapter 1'), (10, 'chapter 2'), (30, 'chapter 3')
    ]))
