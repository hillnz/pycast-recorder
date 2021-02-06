import asyncio
import logging
import os
import shlex
from asyncio.subprocess import Process, PIPE, DEVNULL
from subprocess import CalledProcessError

log = logging.getLogger(__name__)

def _fmt_float(num):
    return '{:.3f}'.format(num)

async def get_duration(filepath):
    try:
        proc = await asyncio.create_subprocess_exec(
            'ffprobe', 
            '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1',
            filepath,
            stdout=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        result = stdout.decode()
        # log.debug(f'ffprobe: {result}')
        return float(result)
    except Exception as e:
        log.debug('get_duration exception')
        log.debug(e)
        return 0

async def convert_with_stderr(source, dest, codec, bitrate, format, append=False):
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
        proc: Process = await asyncio.create_subprocess_exec(*args, stdout=PIPE if append else DEVNULL, stderr=PIPE)

        stderr_available = asyncio.Event()
        stderr_buffer = []

        stdout_task = None
        if append:
            async def read_stdout():
                with open(dest, 'ab') as f:
                    while chunk := await proc.stdout.read(64 * 1024):
                        f.write(chunk)
            stdout_task = asyncio.create_task(read_stdout())
        
        async def read_stderr():
            while not proc.stderr.at_eof():
                line = await proc.stderr.readline()
                if line:
                    log.debug(line)
                    stderr_buffer.append(line.decode().strip())
                    stderr_available.set()
            stderr_available.set()

        stderr_task = asyncio.create_task(read_stderr())

        full_stderr = ''
        pending = set()
        while proc.returncode is None:
            pending = { stderr_available.wait(), proc.wait(), stderr_task }
            if append:
                pending.add(stdout_task)
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
            for item in stderr_buffer:
                full_stderr += item + '\n'
                yield item
            stderr_buffer = []
            stderr_available.clear()
        
        # To mop up any remaining exceptions
        for t in asyncio.as_completed(pending):
            await t

        if proc.returncode != 0:
            log.error('ffmpeg process failed')
            try:
                if os.stat(dest).st_size == 0:
                    os.remove(dest)
            except:
                pass
            raise CalledProcessError(proc.returncode, full_cmd, '', full_stderr)
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

async def convert(source, dest, codec, bitrate, format, append=False):
    async for _ in convert_with_stderr(source, dest, codec, bitrate, format, append):
        pass
