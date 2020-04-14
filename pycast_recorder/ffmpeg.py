import asyncio
import logging
import shlex

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

async def create_part(source_parts: list, dest, start, end):
    log.debug(f'create_part from {source_parts} to {dest} starting at {start} to {end}')
    if len(source_parts) > 1:
        input = 'concat:' + '|'.join(source_parts)
    else:
        input = source_parts[0]

    proc = await asyncio.create_subprocess_exec(
        'ffmpeg',
        '-i', input, '-ss', _fmt_float(start), '-to', _fmt_float(end), '-acodec', 'copy', '-y', '-loglevel', 'error', '-nostats', dest)
    await proc.communicate()
    log.debug('create_part completed')

async def convert_with_stderr(source, dest, format, bitrate):
    proc: asyncio.Process = None
    try:
        log.info(f'convert {source} to {dest} ({format}, {bitrate})')
        args = ['ffmpeg', '-i', source, '-acodec', format, '-b:a', bitrate, '-flush_packets', '1', '-y', dest]
        log.debug(' '.join((shlex.quote(s) for s in args)))
        proc = await asyncio.create_subprocess_exec(*args, stderr=asyncio.subprocess.PIPE)

        stderr_available = asyncio.Event()
        stderr_buffer = []
        
        async def read_stderr():
            while not proc.stderr.at_eof():
                line = await proc.stderr.readline()
                if line:
                    log.debug(line)
                    stderr_buffer.append(line.decode().strip())
                    stderr_available.set()
            stderr_available.set()

        stderr_task = asyncio.create_task(read_stderr())

        while proc.returncode is None:
            _, pending = await asyncio.wait({ stderr_available.wait(), proc.wait(), stderr_task }, return_when=asyncio.FIRST_COMPLETED)
            for item in stderr_buffer:
                yield item
            stderr_buffer = []
            stderr_available.clear()
        
        # To mop up any remaining exceptions
        for t in asyncio.as_completed(pending):
            await t

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

async def convert(source, dest, format, bitrate):
    async for _ in convert_with_stderr(source, dest, format, bitrate):
        pass
