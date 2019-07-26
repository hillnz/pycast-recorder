import asyncio
import logging

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
        log.debug(f'ffprobe: {result}')
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

async def convert(source, dest, format, bitrate):
    proc: asyncio.Process = None
    try:
        log.debug(f'convert {source} to {dest} ({format}, {bitrate})')
        proc = await asyncio.create_subprocess_exec(
            'ffmpeg',
            '-i', source, '-acodec', format, '-b:a', bitrate, '-loglevel', 'error', '-nostats', '-flush_packets', '1', '-y', dest
        )
        await proc.communicate()
        log.debug('convert finished')
    except:
        if proc:
            try:
                proc.terminate()
            except ProcessLookupError: # already dead
                pass
        raise
    finally:
        log.debug('convert completed')
