import asyncio
import logging
from itertools import takewhile
from time import time

import aiohttp

log = logging.getLogger(__name__)

MAGIC = '#EXTM3U'

class M3u8:

    def __init__(self, stream_url):
        self.stream_url = stream_url

    async def _read_stream_inf(_, url_line):
        """Example:
            #EXT-X-STREAM-INF:BANDWIDTH=33000,CODECS="mp4a.40.5"
            https://url-to-another-stream.m3u8
        """
        m3u8 = M3u8(url_line)
        async for result in m3u8.read_song_info():
            yield result

    def _read_inf(_, tag_line, url_line):
        tag_map = {}
        tag_map['file'] = url_line

        chars = iter(tag_line)
        pop = lambda: next(chars, None)
        until = lambda c: ''.join(takewhile(lambda d: c != d, chars))

        # duration
        tag_map['duration'] = float(until(','))

        eol = False
        while not eol:
            # key
            key = until('=')
            
            # value
            value = ''
            escape = False
            quote = False
            while not eol:
                c = pop()
                if not c:
                    eol = True
                    break
                if escape:
                    value += c
                    escape = False
                elif c == '\\':
                    escape = True
                elif quote:
                    if c == '"':
                        quote = False
                    else:
                        value += c
                elif c == '"':
                    quote = True
                elif c == ',':
                    break

            tag_map[key] = value

        return tag_map

    async def read_song_info(self):
        recent = [None] * 20
        def not_recent(item):
            file = item['file']
            if file in recent:
                return False
            recent.append(file)
            recent.pop(0)
            return True

        async with aiohttp.ClientSession() as http:
            while True:
                target_duration = 5
                response = await http.get(self.stream_url)
                async def read(n=-1): 
                    return (await response.content.read(n)).decode().strip()
                async def readline():
                    return (await response.content.readline()).decode().strip()

                header = await read(len(MAGIC))
                if header != MAGIC:
                    raise Exception('Not an m3u8 stream')

                await readline() # To consume rest of header line
                line2 = await readline()
                while True:
                    line1 = line2
                    line2 = await readline()
                    if not line1:
                        break
                    try:
                        tag, value = tuple(line1.split(':', maxsplit=1))
                    except ValueError:
                        tag, value = line1, ''
                    if tag == '#EXT-X-STREAM-INF':
                        async for item in self._read_stream_inf(line2):
                            if not_recent(item):
                                yield item
                    elif tag == '#EXT-X-TARGETDURATION':
                        target_duration = min(target_duration, max(int(value), 1))
                    elif tag == '#EXTINF': 
                        inf = self._read_inf(value, line2)
                        target_duration = max(0, min(target_duration, inf.get('duration', target_duration)) - 1)
                        if not_recent(inf):
                            start = time()
                            yield inf
                            end = time()
                            target_duration = max(0, target_duration - (end - start))

                await asyncio.sleep(target_duration)

if __name__ == "__main__":

    async def main():
        m3u8 = M3u8('https://ais-nzme.streamguys1.com/nz_009/playlist.m3u8')
        async for item in m3u8.read_song_info():
            print(item)

    asyncio.run(main())
