import asyncio
import aiohttp
import logging

log = logging.getLogger(__name__)

class M3u8:

    def __init__(self, stream_url):
        self.stream_url = stream_url
        self.target_duration = 5
        self.__http = None
        self.__response = None

    def _http(self):
        if not self.__http:
            raise Exception('No http session')
        return self.__http
    
    def _response(self):
        if not self.__response:
            raise Exception('No response')
        return self.__response

    async def _readline(self):
        return (await self._response().content.readline()).decode().strip()

    async def _read_stream_inf(self, line):
        url = await self._readline()
        m3u8 = M3u8(url)
        async for result in m3u8.read_song_info():
            yield result

    async def _read_targetduration(self, line):
        self.target_duration = max(int(line.split(':')[1]), 1)

    async def _read_inf(self, line):
        # TODO Is there an existing parser that can parse this format?
        
        line = list(line)
        tag_map = {}
        tag_map['file'] = await self._readline()
        pop = lambda: line.pop(0)

        try:
            # tag (discard)
            while pop() != ':':
                pass

            # duration (discard)
            while pop() != ',':
                pass

            while True:
                # key
                key = ''
                while (c := pop()) != '=':
                    key += c
                
                # value
                value = ''
                escape = False
                quote = False
                while True:
                    c = pop()
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

        except IndexError:
            if key and value:
                tag_map[key] = value

        if tag_map:
            yield tag_map

    async def read_song_info(self):
        if self.__http or self.__response:
            raise Exception('Already reading')

        try:
            async with aiohttp.ClientSession() as http:
                self.__http = http

                while True:
                    self.__response = await http.get(self.stream_url)

                    header = await self._readline()
                    if header.strip() != '#EXTM3U':
                        raise Exception('Not an m3u8 stream')

                    while line := await self._readline():
                        tag_map = {
                            '#EXT-X-STREAM-INF': self._read_stream_inf,
                            '#EXT-X-TARGETDURATION': self._read_targetduration,
                            '#EXTINF': self._read_inf
                        }
                        try:
                            f = next((f for t, f in tag_map.items() if line.startswith(t + ':')))
                            result = f(line)
                            try:
                                async for item in result:
                                    yield item
                            except TypeError:
                                await result
                        except StopIteration:
                            pass
                    
                    await asyncio.sleep(self.target_duration)
                    
        finally:
            self.__http = None
            self.__response = None

if __name__ == "__main__":

    async def main():
        m3u8 = M3u8('https://ais-nzme.streamguys1.com/nz_009/playlist.m3u8')
        async for item in m3u8.read_song_info():
            print(item)

    asyncio.run(main())
