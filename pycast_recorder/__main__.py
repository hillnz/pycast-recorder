import asyncio
import logging
import os
import signal

from .recorder import Recorder


def main():
    try:
        try:
            os.setpgrp()
        except:
            pass # Not allowed e.g. in Docker
        asyncio.run(Recorder().run())
    finally:
        try:
            # Ensure any child processes are also terminated
            os.killpg(0, signal.SIGINT)
        except:
            pass

if __name__ == "__main__":
    LOGLEVEL = os.environ.get('LOGLEVEL', 'WARNING').upper()
    logging.basicConfig(level=LOGLEVEL)
    DEBUG = os.environ.get('DEBUG', '')
    if DEBUG:
        for name in DEBUG.split(','):
            logging.getLogger(name).setLevel('DEBUG')

    main()
