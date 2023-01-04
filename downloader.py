import asyncio
import tempfile
import argparse
import timeit
from pathlib import Path
from urllib.parse import urlparse

import aiohttp

async def main():
    '''
        Run parallel download
    '''
    #   CLI Argument parsing
    parser = argparse.ArgumentParser(
    description='''\
        Parallel Downloader.
        Download files from multiple urls in parallel.\
        '''
    )
    parser.add_argument('urls', type=str, nargs='+',
                        help='Url from where the file can be downloaded')
    parser.add_argument('-s', '--stream', type=int, default=10, dest='session_num',
                        help='number of stream to be generated per url. Each session download the total file size divided by the number of session',
                        metavar='STREAM_COUNT')
    parser.add_argument('-b', '--buffer', type=int, default=1024*5, dest='buffer_size',
                        help='buffer size per stream.How much data to be stored in memory before appended to temp file', metavar='BUFFER_SIZE')
    parser.add_argument('-d', '--directory', type=Path, default=Path(__file__).parent / 'saved_download', dest='output_dir',
                        help='directory where every downloaded file will be saved', metavar='DOWNLOAD_DIRECTORY')

    args = parser.parse_args()

    # Create necessary directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Run parallel download and track execution time
    start = timeit.default_timer()
    await parallel_download(**vars(args))
    end = timeit.default_timer()
    print()
    print(f'Finished in {end-start} second(s)')


async def parallel_download(urls, session_num=10, buffer_size=1024*5, output_dir=Path(__file__).parent/'saved_download'):
    '''
        Download multiple files from urls simultaneously.
        Return filepaths of downloaded files as a list.
    '''
    downloads = []  # List downloads to be run

    # Appending awaitable downloads to be passed to concurrent_download for each url
    for url in urls:
        parsed_url = urlparse(url)
        file_url = parsed_url.geturl()
        save_path = output_dir/Path(parsed_url.path).name
        downloads.append(concurrent_download(file_url, save_path, session_num, buffer_size))

    # Run each downloads
    saved_paths = await asyncio.gather(*downloads)

    return saved_paths  # Path where the downloaded file is located


async def concurrent_download(url, save_path, session_num=10, buffer_size=1024*5):
    '''
        Download a file asynchronously by dividing the file to multiple part and creating multiple stream for each part.
        Return filepath of downloaded file as a string.
    '''
    print(f'Getting file information from {url}')
    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url) as resp:
                file_length = int(resp.headers['Content-Length'])
    except aiohttp.ClientConnectionError:
        print(f"Couldn't connect to {url}")
        return
    except aiohttp.InvalidURL:
        print(f"URL {url} is invalid")
        return
    except KeyError:
        print(f"URL {url} does not contain a file")
        return

    chunk_size = file_length//(session_num-1)

    # Appending awaitable partial downloads to be passed to _partial_download for each byte range
    downloads = []
    for part_num, start in enumerate(range(0, file_length, chunk_size), 1):
        downloads.append(_partial_download(url, start, chunk_size-1, part_num, buffer_size))
    content = await asyncio.gather(*downloads)

    # Iterating through temp files written by _partial_download to concatenate their data and saving to output_dir
    print(f'Writing to {save_path}')
    with open(save_path, 'wb') as f:
        for part in content:
            with open(part, 'rb') as p:
                f.write(p.read())
        print(f'({_get_url_filename(url)}) saved download')

    return save_path


async def _partial_download(url, start_byte, chunk_size, part_num, buffer_size):
    '''
        Download part of a file.
        Return filepath of downloaded part as a string.
    '''

    # Initializing headers to download only a certain part of the file
    headers = {'Range': f'bytes={start_byte}-{start_byte+chunk_size}'}

    # Initializing save_path for temp file
    save_path = tempfile.gettempdir() + "/" + Path(urlparse(url).path).name + f'.part{part_num}'

    # Downloading byte range of file and write to temp file
    print(f'({_get_url_filename(url)}) starting download {headers["Range"]}')
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            with open(save_path, 'wb') as f:
                async for buffer in resp.content.iter_chunked(buffer_size):
                    f.write(buffer)
            print(f'({_get_url_filename(url)}) finished download {headers["Range"]}')

    return save_path    # Temp file location

def _get_url_filename(url):
    '''
        Helper function to get filename from url.
        Return file name.
    '''
    url = urlparse(url)
    filename = Path(url.path).name

    return filename

if __name__ == '__main__':
    asyncio.run(main())
