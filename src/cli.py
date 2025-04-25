# src/cli.py

import argparse
import json
import os
import asyncio
from tqdm import tqdm

from fetcher import smoke_test as fetcher_test, fetch_page
from parser import smoke_test as parser_test, extract_links, find_next_page
from extractor import smoke_test as extractor_test, extract_festival_info
from storage import smoke_test as storage_test, save_record, save_state, load_state


def run_tests():
    print("🧪 Running smoke tests…")
    fetcher_test()
    parser_test()
    extractor_test()
    storage_test()
    print("✅ All tests passed!")


async def fetch_batch(queue, visited, max_depth, output_file, batch_size=10):
    processed_batch = 0
    festivals_count = 0
    errors_count = 0

    with tqdm(total=min(len(queue), batch_size), desc="Crawling") as progress:
        batch_tasks = []
        batch_urls = []

        while queue and len(batch_tasks) < batch_size:
            url, depth = queue.pop(0)
            if url in visited or depth >= max_depth:
                continue

            visited.add(url)
            batch_urls.append((url, depth))
            batch_tasks.append(fetch_page(url))
            processed_batch += 1

        if batch_tasks:
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            for (url, depth), result in zip(batch_urls, results):
                try:
                    if isinstance(result, Exception):
                        raise result

                    status, html = result
                    if status == 200 and html:
                        info = extract_festival_info(html, url)
                        if info:
                            info['source_url'] = url
                            info['depth'] = depth
                            save_record(info, output_file)
                            festivals_count += 1

                        for link in extract_links(html, url):
                            if link not in visited and link not in [u for u, _ in queue]:
                                queue.append((link, depth + 1))

                        next_page = find_next_page(html, url)
                        if next_page and next_page not in visited and next_page not in [u for u, _ in queue]:
                            queue.insert(0, (next_page, depth))

                except Exception as e:
                    errors_count += 1
                    print(f"⚠️  Error processing {url}: {e}")
                    save_state(
                        {'url': url, 'error': str(e), 'depth': depth},
                        f"{output_file}.errors.jsonl"
                    )

                progress.update(1)

    return processed_batch, festivals_count, errors_count, queue, visited


async def crawl_async(seeds_file, state_file, output_file, max_depth=3, batch_size=10):
    state = load_state(state_file) or {}
    visited = set(state.get('visited', []))
    queue = state.get('queue', [])
    festivals = state.get('festivals', 0)
    errors = state.get('errors', 0)

    if not queue:
        with open(seeds_file, 'r', encoding='utf-8') as f:
            queue = [(url.strip(), 0) for url in f if url.strip()]

    processed, new_fests, new_errs, queue, visited = await fetch_batch(
        queue, visited, max_depth, output_file, batch_size
    )
    festivals += new_fests
    errors += new_errs

    save_state({
        'visited': list(visited),
        'queue': queue,
        'festivals': festivals,
        'errors': errors
    }, state_file)

    print(f"🔍 Batch done: {processed} pages, {new_fests} festivals, {new_errs} errors.")
    print(f"⏳ {len(queue)} URLs left in queue.")
    return processed


def main():
    p = argparse.ArgumentParser(description="Film Festival Deadline Crawler")
    p.add_argument('--test', action='store_true', help='Run smoke tests & exit')
    p.add_argument('--run', action='store_true', help='Run one batch')
    p.add_argument('--continuous', action='store_true', help='Keep running batches until queue empty')
    p.add_argument('--seeds', default='seeds.txt', help='Seed URLs file')
    p.add_argument('--state', default='state.json', help='Checkpoint file')
    p.add_argument('--output', default='data.jsonl', help='Output JSONL')
    p.add_argument('--max-depth', type=int, default=3, help='Max crawl depth')
    p.add_argument('--batch-size', type=int, default=10, help='URLs per batch')

    args = p.parse_args()

    if args.test:
        run_tests()
        return

    if args.run or args.continuous:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            total = 0
            while True:
                processed = loop.run_until_complete(
                    crawl_async(args.seeds, args.state, args.output,
                                args.max_depth, args.batch_size)
                )
                total += processed
                if not args.continuous or processed == 0:
                    break
        finally:
            loop.close()
        print(f"🏁 Crawl finished: processed {total} pages.")
    else:
        p.print_help()


if __name__ == '__main__':
    main()
