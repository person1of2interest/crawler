from web_crawler import Crawler
import asyncio
import csv
import sys


if __name__ == '__main__':
    max_hops = int(sys.argv[1])
    # batch_size можно настраивать
    crawler = Crawler(home_domain='spbu.ru', batch_size=8)
    asyncio.run(crawler.run(max_hops=max_hops))
    with open('logs/stats.csv', 'w') as stats_file:
        writer = csv.writer(stats_file, delimiter=',')
        writer.writerow([
            'Links',
            'External links',
            'Unique external inks',
            'Dead links',
            'Unique links to docs',
            'URLs visited'
        ])
        writer.writerow([
            crawler.links_count,
            crawler.ext_links_count,
            len(crawler.unique_ext_links),
            crawler.dead_links_count,
            len(crawler.links_to_docs),
            len(crawler.visited_urls)
        ])

