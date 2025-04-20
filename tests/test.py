import unittest
from unittest.mock import AsyncMock, patch, MagicMock
from code.web_crawler import Crawler

class TestCrawler(unittest.TestCase):

    def setUp(self):
        self.crawler = Crawler()

    def test_initialization(self):
        self.assertEqual(self.crawler.home_domain, 'spbu.ru')
        self.assertEqual(self.crawler.batch_size, 5)
        self.assertEqual(len(self.crawler.visited_urls), 1)
        self.assertEqual(self.crawler.ext_links_count, 0)
        self.assertEqual(self.crawler.dead_links_count, 0)
        self.assertEqual(len(self.crawler.unique_ext_links), 0)
        self.assertEqual(len(self.crawler.links_to_docs), 0)
        self.assertIn('https://spbu.ru', self.crawler.visited_urls)

    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_download_url_success(self, mock_get):
        mock_response = AsyncMock()
        mock_response.raise_for_status = AsyncMock()
        mock_response.text = AsyncMock(return_value='<html>Some content</html>')
        mock_get.return_value = mock_response

        result = await self.crawler.download_url(MagicMock(), 'https://spbu.ru')
        self.assertEqual(result, '<html>Some content</html>')

    @patch('aiohttp.ClientSession.get', new_callable=AsyncMock)
    async def test_download_url_failure(self, mock_get):
    	dead_links_count = self.crawler.dead_links_count
    	mock_get.side_effect = Exception("Network error")
    	# one dead link
    	result = await self.crawler.download_url(MagicMock(), 'https://spbu.ru')
    	self.assertIsNone(result)
    	self.assertEqual(self.crawler.dead_links_count, dead_links_count + 1)
    	# two dead links
    	result = await self.crawler.download_url(MagicMock(), 'https://spbu.ru/sveden')
    	self.assertIsNone(result)
    	self.assertEqual(self.crawler.dead_links_count, dead_links_count + 2)

    def test_get_linked_urls(self):
        html_content = '''
		<a href="/sveden">Link 1</a>
		Some text
		<a href="http://gismeteo.ru">Link 2</a>
        '''
        urls = list(self.crawler.get_linked_urls('https://spbu.ru', html_content))
        self.assertIn('https://spbu.ru/sveden', urls)
        self.assertIn('http://gismeteo.ru', urls)

    def test_add_url_to_visit(self):
        self.crawler.add_url_to_visit('https://spbu.ru/sveden')
        self.assertIn('https://spbu.ru/sveden', self.crawler.urls_to_visit)

        # test duplicate URL
        self.crawler.add_url_to_visit('https://spbu.ru/sveden')
        self.assertEqual(len(self.crawler.urls_to_visit), 2)

        # test external URL
        self.crawler.add_url_to_visit('http://gismeteo.ru')
        self.assertIn('http://gismeteo.ru', self.crawler.unique_ext_links)
        self.assertEqual(self.crawler.ext_links_count, 1)

    @patch('web_crawler.Crawler.download_url', new_callable=AsyncMock)
    @patch('web_crawler.Crawler.get_linked_urls', return_value=['/page1', '/page2'])
    async def test_crawl(self, mock_get_linked_urls, mock_download_url):
        # crawl a page with two subpages
        mock_download_url.return_value = '<html>Some content</html>'
        await self.crawler.crawl(MagicMock(), 'https://spbu.ru')
        self.assertIn('https://spbu.ru/page1', self.crawler.urls_to_visit)
        self.assertIn('https://spbu.ru/page2', self.crawler.urls_to_visit)
        # crawl a document
        links_to_docs = self.crawler.links_to_docs
        await self.crawler.crawl(
	     MagicMock(),
	     'https://acm.math.spbu.ru/~sk1/courses/2122f_au2/conspect/conspect.pdf'
        )
        self.assertEqual(self.crawler.links_to_docs, links_to_docs + 1)

    @patch('aiohttp.ClientSession')
    async def test_run(self, mock_session):
        # mock the session and its methods
        mock_session.return_value.__aenter__.return_value = MagicMock()
        await self.crawler.run(max_hops=10)
        self.assertTrue(len(self.crawler.visited_urls) >= 10)

if __name__ == '__main__':
    unittest.main()

