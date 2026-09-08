"""Route integration checks; no storage or AI services are contacted."""
import unittest
from html.parser import HTMLParser
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
from routers import screens


class AssetParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.assets = []

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        url = values.get('src') or values.get('href', '')
        if url.startswith('/static/react/assets/'):
            self.assets.append(url)


class ReactRoutesTest(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(screens.router)
        app.mount('/static', StaticFiles(directory='static'))
        self.client = TestClient(app)

    def test_library_and_assets(self):
        for path in ['/', '/folder?name=A1']:
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertIn('id="root"', response.text)
            self.assertEqual(response.headers['cache-control'], 'no-cache')
            parser = AssetParser()
            parser.feed(response.text)
            self.assertEqual(len(parser.assets), 2)
            for url in parser.assets:
                self.assertEqual(self.client.get(url).status_code, 200)

    def test_existing_screens(self):
        paths = ['/?mode=flash&deck=Example', '/folder?name=A1&legacy=1',
                 '/create', '/pdf', '/video', '/story', '/learn',
                 '/spelling', '/match', '/line']
        for path in paths:
            response = self.client.get(path)
            self.assertEqual(response.status_code, 200)
            self.assertNotIn('id="root"', response.text)
        self.assertIn('flashApp', self.client.get(paths[0]).text)

    def test_missing_build_falls_back(self):
        with patch.object(screens, 'REACT_INDEX', Path('static/react/missing-build.html')):
            self.assertIn('flashApp', self.client.get('/').text)
            self.assertNotIn('id="root"', self.client.get('/folder?name=A1').text)


if __name__ == '__main__':
    unittest.main()
