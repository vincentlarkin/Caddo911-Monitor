import os
import tempfile
import unittest

_IMPORT_DB_PATH = os.path.join(
    tempfile.gettempdir(), f"louisiana911-canonical-tests-{os.getpid()}.db"
)
os.environ["LOUISIANA911_DB_PATH"] = _IMPORT_DB_PATH
import app


def tearDownModule():
    for suffix in ("", "-shm", "-wal"):
        try:
            os.remove(_IMPORT_DB_PATH + suffix)
        except FileNotFoundError:
            pass


class CanonicalOriginTests(unittest.TestCase):
    def setUp(self):
        self.client = app.app.test_client()

    def test_proxy_http_redirects_to_https_apex_and_preserves_query(self):
        response = self.client.get(
            '/reports/?month=2026-07',
            headers={
                'Host': 'louisiana911.com',
                'X-Forwarded-Proto': 'http',
            },
        )

        self.assertEqual(301, response.status_code)
        self.assertEqual(
            'https://louisiana911.com/reports/?month=2026-07',
            response.headers['Location'],
        )

    def test_www_redirects_to_apex_even_when_proxy_uses_https(self):
        response = self.client.get(
            '/about/',
            headers={
                'Host': 'www.louisiana911.com',
                'X-Forwarded-Proto': 'https',
            },
        )

        self.assertEqual(301, response.status_code)
        self.assertEqual(
            'https://louisiana911.com/about/',
            response.headers['Location'],
        )

    def test_https_apex_is_not_redirected(self):
        response = self.client.get(
            '/',
            headers={
                'Host': 'louisiana911.com',
                'X-Forwarded-Proto': 'https',
            },
        )

        self.assertEqual(200, response.status_code)

    def test_local_development_host_is_not_redirected(self):
        response = self.client.get('/', headers={'Host': 'localhost'})

        self.assertEqual(200, response.status_code)

    def test_coverage_pages_are_public_html(self):
        expected_pages = {
            '/coverage/': 'Louisiana 911 Coverage by City and Parish',
            '/caddo911/': 'Caddo 911 Live Calls',
            '/coverage/baton-rouge/': 'Baton Rouge Traffic Incidents',
            '/coverage/lafayette/': 'Lafayette Parish Traffic Incidents',
            '/coverage/new-orleans/': 'New Orleans NOPD Calls for Service',
        }

        for path, expected_title in expected_pages.items():
            with self.subTest(path=path):
                response = self.client.get(path, headers={'Host': 'localhost'})
                self.assertEqual(200, response.status_code)
                self.assertIn('text/html', response.content_type)
                self.assertIn(expected_title, response.get_data(as_text=True))

    def test_coverage_routes_without_slashes_redirect_permanently(self):
        for path in (
            '/coverage',
            '/coverage/baton-rouge',
            '/coverage/lafayette',
            '/coverage/new-orleans',
        ):
            with self.subTest(path=path):
                response = self.client.get(path, headers={'Host': 'localhost'})
                self.assertEqual(301, response.status_code)
                self.assertEqual(f'{path}/', response.headers['Location'])

    def test_versioned_shell_assets_are_immutable(self):
        for path in (
            '/styles.css?v=4.2.1',
            '/service-worker.js?v=4.2.1',
            '/manifest.webmanifest?v=4.2.1',
        ):
            with self.subTest(path=path):
                response = self.client.get(path, headers={'Host': 'localhost'})
                self.assertEqual(200, response.status_code)
                self.assertEqual(
                    'public, max-age=31536000, immutable',
                    response.headers['Cache-Control'],
                )

    def test_html_is_not_marked_immutable(self):
        response = self.client.get('/coverage/', headers={'Host': 'localhost'})

        self.assertEqual(200, response.status_code)
        self.assertNotIn('immutable', response.headers.get('Cache-Control', ''))


if __name__ == '__main__':
    unittest.main()
