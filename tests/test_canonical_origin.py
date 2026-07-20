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
            '/styles.css?v=4.2.2',
            '/service-worker.js?v=4.2.2',
            '/manifest.webmanifest?v=4.2.2',
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

    def test_map_markers_include_mobile_tap_target_and_incident_dialog(self):
        response = self.client.get('/', headers={'Host': 'localhost'})
        html = response.get_data(as_text=True)

        self.assertEqual(200, response.status_code)
        self.assertIn("const hitTarget = L.circleMarker", html)
        self.assertIn("radius: shouldUseMobileIncidentDialog() ? 18 : 10", html)
        self.assertIn("openIncidentDialog(activeIncident", html)
        self.assertIn("const marker = L.featureGroup([triangle, hitTarget])", html)

    def test_api_request_contexts(self):
        cross_site = self.client.get(
            '/api/incidents/active',
            headers={
                'Host': 'louisiana911.com',
                'X-Forwarded-Proto': 'https',
                'Sec-Fetch-Site': 'cross-site',
                'Sec-Fetch-Mode': 'cors',
            },
        )
        direct_navigation = self.client.get(
            '/api/incidents/active',
            headers={
                'Host': 'louisiana911.com',
                'X-Forwarded-Proto': 'https',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-Mode': 'navigate',
            },
        )
        headerless_client = self.client.get(
            '/api/incidents/active',
            headers={
                'Host': 'louisiana911.com',
                'X-Forwarded-Proto': 'https',
            },
        )

        self.assertEqual(403, cross_site.status_code)
        self.assertEqual(404, direct_navigation.status_code)
        self.assertEqual(404, headerless_client.status_code)

    def test_api_response_contract(self):
        headers = {
            'Host': 'louisiana911.com',
            'X-Forwarded-Proto': 'https',
            'Origin': 'https://louisiana911.com',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
        }
        active = self.client.get('/api/incidents/active', headers=headers)
        unbounded_history = self.client.get('/api/incidents/history', headers=headers)

        self.assertEqual(200, active.status_code)
        self.assertEqual('no-store, private', active.headers['Cache-Control'])
        self.assertEqual('noindex, nofollow, noarchive', active.headers['X-Robots-Tag'])
        self.assertEqual(400, unbounded_history.status_code)


if __name__ == '__main__':
    unittest.main()
