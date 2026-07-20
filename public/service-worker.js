const CACHE_NAME = 'louisiana911-shell-v4.2.1';
const CORE_ASSETS = [
  new URL('./', self.location).toString(),
  new URL('./index.html', self.location).toString(),
  new URL('./about/', self.location).toString(),
  new URL('./about.html', self.location).toString(),
  new URL('./reports/', self.location).toString(),
  new URL('./reports.html', self.location).toString(),
  new URL('./reports/monthly/', self.location).toString(),
  new URL('./monthly-reports.html', self.location).toString(),
  new URL('./styles.css', self.location).toString(),
  new URL('./manifest.webmanifest', self.location).toString(),
  new URL('./favicon-96x96.png', self.location).toString(),
  new URL('./images/louisiana911-icon-192.png', self.location).toString()
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(CORE_ASSETS))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET') return;
  const url = new URL(event.request.url);
  const indexUrl = new URL('./index.html', self.location).toString();

  if (url.origin !== self.location.origin) return;
  if (url.pathname.startsWith('/api/')) return;

  event.respondWith(
    fetch(event.request)
      .then((response) => {
        if (!response || response.status !== 200) return response;
        const copy = response.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
        return response;
      })
      .catch(async () => {
        const cached = await caches.match(event.request);
        if (cached) return cached;
        if (event.request.mode === 'navigate') return caches.match(indexUrl);
        return new Response('Offline', { status: 503, statusText: 'Offline' });
      })
  );
});
