const CACHE_NAME = 'caddo911-shell-v1';
const CORE_ASSETS = [
  new URL('./', self.location).toString(),
  new URL('./index.html', self.location).toString(),
  new URL('./styles.css', self.location).toString(),
  new URL('./manifest.webmanifest', self.location).toString(),
  new URL('./images/caddo911logo.png', self.location).toString(),
  new URL('./images/caddo911logo.webp', self.location).toString(),
  new URL('./images/cfd-fire.png', self.location).toString(),
  new URL('./images/sfd-fire.png', self.location).toString(),
  new URL('./images/spd-police.png', self.location).toString(),
  new URL('./images/cso-sheriff.png', self.location).toString()
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

  if (url.origin !== self.location.origin) return;
  if (url.pathname.startsWith('/api/')) return;

  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          const copy = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(new URL('./index.html', self.location).toString(), copy));
          return response;
        })
        .catch(() => caches.match(new URL('./index.html', self.location).toString()))
    );
    return;
  }

  event.respondWith(
    caches.match(event.request).then((cached) => cached || fetch(event.request))
  );
});
