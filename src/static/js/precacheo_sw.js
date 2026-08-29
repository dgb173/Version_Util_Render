const STATIC_CACHE = 'precacheo-static-v3';
const API_CACHE = 'precacheo-api-v3';

const OFFLINE_SHELL = [
    '/precacheo',
    '/html_offline',
    '/html_offline/offline_precacheo_simple.html',
    '/static/js/pattern_explorer_modal.js',
    'https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css',
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css',
    'https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js',
];

const CDN_HOSTS = new Set([
    'cdn.jsdelivr.net',
    'cdnjs.cloudflare.com',
]);

async function precacheShell() {
    const cache = await caches.open(STATIC_CACHE);
    await Promise.allSettled(
        OFFLINE_SHELL.map(async (url) => {
            try {
                const request = url.startsWith('http')
                    ? new Request(url, { mode: 'no-cors' })
                    : new Request(url, { cache: 'reload' });
                const response = await fetch(request);
                if (response && (response.ok || response.type === 'opaque')) {
                    await cache.put(request, response.clone());
                }
            } catch (_) {
                // Best effort: continue caching other files.
            }
        })
    );
}

self.addEventListener('install', (event) => {
    event.waitUntil((async () => {
        await precacheShell();
        self.skipWaiting();
    })());
});

self.addEventListener('activate', (event) => {
    event.waitUntil((async () => {
        const keep = new Set([STATIC_CACHE, API_CACHE]);
        const keys = await caches.keys();
        await Promise.all(keys.filter((key) => !keep.has(key)).map((key) => caches.delete(key)));
        await self.clients.claim();
    })());
});

async function cacheFirst(request, cacheName) {
    const cache = await caches.open(cacheName);
    const cached = await cache.match(request);
    if (cached) return cached;

    try {
        const response = await fetch(request);
        if (response && (response.ok || response.type === 'opaque')) {
            await cache.put(request, response.clone());
        }
        return response;
    } catch (_) {
        return cached || Response.error();
    }
}

async function networkFirstPage(request) {
    const cache = await caches.open(STATIC_CACHE);
    const pathname = new URL(request.url).pathname || '/';
    const fallbackKey = pathname.startsWith('/html_offline') ? '/html_offline' : '/precacheo';
    try {
        const response = await fetch(request);
        if (response && response.ok) {
            await cache.put(fallbackKey, response.clone());
            await cache.put(request, response.clone());
        }
        return response;
    } catch (_) {
        return (
            (await cache.match(request)) ||
            (await cache.match(fallbackKey)) ||
            (await cache.match('/precacheo')) ||
            Response.error()
        );
    }
}

async function networkFirstPrecacheList(request) {
    const cache = await caches.open(API_CACHE);
    try {
        const response = await fetch(request);
        if (response && response.ok) {
            await cache.put('/api/precacheo_list', response.clone());
            await cache.put(request, response.clone());
        }
        return response;
    } catch (_) {
        const cached = await cache.match(request) || await cache.match('/api/precacheo_list');
        if (cached) {
            try {
                const payload = await cached.clone().json();
                payload.offline = true;
                payload.cache_fallback = true;
                return new Response(JSON.stringify(payload), {
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Precacheo-Cache-Fallback': '1',
                    },
                    status: 200,
                });
            } catch (_) {
                // Do not present an invalid legacy response as current data.
            }
        }
        return new Response(JSON.stringify({ matches: [], offline: true }), {
            headers: { 'Content-Type': 'application/json' },
            status: 200,
        });
    }
}

self.addEventListener('fetch', (event) => {
    const { request } = event;
    if (request.method !== 'GET') return;

    const url = new URL(request.url);
    const sameOrigin = url.origin === self.location.origin;

    if (sameOrigin && url.pathname === '/api/precacheo_list') {
        event.respondWith(networkFirstPrecacheList(request));
        return;
    }

    if (
        sameOrigin &&
        request.mode === 'navigate' &&
        (url.pathname === '/precacheo' || url.pathname === '/' || url.pathname === '/html_offline')
    ) {
        event.respondWith(networkFirstPage(request));
        return;
    }

    if (sameOrigin && url.pathname.startsWith('/static/')) {
        event.respondWith(cacheFirst(request, STATIC_CACHE));
        return;
    }

    if (!sameOrigin && CDN_HOSTS.has(url.host)) {
        event.respondWith(cacheFirst(request, STATIC_CACHE));
    }
});
