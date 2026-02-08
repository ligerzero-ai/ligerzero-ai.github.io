// ============================================================
// Cloudflare Worker: GitHub OAuth Token Exchange Proxy
// ============================================================
// Deploy this as a Cloudflare Worker to securely exchange
// GitHub OAuth authorization codes for access tokens.
//
// Setup:
//   1. Go to https://dash.cloudflare.com → Workers & Pages
//   2. Create a new Worker
//   3. Paste this code
//   4. Add environment variables:
//      - GITHUB_CLIENT_ID:     Your GitHub OAuth App Client ID
//      - GITHUB_CLIENT_SECRET: Your GitHub OAuth App Client Secret
//   5. Deploy and note the worker URL (e.g. https://your-oauth-proxy.workers.dev)
//   6. Update AUTH_CONFIG.tokenProxyUrl in auth-config.js
//   7. Update the ALLOWED_ORIGINS array below
// ============================================================

const ALLOWED_ORIGINS = [
    'https://ligerzero-ai.github.io',
    'http://localhost:8000',    // for local development
    'http://127.0.0.1:8000',
];

export default {
    async fetch(request, env) {
        // Handle CORS preflight
        if (request.method === 'OPTIONS') {
            return handleCORS(request);
        }

        // Only accept POST to /exchange
        const url = new URL(request.url);
        if (request.method !== 'POST' || url.pathname !== '/exchange') {
            return new Response('Not Found', { status: 404 });
        }

        const origin = request.headers.get('Origin');
        if (!ALLOWED_ORIGINS.includes(origin)) {
            return new Response('Forbidden', { status: 403 });
        }

        try {
            const { code } = await request.json();
            if (!code) {
                return jsonResponse({ error: 'Missing code parameter' }, 400, origin);
            }

            // Exchange the code for an access token
            const tokenResponse = await fetch('https://github.com/login/oauth/access_token', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                },
                body: JSON.stringify({
                    client_id: env.GITHUB_CLIENT_ID,
                    client_secret: env.GITHUB_CLIENT_SECRET,
                    code: code,
                }),
            });

            const tokenData = await tokenResponse.json();

            if (tokenData.error) {
                return jsonResponse({ error: tokenData.error_description || tokenData.error }, 400, origin);
            }

            return jsonResponse({ access_token: tokenData.access_token }, 200, origin);
        } catch (err) {
            return jsonResponse({ error: 'Internal server error' }, 500, origin);
        }
    },
};

function handleCORS(request) {
    const origin = request.headers.get('Origin');
    const headers = {
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Max-Age': '86400',
    };
    if (ALLOWED_ORIGINS.includes(origin)) {
        headers['Access-Control-Allow-Origin'] = origin;
    }
    return new Response(null, { status: 204, headers });
}

function jsonResponse(data, status, origin) {
    const headers = {
        'Content-Type': 'application/json',
    };
    if (origin && ALLOWED_ORIGINS.includes(origin)) {
        headers['Access-Control-Allow-Origin'] = origin;
    }
    return new Response(JSON.stringify(data), { status, headers });
}
