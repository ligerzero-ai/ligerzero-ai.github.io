// ============================================================
// GitHub OAuth Configuration
// ============================================================
// To set up GitHub authentication:
//
// 1. Go to https://github.com/settings/developers
// 2. Click "New OAuth App"
// 3. Fill in:
//    - Application name: e.g. "Han Lin Mai - Data Explorer"
//    - Homepage URL: https://ligerzero-ai.github.io
//    - Authorization callback URL: https://ligerzero-ai.github.io/callback.html
// 4. Copy the Client ID and paste it below.
//
// NOTE: The client secret is NOT used client-side. For the
// token exchange you need a small proxy (see auth.js comments).
// ============================================================

const AUTH_CONFIG = {
    // Replace with your GitHub OAuth App Client ID
    clientId: 'YOUR_GITHUB_CLIENT_ID',

    // The URL GitHub redirects to after authorization
    redirectUri: window.location.origin + '/callback.html',

    // Scopes: 'read:user' is enough to verify identity
    scope: 'read:user',

    // Token exchange proxy URL.
    // Since GitHub OAuth requires a server-side token exchange (client_secret
    // must not be exposed in frontend code), you need a small proxy.
    //
    // Options:
    //   1. Cloudflare Worker (recommended for this site since you already use R2)
    //   2. Any serverless function (Vercel, Netlify, AWS Lambda)
    //   3. A lightweight proxy like https://github.com/nicedoc/oauth-proxy
    //
    // The proxy should accept POST { code } and return { access_token }.
    // See the README or oauth-proxy-worker.js for a Cloudflare Worker template.
    tokenProxyUrl: 'https://your-oauth-proxy.workers.dev/exchange',

    // Pages that require authentication (relative paths)
    protectedPages: [
        'data-explorer.html',
        'dataset-info.html',
        'benchmarks.html'
    ],

    // Key used for localStorage
    storageKey: 'gh_auth_token',
    userStorageKey: 'gh_auth_user',
};
