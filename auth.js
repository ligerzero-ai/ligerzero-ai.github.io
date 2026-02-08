// ============================================================
// GitHub OAuth Authentication Module
// ============================================================
// This module gates protected pages behind GitHub login.
// It relies on auth-config.js being loaded first.
//
// Flow:
//   1. Page loads → auth.js checks if current page is protected
//   2. If protected, checks localStorage for a valid GitHub token
//   3. If no token, shows a login overlay with "Sign in with GitHub"
//   4. User clicks → redirected to GitHub OAuth authorize endpoint
//   5. GitHub redirects back to callback.html with ?code=...
//   6. callback.html sends the code to a proxy to exchange for a token
//   7. Token is stored in localStorage; user is redirected to original page
//   8. On subsequent visits, the token is validated via GitHub API
// ============================================================

const GitHubAuth = {
    // ---- State ----
    _user: null,
    _token: null,
    _initialized: false,

    // ---- Public API ----

    /**
     * Initialize auth. Call on DOMContentLoaded.
     * Returns a promise that resolves to { authenticated, user } or shows the login gate.
     */
    async init() {
        if (this._initialized) return;
        this._initialized = true;

        this._token = localStorage.getItem(AUTH_CONFIG.storageKey);
        const cachedUser = localStorage.getItem(AUTH_CONFIG.userStorageKey);
        if (cachedUser) {
            try { this._user = JSON.parse(cachedUser); } catch (_) { /* ignore */ }
        }

        const isProtected = this._isProtectedPage();

        if (isProtected) {
            if (this._token) {
                // Validate the token
                const valid = await this._validateToken();
                if (valid) {
                    this._showAuthUI();
                    return { authenticated: true, user: this._user };
                } else {
                    // Token expired or revoked
                    this._clearAuth();
                }
            }
            // Not authenticated → show login gate
            this._showLoginGate();
            return { authenticated: false, user: null };
        } else {
            // Non-protected page: just show auth status in sidebar if logged in
            this._showAuthUI();
            return { authenticated: !!this._token, user: this._user };
        }
    },

    /**
     * Start the GitHub OAuth login flow.
     */
    login() {
        // Store the current page so we can redirect back after login
        sessionStorage.setItem('gh_auth_return', window.location.href);

        const params = new URLSearchParams({
            client_id: AUTH_CONFIG.clientId,
            redirect_uri: AUTH_CONFIG.redirectUri,
            scope: AUTH_CONFIG.scope,
            state: this._generateState(),
        });

        window.location.href = `https://github.com/login/oauth/authorize?${params}`;
    },

    /**
     * Log out: clear stored token and user data.
     */
    logout() {
        this._clearAuth();
        window.location.reload();
    },

    /**
     * Exchange an authorization code for an access token (called from callback.html).
     */
    async exchangeCode(code, state) {
        // Verify state to prevent CSRF
        const savedState = sessionStorage.getItem('gh_oauth_state');
        if (state !== savedState) {
            throw new Error('OAuth state mismatch. Possible CSRF attack.');
        }
        sessionStorage.removeItem('gh_oauth_state');

        const response = await fetch(AUTH_CONFIG.tokenProxyUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ code }),
        });

        if (!response.ok) {
            throw new Error(`Token exchange failed: ${response.status}`);
        }

        const data = await response.json();
        if (!data.access_token) {
            throw new Error('No access_token in response');
        }

        // Store the token
        localStorage.setItem(AUTH_CONFIG.storageKey, data.access_token);
        this._token = data.access_token;

        // Fetch and cache user info
        await this._fetchAndCacheUser();

        return data.access_token;
    },

    /**
     * Get the current authenticated user object (or null).
     */
    getUser() {
        return this._user;
    },

    /**
     * Check if the user is authenticated.
     */
    isAuthenticated() {
        return !!this._token;
    },

    // ---- Private methods ----

    _isProtectedPage() {
        const path = window.location.pathname;
        const page = path.split('/').pop() || 'index.html';
        return AUTH_CONFIG.protectedPages.includes(page);
    },

    _generateState() {
        const array = new Uint8Array(16);
        crypto.getRandomValues(array);
        const state = Array.from(array, b => b.toString(16).padStart(2, '0')).join('');
        sessionStorage.setItem('gh_oauth_state', state);
        return state;
    },

    async _validateToken() {
        try {
            const res = await fetch('https://api.github.com/user', {
                headers: {
                    'Authorization': `Bearer ${this._token}`,
                    'Accept': 'application/vnd.github.v3+json',
                },
            });
            if (res.ok) {
                const user = await res.json();
                this._user = user;
                localStorage.setItem(AUTH_CONFIG.userStorageKey, JSON.stringify(user));
                return true;
            }
            return false;
        } catch (e) {
            console.warn('Token validation failed:', e);
            return false;
        }
    },

    async _fetchAndCacheUser() {
        try {
            const res = await fetch('https://api.github.com/user', {
                headers: {
                    'Authorization': `Bearer ${this._token}`,
                    'Accept': 'application/vnd.github.v3+json',
                },
            });
            if (res.ok) {
                this._user = await res.json();
                localStorage.setItem(AUTH_CONFIG.userStorageKey, JSON.stringify(this._user));
            }
        } catch (e) {
            console.warn('Failed to fetch user:', e);
        }
    },

    _clearAuth() {
        localStorage.removeItem(AUTH_CONFIG.storageKey);
        localStorage.removeItem(AUTH_CONFIG.userStorageKey);
        this._token = null;
        this._user = null;
    },

    /**
     * Show login gate overlay (blocks page content until authenticated).
     */
    _showLoginGate() {
        // Hide the main content
        const main = document.querySelector('main');
        if (main) main.style.display = 'none';

        // Create overlay
        const overlay = document.createElement('div');
        overlay.id = 'auth-gate';
        overlay.innerHTML = `
            <div class="auth-gate-backdrop">
                <div class="auth-gate-card">
                    <div class="auth-gate-icon">
                        <svg viewBox="0 0 16 16" width="48" height="48" fill="currentColor">
                            <path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path>
                        </svg>
                    </div>
                    <h2>Authentication Required</h2>
                    <p>Sign in with your GitHub account to access the Data Explorer and datasets.</p>
                    <button class="auth-gate-btn" onclick="GitHubAuth.login()">
                        <svg viewBox="0 0 16 16" width="20" height="20" fill="currentColor" style="vertical-align: middle; margin-right: 8px;">
                            <path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path>
                        </svg>
                        Sign in with GitHub
                    </button>
                    <p class="auth-gate-hint">You'll be redirected to GitHub to authorize access.</p>
                </div>
            </div>
        `;
        document.body.appendChild(overlay);
    },

    /**
     * Show auth status UI (logged-in badge and logout button in the sidebar).
     */
    _showAuthUI() {
        if (!this._user) return;

        const nav = document.querySelector('nav');
        if (!nav) return;

        // Check if auth UI already exists
        if (document.getElementById('auth-user-info')) return;

        const userDiv = document.createElement('div');
        userDiv.id = 'auth-user-info';
        userDiv.innerHTML = `
            <div class="auth-user-badge">
                <img src="${this._user.avatar_url}" alt="${this._user.login}" class="auth-avatar" />
                <span class="auth-username">${this._user.login}</span>
                <button class="auth-logout-btn" onclick="GitHubAuth.logout()" title="Sign out">
                    <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
                        <path fill-rule="evenodd" d="M2 2.75C2 1.784 2.784 1 3.75 1h2.5a.75.75 0 010 1.5h-2.5a.25.25 0 00-.25.25v10.5c0 .138.112.25.25.25h2.5a.75.75 0 010 1.5h-2.5A1.75 1.75 0 012 13.25V2.75zm10.44 4.5H6.75a.75.75 0 000 1.5h5.69l-1.97 1.97a.75.75 0 101.06 1.06l3.25-3.25a.75.75 0 000-1.06l-3.25-3.25a.75.75 0 10-1.06 1.06l1.97 1.97z"></path>
                    </svg>
                </button>
            </div>
        `;
        nav.appendChild(userDiv);
    },
};

// ---- Auto-initialize on DOMContentLoaded ----
document.addEventListener('DOMContentLoaded', () => {
    GitHubAuth.init();
});
