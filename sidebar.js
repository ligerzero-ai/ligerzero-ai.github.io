document.addEventListener("DOMContentLoaded", function() {
    const nav = document.querySelector('nav');
    if (!nav) return;

    // Get current filename to highlight active link
    const path = window.location.pathname;
    const page = path.split("/").pop() || "index.html";

    // Pages that require authentication (must match AUTH_CONFIG.protectedPages)
    const protectedPages = [
        'data-explorer.html',
        'dataset-info.html',
        'benchmarks.html'
    ];

    const isLoggedIn = !!localStorage.getItem('gh_auth_token');

    const menuItems = [
        { name: "Introduction", href: "index.html" },
        { name: "CV", href: "cv.html" },
        { name: "Publications", href: "publications.html" },
        { name: "Presentations", href: "presentations.html" },
        { name: "YouTube Videos", href: "youtube.html" },
        { name: "GitHub Repositories", href: "github.html" },
        { name: "Datasets", href: "dataset-info.html" },
        { name: "Data Explorer", href: "data-explorer.html" },
        { name: "Benchmarks", href: "benchmarks.html" }
    ];

    const lockIcon = '<svg viewBox="0 0 16 16" width="12" height="12" fill="currentColor" style="margin-left:4px;opacity:0.5;vertical-align:middle;"><path fill-rule="evenodd" d="M4 4v2h-.25A1.75 1.75 0 002 7.75v5.5c0 .966.784 1.75 1.75 1.75h8.5A1.75 1.75 0 0014 13.25v-5.5A1.75 1.75 0 0012.25 6H12V4a4 4 0 10-8 0zm6.5 2V4a2.5 2.5 0 00-5 0v2h5zM12.25 7.5a.25.25 0 01.25.25v5.5a.25.25 0 01-.25.25h-8.5a.25.25 0 01-.25-.25v-5.5a.25.25 0 01.25-.25h8.5z"></path></svg>';

    let html = '<h1>Han Lin Mai</h1><ul>';
    menuItems.forEach(item => {
        const isActive = page === item.href ? 'class="active"' : '';
        const isProtected = protectedPages.includes(item.href);
        const showLock = isProtected && !isLoggedIn;
        html += `<li><a href="${item.href}" ${isActive}>${item.name}${showLock ? lockIcon : ''}</a></li>`;
    });
    html += '</ul>';
    
    nav.innerHTML = html;
});
