document.addEventListener("DOMContentLoaded", function() {
    const nav = document.querySelector('nav');
    if (!nav) return;

    // Get current filename to highlight active link
    const path = window.location.pathname;
    const page = path.split("/").pop() || "index.html";
    
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

    let html = '<h1>Han Lin Mai</h1><ul>';
    menuItems.forEach(item => {
        const isActive = page === item.href ? 'class="active"' : '';
        html += `<li><a href="${item.href}" ${isActive}>${item.name}</a></li>`;
    });
    html += '</ul>';
    
    nav.innerHTML = html;
});
