const apiBase = "";

// Tab logic for main content
function showMainTab(tab) {
    ["create", "upload", "search"].forEach(t => {
        document.getElementById(`tab-${t}`).classList.remove("border-blue-600", "bg-blue-600");
        document.getElementById(`${t}-section`).classList.add("hidden");
    });
    document.getElementById(`tab-${tab}`).classList.add("border-blue-600", "bg-blue-600");
    document.getElementById(`${tab}-section`).classList.remove("hidden");
}
document.getElementById("tab-create").onclick = () => showMainTab("create");
document.getElementById("tab-upload").onclick = () => showMainTab("upload");
document.getElementById("tab-search").onclick = () => showMainTab("search");

// Upload source tabs
function showUploadSource(tab) {
    ["file", "url", "paste", "db"].forEach(t => {
        document.getElementById(`upload-tab-${t}`).classList.remove("border-blue-600", "bg-blue-600");
        document.getElementById(`upload-source-${t}`).classList.add("hidden");
    });
    document.getElementById(`upload-tab-${tab}`).classList.add("border-blue-600", "bg-blue-600");
    document.getElementById(`upload-source-${tab}`).classList.remove("hidden");
}
document.getElementById("upload-tab-file").onclick = () => showUploadSource("file");
document.getElementById("upload-tab-url").onclick = () => showUploadSource("url");
document.getElementById("upload-tab-paste").onclick = () => showUploadSource("paste");
document.getElementById("upload-tab-db").onclick = () => showUploadSource("db");

let filters = [];

function renderFilters() {
    const list = document.getElementById("filters-list");
    list.innerHTML = "";
    filters.forEach((f, i) => {
        const chip = document.createElement("span");
        chip.className = "filter-chip";
        chip.textContent = `${f.field} ${f.operator} ${f.value}`;
        const btn = document.createElement("button");
        btn.innerHTML = "&times;";
        btn.onclick = () => { filters.splice(i, 1); renderFilters(); };
        chip.appendChild(btn);
        list.appendChild(chip);
    });
}

document.getElementById("add-filter").onclick = function () {
    const field = document.getElementById("filter-field").value.trim();
    const value = document.getElementById("filter-value").value.trim();
    const operator = document.getElementById("filter-operator").value.trim();
    if (field && operator) {
        filters.push({ field, operator, value });
        renderFilters();
        document.getElementById("filter-field").value = "";
        document.getElementById("filter-value").value = "";
    }
};

function fetchIndexes() {
    fetch(apiBase + "/indexes")
        .then(res => res.json())
        .then(indexes => {
            // Sidebar list
            const list = document.getElementById("indexes-list");
            list.innerHTML = "";
            indexes.forEach(idx => {
                const li = document.createElement("li");
                li.textContent = idx;
                li.onclick = () => {
                    // Switch to search tab and select this index
                    showMainTab("search");
                    document.getElementById("search-index").value = idx;
                };
                list.appendChild(li);
            });
            // Update selects for all upload/search forms
            [
                "search-index",
                "upload-index",
                "upload-index-url",
                "upload-index-paste",
                "upload-index-db"
            ].forEach(id => {
                const sel = document.getElementById(id);
                if (!sel) return;
                sel.innerHTML = "";
                indexes.forEach(idx => {
                    const opt = document.createElement("option");
                    opt.value = idx;
                    opt.textContent = idx;
                    sel.appendChild(opt);
                });
            });
        });
}

document.getElementById("refresh-indexes").onclick = fetchIndexes;
window.onload = () => {
    fetchIndexes();
    showMainTab("create");
};

// Add Index
document.getElementById("add-index-form").onsubmit = function (e) {
    e.preventDefault();
    const id = document.getElementById("index-id").value.trim();
    const docIdField = document.getElementById("doc-id-field").value.trim();
    const fields = document.getElementById("fields").value.trim();
    const except = document.getElementById("except").value.trim();
    const workers = parseInt(document.getElementById("workers").value, 10) || undefined;
    const cache = parseInt(document.getElementById("cache").value, 10) || undefined;
    const payload = {
        id,
        doc_id_field: docIdField,
        fields: fields ? fields.split(",").map(f => f.trim()) : [],
        except: except ? except.split(",").map(f => f.trim()) : [],
        workers,
        cache
    };
    fetch(apiBase + "/index/add", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
    })
        .then(res => res.text())
        .then(msg => {
            document.getElementById("add-index-result").textContent = msg;
            fetchIndexes();
        });
};

// Upload from file
document.getElementById("upload-form-file").onsubmit = function (e) {
    e.preventDefault();
    const index = document.getElementById("upload-index").value;
    const fileInput = document.getElementById("upload-file");
    if (!fileInput.files.length) return;
    const file = fileInput.files[0];
    const reader = new FileReader();
    reader.onload = function (evt) {
        const data = evt.target.result;
        fetch(`${apiBase}/${index}/build`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: data
        })
            .then(res => res.text())
            .then(msg => {
                document.getElementById("upload-result").textContent = msg;
            });
    };
    reader.readAsText(file);
};

// Upload from URL
document.getElementById("upload-form-url").onsubmit = function (e) {
    e.preventDefault();
    const index = document.getElementById("upload-index-url").value;
    const url = document.getElementById("upload-url").value.trim();
    if (!url) return;
    fetch(url)
        .then(res => res.text())
        .then(data => {
            fetch(`${apiBase}/${index}/build`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: data
            })
                .then(res => res.text())
                .then(msg => {
                    document.getElementById("upload-result").textContent = msg;
                });
        })
        .catch(err => {
            document.getElementById("upload-result").textContent = "Failed to fetch URL: " + err;
        });
};

// Upload from pasted JSON
document.getElementById("upload-form-paste").onsubmit = function (e) {
    e.preventDefault();
    const index = document.getElementById("upload-index-paste").value;
    const data = document.getElementById("upload-paste").value.trim();
    if (!data) return;
    fetch(`${apiBase}/${index}/build`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: data
    })
        .then(res => res.text())
        .then(msg => {
            document.getElementById("upload-result").textContent = msg;
        });
};

// Upload from database
document.getElementById("upload-form-db").onsubmit = function (e) {
    e.preventDefault();
    const index = document.getElementById("upload-index-db").value;
    const dbType = document.getElementById("db-type").value.trim();
    const dbHost = document.getElementById("db-host").value.trim();
    const dbPort = parseInt(document.getElementById("db-port").value, 10);
    const dbUser = document.getElementById("db-user").value.trim();
    const dbPassword = document.getElementById("db-password").value;
    const dbName = document.getElementById("db-name").value.trim();
    const dbQuery = document.getElementById("db-query").value.trim();
    const payload = {
        database: {
            type: dbType,
            host: dbHost,
            port: dbPort,
            user: dbUser,
            password: dbPassword,
            database: dbName,
            query: dbQuery
        }
    };
    fetch(`${apiBase}/${index}/build`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
    })
        .then(res => res.text())
        .then(msg => {
            document.getElementById("upload-result").textContent = msg;
        });
};

// Search
document.getElementById("search-form").onsubmit = function (e) {
    e.preventDefault();
    const index = document.getElementById("search-index").value;
    const query = document.getElementById("search-query").value;
    const page = parseInt(document.getElementById("search-page").value, 10) || 1;
    const size = parseInt(document.getElementById("search-size").value, 10) || 10;
    const req = { q: query, p: page, s: size, filters };
    fetch(`${apiBase}/${index}/search`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req)
    })
        .then(res => res.json())
        .then(data => {
            renderResults(data);
        });
};

function renderResults(data) {
    const el = document.getElementById("search-result");
    if (!data || !data.items) {
        el.innerHTML = "<em>No results</em>";
        return;
    }
    let html = `<div style="margin-bottom:5px;"><b>Total:</b> ${data.total} | <b>Page:</b> ${data.page}/${data.total_pages}, <b>Time Take</b>:${data.latency}</div>`;
    html += "<table><thead><tr>";
    if (data.items.length > 0) {
        Object.keys(data.items[0]).forEach(k => {
            html += `<th class="px-3 py-1">${k}</th>`;
        });
        html += "</tr></thead><tbody>";
        data.items.forEach(row => {
            html += "<tr>";
            Object.values(row).forEach(val => {
                html += `<td class="px-6 py-2">${escapeHtml(val)}</td>`;
            });
            html += "</tr>";
        });
        html += "</tbody></table>";
    } else {
        html += "<tr><td>No items found</td></tr></table>";
    }
    el.innerHTML = html;
}

function escapeHtml(text) {
    if (typeof text !== "string") text = String(text);
    return text.replace(/[<>&"']/g, function (c) {
        return ({
            '<': '&lt;',
            '>': '&gt;',
            '&': '&amp;',
            '"': '&quot;',
            "'": '&#39;'
        })[c];
    });
}
