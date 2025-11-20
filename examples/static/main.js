const apiBase = "/api";
let indexStats = {};

async function fetchJSON(url, options = {}) {
    const response = await fetch(url, options);
    if (!response.ok) {
        const body = await response.text().catch(() => "");
        throw new Error(body || `${response.status} ${response.statusText}`);
    }
    const text = await response.text();
    if (!text) {
        return null;
    }
    try {
        return JSON.parse(text);
    } catch (err) {
        throw new Error("Failed to parse JSON response");
    }
}

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

async function fetchIndexes() {
    try {
        const stats = await fetchJSON(`${apiBase}/indexes`);
        indexStats = stats || {};
        const names = Object.keys(indexStats).sort();

        const list = document.getElementById("indexes-list");
        list.innerHTML = "";
        if (names.length === 0) {
            const li = document.createElement("li");
            li.textContent = "No indexes yet";
            list.appendChild(li);
        } else {
            names.forEach(idx => {
                const li = document.createElement("li");
                li.textContent = idx;
                li.onclick = () => {
                    showMainTab("search");
                    document.getElementById("search-index").value = idx;
                    renderIndexStats(idx);
                };
                list.appendChild(li);
            });
        }

        const selectIds = [
            "search-index",
            "upload-index",
            "upload-index-url",
            "upload-index-paste",
            "upload-index-db"
        ];
        selectIds.forEach(id => {
            const sel = document.getElementById(id);
            if (!sel) {
                return;
            }
            sel.innerHTML = "";
            names.forEach(idx => {
                const opt = document.createElement("option");
                opt.value = idx;
                opt.textContent = idx;
                sel.appendChild(opt);
            });
        });

        if (names.length > 0) {
            renderIndexStats(names[0]);
        } else {
            renderIndexStats(null);
        }
    } catch (err) {
        console.error("Failed to load indexes", err);
        const list = document.getElementById("indexes-list");
        list.innerHTML = `<li class="text-red-600">Error: ${err.message}</li>`;
        renderIndexStats(null);
    }
}

function renderIndexStats(indexName) {
    const statsPanel = document.getElementById("index-stats");
    if (!statsPanel) {
        return;
    }
    if (!indexName || !indexStats[indexName]) {
        statsPanel.innerHTML = "<em>No index selected</em>";
        return;
    }
    const stats = indexStats[indexName];
    const totalDocs = stats?.document_count ?? stats?.DocumentCount ?? 0;
    const totalTerms = stats?.term_count ?? stats?.TermCount ?? 0;
    const avgLatency = stats?.average_latency ?? stats?.AverageLatency ?? "-";
    const totalQueries = stats?.total_queries ?? stats?.TotalQueries ?? 0;
    statsPanel.innerHTML = `
        <div><strong>${indexName}</strong></div>
        <div>Total documents: ${totalDocs}</div>
        <div>Indexed terms: ${totalTerms}</div>
        <div>Total queries: ${totalQueries}</div>
        <div>Avg. latency: ${avgLatency}</div>
    `;
}

document.getElementById("refresh-indexes").onclick = fetchIndexes;
window.onload = () => {
    fetchIndexes();
    showMainTab("create");
};

// Add Index
document.getElementById("add-index-form").onsubmit = async function (e) {
    e.preventDefault();
    const id = document.getElementById("index-id").value.trim();
    if (!id) {
        document.getElementById("add-index-result").textContent = "Index ID is required";
        return;
    }

    try {
        const params = new URLSearchParams({ name: id });
        await fetchJSON(`${apiBase}/index/create?${params.toString()}`, { method: "POST" });
        document.getElementById("add-index-result").textContent = `Index '${id}' created`;
        document.getElementById("index-id").value = "";
        fetchIndexes();
    } catch (err) {
        document.getElementById("add-index-result").textContent = `Error: ${err.message}`;
    }
};

async function ingestRecords(index, records) {
    const payload = Array.isArray(records) ? records : [records];
    let count = 0;
    for (const record of payload) {
        if (!record || typeof record !== "object") {
            continue;
        }
        await fetchJSON(`${apiBase}/index/${encodeURIComponent(index)}/document`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(record)
        });
        count++;
    }
    return count;
}

// Upload from file
document.getElementById("upload-form-file").onsubmit = function (e) {
    e.preventDefault();
    const index = document.getElementById("upload-index").value;
    const fileInput = document.getElementById("upload-file");
    if (!fileInput.files.length) return;
    const file = fileInput.files[0];
    const reader = new FileReader();
    reader.onload = function (evt) {
        try {
            const parsed = JSON.parse(evt.target.result);
            ingestRecords(index, parsed)
                .then(count => {
                    document.getElementById("upload-result").textContent = `Uploaded ${count} records`;
                })
                .catch(err => {
                    document.getElementById("upload-result").textContent = `Upload failed: ${err.message}`;
                });
        } catch (err) {
            document.getElementById("upload-result").textContent = `Invalid JSON: ${err.message}`;
        }
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
            try {
                const parsed = JSON.parse(data);
                ingestRecords(index, parsed)
                    .then(count => {
                        document.getElementById("upload-result").textContent = `Uploaded ${count} records`;
                    })
                    .catch(err => {
                        document.getElementById("upload-result").textContent = `Upload failed: ${err.message}`;
                    });
            } catch (err) {
                document.getElementById("upload-result").textContent = `Invalid JSON: ${err.message}`;
            }
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
    try {
        const parsed = JSON.parse(data);
        ingestRecords(index, parsed)
            .then(count => {
                document.getElementById("upload-result").textContent = `Uploaded ${count} records`;
            })
            .catch(err => {
                document.getElementById("upload-result").textContent = `Upload failed: ${err.message}`;
            });
    } catch (err) {
        document.getElementById("upload-result").textContent = `Invalid JSON: ${err.message}`;
    }
};

// Upload from database
document.getElementById("upload-form-db").onsubmit = function (e) {
    e.preventDefault();
    document.getElementById("upload-result").textContent = "Database ingestion is not yet supported in this interface.";
};

// Search
document.getElementById("search-form").onsubmit = async function (e) {
    e.preventDefault();
    const index = document.getElementById("search-index").value;
    const query = document.getElementById("search-query").value.trim();
    const page = parseInt(document.getElementById("search-page").value, 10) || 1;
    const size = parseInt(document.getElementById("search-size").value, 10) || 10;
    const searchType = document.getElementById("search-type")?.value || "fuzzy";
    const fuzzyThreshold = parseInt(document.getElementById("fuzzy-threshold")?.value, 10) || 2;

    if (!index) {
        document.getElementById("search-result").innerHTML = "<em>Select an index first</em>";
        return;
    }
    if (!query) {
        document.getElementById("search-result").innerHTML = "<em>Enter a query</em>";
        return;
    }

    try {
        const params = new URLSearchParams({
            q: query,
            page: String(page),
            size: String(size),
            search_type: searchType,
            fuzzy_threshold: String(fuzzyThreshold)
        });
        if (filters.length > 0) {
            params.set("filters", JSON.stringify(filters));
        }
        const data = await fetchJSON(`${apiBase}/search/${encodeURIComponent(index)}?${params.toString()}`);
        renderResults(data);
        renderIndexStats(index);
    } catch (err) {
        document.getElementById("search-result").innerHTML = `<em>Error: ${err.message}</em>`;
    }
};

function renderResults(data) {
    const el = document.getElementById("search-result");
    if (!data) {
        el.innerHTML = "<em>No results</em>";
        return;
    }
    const items = data.items || data.Items || [];
    const total = data.total ?? data.Total ?? items.length;
    let html = `<div style="margin-bottom:5px;"><b>Total:</b> ${total}`;
    if (typeof data.page !== "undefined" || typeof data.Page !== "undefined") {
        const page = data.page ?? data.Page;
        const totalPages = data.total_pages ?? data.TotalPages ?? 1;
        html += ` | <b>Page:</b> ${page}/${totalPages}`;
    }
    if (typeof data.latency !== "undefined") {
        html += ` | <b>Latency:</b> ${data.latency}`;
    }
    html += "</div>";
    html += "<table><thead><tr>";
    if (items.length > 0) {
        Object.keys(items[0]).forEach(k => {
            html += `<th class="px-3 py-1">${k}</th>`;
        });
        html += "</tr></thead><tbody>";
        items.forEach(row => {
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
