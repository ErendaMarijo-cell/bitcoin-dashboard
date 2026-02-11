// ==================================================
// EXPLORER_ADDRESS.js – Address Explorer (lazy-init)
// ==================================================

(function () {

    function loadExplorerAddress() {

        const input   = document.getElementById("explorer-address-input");
        const button  = document.getElementById("explorer-address-search");
        const status  = document.getElementById("explorer-address-status");
        const summary = document.getElementById("explorer-address-summary");
        const history = document.getElementById("explorer-address-history");

        if (!input || !button || !status || !summary || !history) {
            console.warn("[EXPLORER_ADDRESS] DOM not ready");
            return;
        }

        // Prevent double initialization
        if (button.dataset.bound === "1") return;
        button.dataset.bound = "1";

        // --------------------------------------------------
        // Status handling (info / ok / error)
        // --------------------------------------------------
        const setStatus = (text, type = "info") => {
            status.textContent = text;
            status.classList.remove("status-ok", "status-error");

            if (type === "ok") status.classList.add("status-ok");
            if (type === "error") status.classList.add("status-error");
        };

        const hideResults = () => {
            summary.style.display = "none";
            history.style.display = "none";
        };

        const isValidAddress = addr =>
            /^bc1[0-9a-z]{20,}$/i.test(addr);

        // --------------------------------------------------
        // Summary (balances & counts)
        // --------------------------------------------------
        const renderSummary = data => {
            summary.innerHTML = `
                <div class="address-summary-grid">
                    <div><strong>Confirmed Balance</strong></div>
                    <div>${(data.balance.confirmed / 1e8).toFixed(8)} BTC</div>

                    <div><strong>Unconfirmed Balance</strong></div>
                    <div>${(data.balance.unconfirmed / 1e8).toFixed(8)} BTC</div>

                    <div><strong>UTXOs</strong></div>
                    <div>${data.utxos.length}</div>

                    <div><strong>Transactions</strong></div>
                    <div>${data.history.length}</div>
                </div>
            `;
            summary.style.display = "block";
        };

        // --------------------------------------------------
        // Transaction history (structure only, no styling)
        // --------------------------------------------------
        const renderHistory = items => {

            if (!items || items.length === 0) {
                history.innerHTML = "<p>No transactions found.</p>";
                history.style.display = "block";
                return;
            }

            history.innerHTML = `
                <table class="explorer-table">
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>TXID</th>
                            <th>Block</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${items.map((tx, index) => `
                            <tr>
                                <td>${index + 1}</td>
                                <td>${tx.tx_hash}</td>
                                <td>${tx.height > 0 ? tx.height : "Mempool"}</td>
                            </tr>
                        `).join("")}
                    </tbody>
                </table>
            `;

            history.style.display = "block";
        };

        // --------------------------------------------------
        // API fetch
        // --------------------------------------------------
        async function fetchAddress(address) {
            setStatus("Loading address data …");
            hideResults();

            try {
                const res  = await fetch(`/api/address/${address}`);
                const json = await res.json();

                if (!res.ok || json.status !== "ok") {
                    throw new Error(json.error || "Request failed");
                }

                setStatus("Address found", "ok");
                renderSummary(json.data);
                renderHistory(json.data.history);

            } catch (err) {
                setStatus(`Error: ${err.message}`, "error");
            }
        }

        // --------------------------------------------------
        // Events
        // --------------------------------------------------
        button.onclick = () => {
            const addr = input.value.trim();

            if (!isValidAddress(addr)) {
                setStatus("Invalid Bitcoin address", "error");
                hideResults();
                return;
            }

            fetchAddress(addr);
        };

        input.onkeydown = e => {
            if (e.key === "Enter") button.click();
        };

        console.log("[EXPLORER_ADDRESS] initialized");
    }


    // ==================================================
    // 👉 PUBLIC EXPORTS
    // ==================================================
    window.loadExplorerAddress = loadExplorerAddress;

    // 🔹 Dynamic Page Helper (optional but clean)
    window.searchAddressDirect = function(addr){

        const input  = document.getElementById("explorer-address-input");
        const button = document.getElementById("explorer-address-search");

        if(input && button){
            input.value = addr;
            button.click();
        }
    };


})();
