import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title="uRetOS Activation Tool", version="0.1.0")

HTML_CONTENT = r"""<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8">
  <title>uRetOS :: Activation Tool</title>
  <style>
    body { font-family: monospace; background: #121212; color: #00ffcc; padding: 20px; }
    .card { border: 1px solid #00ffcc; padding: 15px; margin-bottom: 20px; border-radius: 4px; }
    input, button { background: #222; color: #fff; border: 1px solid #00ffcc; padding: 8px; margin: 5px 0; }
    button { cursor: pointer; background: #00ffcc; color: #121212; font-weight: bold; }
    pre { background: #000; padding: 10px; border: 1px solid #333; max-height: 300px; overflow: auto; }
    .node-card { background: #1a1a1a; border: 1px solid #00ffcc; padding: 12px; border-radius: 4px; }
  </style>
</head>
<body>
  <h2>uRetOS :: Activation & OPC Coupling Tool</h2>

  <div class="card">
    <h3>1. Edge Box Aktivierung</h3>
    <label>Client / Tenant ID:</label><br>
    <input type="text" id="createClientId" value="tenant_cnc_01" style="width: 320px;"><br>
    <button onclick="generateToken()">Aktivierungstoken Anfordern</button><br><br>
    
    <label>Aktivierungs-Token (GUID):</label><br>
    <input type="text" id="tokenId" placeholder="Aktivierungs-Token (GUID)" style="width: 320px;"><br>
    <button onclick="activateToken()">Token Validieren</button>
    <div id="tokenStatus" style="margin-top: 10px;"></div>
  </div>

  <div class="card">
    <h3>2. OPC UA Server Koppeln</h3>
    <input type="text" id="tenantId" placeholder="Tenant ID"><br>
    <input type="text" id="opcUrl" value="opc.tcp://opc-server:4840" style="width: 320px;"><br>
    <button onclick="connectOpc()">Maschine Koppeln</button>
  </div>

  <div class="card">
    <h3>3. Erkannte Maschinenstammdaten</h3>
    <div id="metadataMeta" style="color: #888; font-size: 12px; margin-bottom: 10px;">Warte auf Daten...</div>
    
    <div id="nodesGrid" style="display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 15px;">
      <!-- Maschinenstammdaten-Cards werden hier injiziert -->
    </div>

    <details style="margin-top: 15px;">
      <summary style="cursor: pointer; color: #888;">Raw JSON Payload anzeigen</summary>
      <pre id="output" style="margin-top: 10px;">Bereit...</pre>
    </details>
  </div>

  <script>
    const API_BASE = "http://localhost:8001/api/v1";
    let eventSource = null;

    async function generateToken() {
      const clientId = document.getElementById("createClientId").value.trim();
      const statusDiv = document.getElementById("tokenStatus");
      if (!clientId) return alert("Bitte Client / Tenant ID eingeben!");

      statusDiv.innerText = "Erzeuge Token für Client '" + clientId + "'...";

      try {
        const res = await fetch(API_BASE + "/tokens", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            client_id: clientId,
            permissions: ["opc:read", "opc:write"]
          })
        });

        const data = await res.json();

        if (res.status === 202 && data.token_id) {
          document.getElementById("tokenId").value = data.token_id;
          document.getElementById("tenantId").value = clientId;
          statusDiv.innerText = "Token ID: " + data.token_id;
        } else {
          statusDiv.innerText = "Fehler (" + res.status + "): " + (data.detail || JSON.stringify(data));
        }
      } catch (err) {
        statusDiv.innerText = "Gateway nicht erreichbar: " + err.message;
      }
    }

    async function activateToken() {
      const tokenId = document.getElementById("tokenId").value.trim();
      const statusDiv = document.getElementById("tokenStatus");
      if (!tokenId) return alert("Bitte Token ID eingeben!");

      try {
        const res = await fetch(API_BASE + "/tokens/" + tokenId);
        const data = await res.json();
        if (res.ok) {
          statusDiv.innerText = "Token Gueltig! Client ID: " + data.client_id;
          document.getElementById("tenantId").value = data.client_id;
        } else {
          statusDiv.innerText = data.detail || 'Token Ungueltig';
        }
      } catch (err) {
        statusDiv.innerText = "Fehler: " + err.message;
      }
    }

    function renderMetadata(data) {
      const output = document.getElementById("output");
      const metaDiv = document.getElementById("metadataMeta");
      const grid = document.getElementById("nodesGrid");

      output.innerText = JSON.stringify(data, null, 2);
      metaDiv.innerText = "Empfangen um: " + new Date(data.received_at).toLocaleTimeString() + " | Status: " + (data.payload.status || 'OK');

      const nodes = data.payload.nodes || [];
      grid.innerHTML = "";

      if (nodes.length === 0) {
        grid.innerHTML = "<div style='color: #888;'>Keine Stammdaten-Knoten gefunden.</div>";
        return;
      }

      nodes.forEach(function(node) {
        const card = document.createElement("div");
        card.className = "node-card";
        
        card.innerHTML = 
          '<div style="font-weight: bold; color: #00ffcc; font-size: 14px; margin-bottom: 6px;">' +
            '📦 ' + (node.display_name || node.browse_name) +
          '</div>' +
          '<div style="font-size: 11px; color: #aaa; line-height: 1.5;">' +
            '<div><strong>Node ID:</strong> <code style="color: #ff00ff;">' + node.node_id + '</code></div>' +
            '<div><strong>Browse Name:</strong> ' + node.browse_name + '</div>' +
            '<div><strong>Node Class:</strong> ' + node.node_class + '</div>' +
          '</div>';
        
        grid.appendChild(card);
      });
    }

    async function connectOpc() {
      const tenantId = document.getElementById("tenantId").value.trim();
      const opcUrl = document.getElementById("opcUrl").value.trim();
      const output = document.getElementById("output");
      const metaDiv = document.getElementById("metadataMeta");

      if (!tenantId || !opcUrl) return alert("Tenant ID & OPC URL erforderlich!");

      metaDiv.innerText = "Sende Command an Gateway...";

      try {
        const res = await fetch(API_BASE + "/opc/connect", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ tenant_id: tenantId, opc_url: opcUrl })
        });
        const result = await res.json();
        metaDiv.innerText = "Command Status: " + result.status + " | Verbinde mit SSE-Stream...";
      } catch (err) {
        metaDiv.innerText = "Fehler beim Senden: " + err.message;
        return;
      }

      if (eventSource) eventSource.close();
      eventSource = new EventSource(API_BASE + "/opc/stream/" + tenantId);

      eventSource.onmessage = function(event) {
        const data = JSON.parse(event.data);
        renderMetadata(data);
      };
    }
  </script>
</body>
</html>"""

@app.get("/healthz")
def healthz():
    return {"status": "ok", "service": "activation-tool"}

@app.get("/", response_class=HTMLResponse)
def get_ui():
    return HTML_CONTENT