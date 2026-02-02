import json
from typing import Any, Dict, Tuple, Optional, List

from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
import httpx

HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Mini-Dynamo Demo UI</title>
  <style>
    body {{
      font-family: system-ui, sans-serif;
      margin: 2rem;
      max-width: 1100px;
    }}
    .topbar {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 1rem;
      margin-bottom: 1rem;
    }}
    .row {{
      display: flex;
      gap: 1rem;
      align-items: stretch;
      margin-bottom: 1.25rem;
    }}
    .card {{
      border: 1px solid #ddd;
      padding: 1rem;
      border-radius: 12px;
      flex: 1;
      background: #fafafa;
    }}
    .panel {{
      border: 1px solid #ddd;
      border-radius: 12px;
      padding: 1rem;
      background: #f6f8fa;
      margin-bottom: 1rem;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 180px 1fr;
      gap: .4rem 1rem;
      align-items: start;
    }}
    .k {{
      color: #444;
      font-size: .95rem;
    }}
    .v {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: .95rem;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    .chips {{
      display: flex;
      flex-wrap: wrap;
      gap: .4rem;
    }}
    .chip {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: .85rem;
      padding: .2rem .5rem;
      border: 1px solid #ddd;
      border-radius: 999px;
      background: white;
    }}
    .badge {{
      display: inline-block;
      padding: .2rem .6rem;
      border-radius: 999px;
      font-weight: 600;
      font-size: .85rem;
      border: 1px solid #ddd;
      background: white;
    }}
    .ok {{
      border-color: #86efac;
      background: #dcfce7;
    }}
    .err {{
      border-color: #fca5a5;
      background: #fee2e2;
    }}
    input, textarea {{
      width: 100%;
      padding: .5rem;
      margin-bottom: .5rem;
      font-family: inherit;
      border: 1px solid #ddd;
      border-radius: 10px;
      background: white;
    }}
    button {{
      padding: .5rem 1rem;
      cursor: pointer;
      border-radius: 10px;
      border: 1px solid #ddd;
      background: white;
    }}
    small {{
      color: #666;
    }}
    details {{
      margin-top: .75rem;
    }}
    pre.json {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.9rem;
      line-height: 1.35;
      margin: .5rem 0 0 0;
      white-space: pre-wrap;
    }}
  </style>
</head>
<body>

  <div class="topbar">
    <h1 style="margin:0;">Mini-Dynamo Demo UI</h1>
    <div><small>Target node: <span class="v">{target}</span></small></div>
  </div>

  <div class="row">
    <div class="card">
      <h3 style="margin-top:0;">PUT</h3>
      <form method="post" action="/put">
        <label>Key</label>
        <input name="key" required />
        <label>Value</label>
        <textarea name="value" rows="4" required></textarea>
        <button type="submit">PUT</button>
      </form>
    </div>

    <div class="card">
      <h3 style="margin-top:0;">GET</h3>
      <form method="post" action="/get">
        <label>Key</label>
        <input name="key" required />
        <button type="submit">GET</button>
      </form>
    </div>

    <div class="card">
      <h3 style="margin-top:0;">DELETE</h3>
      <form method="post" action="/delete">
        <label>Key</label>
        <input name="key" required />
        <button type="submit">DELETE</button>
      </form>
    </div>
  </div>

  <h2>Result</h2>
  <div class="panel">
    {result_html}
  </div>

  <h2>Node State</h2>
  <div class="panel">
    {state_html}
  </div>

</body>
</html>
"""


def _try_parse_json(text: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Returns (obj, pretty_text). obj is None if not JSON.
    """
    try:
        obj = json.loads(text)
        pretty = json.dumps(obj, indent=2, ensure_ascii=False)
        return obj, pretty
    except Exception:
        return None, text


def _html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )


def _chip_list(items: List[str]) -> str:
    if not items:
        return "<span class='v'>(none)</span>"
    chips = "".join(f"<span class='chip'>{_html_escape(x)}</span>" for x in items)
    return f"<div class='chips'>{chips}</div>"


def render_result(result_text: str) -> str:
    obj, raw_pretty = _try_parse_json(result_text)

    # If it isn't JSON, show raw
    if obj is None:
        return f"<div class='badge err'>ERROR</div><pre class='json'>{_html_escape(raw_pretty)}</pre>"

    # If it has a "detail" field, show that as error detail
    if "detail" in obj and isinstance(obj["detail"], (dict, list, str)):
        badge = "<span class='badge err'>ERROR</span>"
        main = obj["detail"]
        pretty_detail = json.dumps(main, indent=2, ensure_ascii=False) if not isinstance(main, str) else main
        return f"""
          {badge}
          <div style="margin-top:.75rem;">
            <div class="k">Error detail</div>
            <pre class="json">{_html_escape(pretty_detail)}</pre>
            <details>
              <summary>Show raw JSON</summary>
              <pre class="json">{_html_escape(raw_pretty)}</pre>
            </details>
          </div>
        """

    ok = bool(obj.get("ok", False))
    badge = f"<span class='badge {'ok' if ok else 'err'}'>{'OK' if ok else 'ERROR'}</span>"

    key = obj.get("key")
    replicas = obj.get("replicas", [])
    quorum = obj.get("quorum") or {}
    found = obj.get("found")
    record = obj.get("record") or {}
    value = record.get("value")

    rows = []

    if key is not None:
        rows.append(("Key", str(key)))

    # GET-specific summary
    if found is not None:
        rows.append(("Found", "Yes" if found else "No"))

    if value is not None:
        # keep value readable even if long
        rows.append(("Value", str(value)))

    # PUT/DELETE might include quorum info
    if isinstance(quorum, dict) and quorum:
        acks = quorum.get("acks")
        needed = quorum.get("needed")
        if acks is not None and needed is not None:
            rows.append(("Write quorum", f"{acks}/{needed} acks"))

    # Replicas list
    replicas_html = _chip_list([str(x) for x in replicas]) if isinstance(replicas, list) else _html_escape(str(replicas))

    grid_rows_html = "".join(
        f"<div class='k'>{_html_escape(k)}</div><div class='v'>{_html_escape(v)}</div>"
        for k, v in rows
    )

    return f"""
      <div>{badge}</div>
      <div style="margin-top:.75rem;" class="grid">
        {grid_rows_html}
        <div class="k">Replicas</div>
        <div>{replicas_html}</div>
      </div>

      <details>
        <summary>Show raw JSON</summary>
        <pre class="json">{_html_escape(raw_pretty)}</pre>
      </details>
    """


def render_state(state_text: str) -> str:
    obj, raw_pretty = _try_parse_json(state_text)

    if obj is None:
        return f"<div class='badge err'>STATE ERROR</div><pre class='json'>{_html_escape(raw_pretty)}</pre>"

    ring_nodes = obj.get("ring_nodes", [])
    peers = obj.get("peers", {})
    replication = obj.get("replication")
    w = obj.get("w")
    q = obj.get("q")

    alive = []
    dead = []
    if isinstance(peers, dict):
        for url, st in peers.items():
            if isinstance(st, dict) and st.get("alive") is True:
                alive.append(url)
            else:
                dead.append(url)

    # ring_nodes includes self + alive peers
    ring_nodes_list = ring_nodes if isinstance(ring_nodes, list) else []

    rows = []
    if replication is not None and w is not None and q is not None:
        rows.append(("Consistency", f"R={replication}, W={w}, Q={q}"))
    rows.append(("Ring nodes (active)", ", ".join(ring_nodes_list) if ring_nodes_list else "(none)"))
    rows.append(("Peers alive", str(len(alive))))
    rows.append(("Peers dead", str(len(dead))))

    grid_rows_html = "".join(
        f"<div class='k'>{_html_escape(k)}</div><div class='v'>{_html_escape(v)}</div>"
        for k, v in rows
    )

    return f"""
      <div class="grid">
        {grid_rows_html}
        <div class="k">Alive peers</div>
        <div>{_chip_list(alive)}</div>
        <div class="k">Dead peers</div>
        <div>{_chip_list(dead)}</div>
      </div>

      <details>
        <summary>Show raw JSON</summary>
        <pre class="json">{_html_escape(raw_pretty)}</pre>
      </details>
    """


def create_ui_app(target_node: str, debug: bool = False) -> FastAPI:
    app = FastAPI(title="Mini-Dynamo UI")
    timeout_s = 2.0 if not debug else 5.0

    async def fetch_state_text() -> str:
        try:
            async with httpx.AsyncClient(timeout=timeout_s) as client:
                r = await client.get(f"{target_node}/debug/state")
                return r.text
        except Exception as e:
            return f"Could not fetch /debug/state from {target_node}: {e}"

    def page(result_text: str, state_text: str) -> HTMLResponse:
        return HTMLResponse(
            HTML.format(
                target=target_node,
                result_html=render_result(result_text),
                state_html=render_state(state_text),
            )
        )

    @app.get("/", response_class=HTMLResponse)
    async def home():
        state_text = await fetch_state_text()
        return page("Ready.", state_text)

    @app.post("/put", response_class=HTMLResponse)
    async def do_put(key: str = Form(...), value: str = Form(...)):
        try:
            async with httpx.AsyncClient(timeout=timeout_s) as client:
                r = await client.post(f"{target_node}/kv/put", json={"key": key, "value": value})
                res_text = r.text
        except Exception as e:
            res_text = f"PUT failed: {e}"
        state_text = await fetch_state_text()
        return page(res_text, state_text)

    @app.post("/get", response_class=HTMLResponse)
    async def do_get(key: str = Form(...)):
        try:
            async with httpx.AsyncClient(timeout=timeout_s) as client:
                r = await client.get(f"{target_node}/kv/get", params={"key": key})
                res_text = r.text
        except Exception as e:
            res_text = f"GET failed: {e}"
        state_text = await fetch_state_text()
        return page(res_text, state_text)

    @app.post("/delete", response_class=HTMLResponse)
    async def do_delete(key: str = Form(...)):
        try:
            async with httpx.AsyncClient(timeout=timeout_s) as client:
                r = await client.post(f"{target_node}/kv/delete", json={"key": key})
                res_text = r.text
        except Exception as e:
            res_text = f"DELETE failed: {e}"
        state_text = await fetch_state_text()
        return page(res_text, state_text)

    return app
