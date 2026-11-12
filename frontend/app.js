/**
 * Trading Trends Monitor - Frontend Application
 * CS-E4780 Scalable Systems & Data Management
 * Real-time dashboard consuming live data from the Go WebSocket Gateway.
 */

(function () {
  'use strict';

  // --- State ---
  const state = {
    ws: null,
    wsUrl: '',
    apiBase: '',
    reconnectTimer: null,
    statsTimer: null,
    activeSymbol: '',
    symbols: new Set(),
    markets: new Map(), // symbol -> MarketSummary (windows, close, last_signal, etc.)
    symbolFilter: '',
    symbolListDirty: false,
    symbolSyncTimer: null,
    symbolData: {},
    alerts: [],
    alertKeys: new Set(),
    debugLogs: [],
    soundEnabled: true,
    totalWindows: 0,
    totalBuys: 0,
    totalSells: 0,
    warmupAlerts: 0,
    activeSymbolsCount: 0,
    alertFilter: 'all',
    hoverIndex: -1,
    activeTab: 'tab-alerts',
  };

  const EXCHANGES = { NL: 'Amsterdam', FR: 'Paris', ETR: 'Frankfurt' };

  function getExchange(sym) {
    if (!sym || !sym.includes('.')) return 'Exchange';
    const ext = sym.split('.').pop();
    return EXCHANGES[ext] || ext;
  }

  // --- Audio ---
  let audioCtx = null;
  function playChime(dir) {
    if (!state.soundEnabled) return;
    try {
      if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      if (audioCtx.state === 'suspended') audioCtx.resume();
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      osc.connect(gain);
      gain.connect(audioCtx.destination);
      const now = audioCtx.currentTime;
      if (dir === 'BUY') {
        osc.frequency.setValueAtTime(659, now);
        osc.frequency.exponentialRampToValueAtTime(1047, now + 0.18);
      } else {
        osc.frequency.setValueAtTime(587, now);
        osc.frequency.exponentialRampToValueAtTime(370, now + 0.18);
      }
      gain.gain.setValueAtTime(0.08, now);
      gain.gain.exponentialRampToValueAtTime(0.001, now + 0.3);
      osc.start(now);
      osc.stop(now + 0.3);
    } catch (e) { /* ignore */ }
  }

  // --- DOM ---
  const $ = id => document.getElementById(id);

  const dom = {
    symbolSearch: $('symbol-search'),
    symbolSelect: $('symbol-select'),
    symbolCount: $('symbol-count'),
    statusBadge: $('status-badge'),
    statusText: $('status-text'),
    btnSound: $('btn-sound'),
    btnReconnect: $('btn-reconnect'),
    kpiPrice: $('kpi-price'),
    kpiWindowTime: $('kpi-window-time'),
    kpiEMA38: $('kpi-ema38'),
    kpiEMA100: $('kpi-ema100'),
    kpiSpread: $('kpi-spread'),
    kpiSignalBadge: $('kpi-signal-badge'),
    kpiSignalDesc: $('kpi-signal-desc'),
    telWindows: $('tel-windows'),
    telSymbols: $('tel-symbols'),
    telBuys: $('tel-buys'),
    telSells: $('tel-sells'),
    chartHeading: $('chart-heading'),
    chartWindowCount: $('chart-window-count'),
    chkEMA38: $('chk-ema38'),
    chkEMA100: $('chk-ema100'),
    chkMarkers: $('chk-markers'),
    chkFullScale: $('chk-fullscale'),
    btnFit: $('btn-fit'),
    chartStage: $('chart-stage'),
    chartCanvas: $('chart-canvas'),
    chartCursorInfo: $('chart-cursor-info'),
    alertCountPill: $('alert-count-pill'),
    alertsScrollBody: $('alerts-scroll-body'),
    tabHistSym: $('tab-hist-sym'),
    historyTbody: $('history-tbody'),
    btnClearDebug: $('btn-clear-debug'),
    debugScrollBody: $('debug-scroll-body'),
    footerEndpoint: $('footer-endpoint'),
    footerLatency: $('footer-latency'),
  };

  // --- Formatting ---
  function fmtPrice(val) {
    if (typeof val !== 'number' || !isFinite(val)) return '—';
    const abs = Math.abs(val);
    if (abs >= 1000) return `€${val.toFixed(2)}`;
    if (abs >= 1) return `€${val.toFixed(3)}`;
    return `€${val.toFixed(4)}`;
  }

  function fmtTime(val) {
    if (!val) return '--:--:--';
    const d = new Date(val);
    if (isNaN(d.getTime())) return String(val).substring(11, 19) || String(val);
    // Dataset timestamps are naive local wall clock stored as UTC
    return d.toISOString().substring(11, 19);
  }

  function esc(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  // --- WebSocket & REST ---
  function resolveGateway() {
    const host = window.location.hostname || 'localhost';
    const isHttps = window.location.protocol === 'https:';
    let port = isHttps ? (window.location.port || '443') : '8081';
    if (window.location.port && window.location.port !== '3000') {
      port = window.location.port;
    }
    const wsProto = isHttps ? 'wss' : 'ws';
    const httpProto = isHttps ? 'https' : 'http';
    state.wsUrl = `${wsProto}://${host}:${port}/ws`;
    state.apiBase = `${httpProto}://${host}:${port}`;
    if (dom.footerEndpoint) dom.footerEndpoint.textContent = state.wsUrl;
    return port;
  }

  function connectWS() {
    const port = resolveGateway();
    setBadge('connecting', 'Connecting');

    if (state.ws) {
      try {
        state.ws.onclose = null;
        state.ws.onerror = null;
        state.ws.close();
      } catch (e) {}
    }

    try {
      state.ws = new WebSocket(state.wsUrl);
    } catch (err) {
      setBadge('connecting', 'Retry…');
      retry();
      return;
    }

    state.ws.onopen = () => {
      setBadge('live', `Live :${port}`);
      if (state.activeSymbol) {
        sendWS({ action: 'subscribe', symbol: state.activeSymbol });
        sendWS({ action: 'get_history', symbol: state.activeSymbol });
      }
      pollStats();
    };

    state.ws.onmessage = e => {
      try {
        for (const line of e.data.split('\n')) {
          if (!line.trim()) continue;
          const msg = JSON.parse(line);
          addDebugLog(msg);
          handleMsg(msg);
        }
      } catch (err) { console.error('WS parse:', err); }
    };

    state.ws.onclose = () => { setBadge('connecting', 'Reconnecting'); retry(); };
    state.ws.onerror = () => { setBadge('connecting', 'Reconnecting'); retry(); };
  }

  function retry() {
    if (state.reconnectTimer) return;
    state.reconnectTimer = setTimeout(() => { state.reconnectTimer = null; connectWS(); }, 2500);
  }

  function setBadge(cls, text) {
    dom.statusBadge.className = `badge ${cls}`;
    dom.statusText.textContent = text;
  }

  function sendWS(obj) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      state.ws.send(JSON.stringify(obj));
    }
  }

  async function pollStats() {
    if (!state.apiBase) return;
    try {
      const res = await fetch(`${state.apiBase}/api/stats`);
      if (!res.ok) return;
      const data = await res.json();
      if (data.store_stats) {
        applyStoreStats(data.store_stats);
      }
      if (data.consumer_stats && dom.footerLatency) {
        const cs = data.consumer_stats;
        const parts = [];
        if (cs.messages > 0) parts.push(`${cs.messages.toLocaleString()} msgs`);
        if (cs.latency_samples > 0) parts.push(`E2E avg ${cs.latency_avg_ms.toFixed(1)} ms`);
        if (cs.analyzer_to_gateway_avg_ms > 0) parts.push(`Ana→GW ${cs.analyzer_to_gateway_avg_ms.toFixed(1)} ms`);
        if (parts.length > 0) {
          dom.footerLatency.textContent = parts.join(' · ');
        }
      }
      // If we still have no active symbol or no data yet, check /api/markets
      if (!state.activeSymbol || (state.symbolData[state.activeSymbol] || []).length === 0) {
        const mRes = await fetch(`${state.apiBase}/api/markets?limit=250`);
        if (mRes.ok) {
          const markets = await mRes.json();
          if (Array.isArray(markets) && markets.length > 0) {
            ingestMarkets(markets);
          }
        }
      }
    } catch (e) { /* ignore transient network error */ }
  }

  function applyStoreStats(st) {
    if (!st) return;
    if (typeof st.total_signals === 'number') state.totalWindows = st.total_signals;
    if (typeof st.total_buy_alerts === 'number') state.totalBuys = st.total_buy_alerts;
    if (typeof st.total_sell_alerts === 'number') state.totalSells = st.total_sell_alerts;
    if (typeof st.warmup_alerts === 'number') state.warmupAlerts = st.warmup_alerts;
    if (typeof st.active_symbols === 'number') state.activeSymbolsCount = st.active_symbols;
    renderTelemetry();
  }

  function ingestMarkets(markets) {
    markets.forEach(m => {
      if (!m || !m.symbol) return;
      state.symbols.add(m.symbol);
      state.markets.set(m.symbol, m);
    });
    if (!state.activeSymbol && markets.length > 0) {
      selectSymbol(markets[0].symbol);
    } else {
      scheduleSymbolSync();
    }
  }

  function handleMsg(msg) {
    switch (msg.type) {
      case 'info':
        if (msg.data) {
          if (Array.isArray(msg.data.symbols)) {
            msg.data.symbols.forEach(s => state.symbols.add(s));
          }
          if (Array.isArray(msg.data.markets)) {
            ingestMarkets(msg.data.markets);
          }
          if (msg.data.stats) {
            applyStoreStats(msg.data.stats);
          }
          if (Array.isArray(msg.data.recent_alerts)) {
            // recent_alerts arrives newest-first; insert oldest-first so unshift keeps order
            for (let i = msg.data.recent_alerts.length - 1; i >= 0; i--) {
              addAlert(msg.data.recent_alerts[i], false);
            }
            renderAlerts();
          }
          if (!state.activeSymbol) {
            const first = (msg.data.markets && msg.data.markets[0] && msg.data.markets[0].symbol) ||
                          (msg.data.symbols && msg.data.symbols[0]);
            if (first) selectSymbol(first);
          } else {
            scheduleSymbolSync();
          }
        }
        break;

      case 'symbols':
        if (Array.isArray(msg.data)) {
          msg.data.forEach(s => state.symbols.add(s));
          scheduleSymbolSync();
        }
        break;

      case 'history':
        if (msg.symbol && Array.isArray(msg.data)) {
          state.symbolData[msg.symbol] = normalizeSeries(msg.data);
          if (msg.symbol === state.activeSymbol) {
            state.hoverIndex = -1;
            renderChart();
            renderHistory();
            const pts = state.symbolData[msg.symbol];
            if (pts.length > 0) renderKPI(pts[pts.length - 1]);
          }
        }
        break;

      case 'signal':
        if (msg.data && msg.data.symbol) onSignal(msg.data);
        break;

      case 'alert':
        if (msg.data) addAlert(msg.data, true);
        break;
    }
  }

  function eventTimeKey(ev) {
    return ev.window_end || ev.timestamp || '';
  }

  function normalizeSeries(arr) {
    const byKey = new Map();
    arr.forEach(ev => {
      if (!ev) return;
      const key = eventTimeKey(ev) || String(byKey.size);
      byKey.set(key, ev);
    });
    return Array.from(byKey.values()).sort((a, b) => {
      const ta = new Date(eventTimeKey(a)).getTime() || 0;
      const tb = new Date(eventTimeKey(b)).getTime() || 0;
      return ta - tb;
    });
  }

  function onSignal(ev) {
    state.totalWindows++;
    state.symbols.add(ev.symbol);

    const existing = state.markets.get(ev.symbol) || { symbol: ev.symbol, windows: 0 };
    existing.windows = (existing.windows || 0) + 1;
    existing.close = ev.close;
    existing.ema38 = ev.ema38;
    existing.ema100 = ev.ema100;
    existing.window_end = ev.window_end;
    state.markets.set(ev.symbol, existing);

    if (!state.symbolData[ev.symbol]) state.symbolData[ev.symbol] = [];
    const pts = state.symbolData[ev.symbol];
    const key = eventTimeKey(ev);
    if (pts.length > 0 && key && eventTimeKey(pts[pts.length - 1]) === key) {
      pts[pts.length - 1] = ev;
    } else {
      pts.push(ev);
      if (pts.length > 500) pts.shift();
    }

    if (!state.activeSymbol) {
      selectSymbol(ev.symbol);
      return;
    }

    if (ev.symbol === state.activeSymbol) {
      renderChart();
      renderHistory();
      renderKPI(ev);
    }

    renderTelemetry();
    scheduleSymbolSync();
  }

  function addAlert(ev, isLive) {
    if (!ev || !ev.signal) return;
    const key = `${ev.symbol}|${ev.signal}|${eventTimeKey(ev)}`;
    if (state.alertKeys.has(key)) return;
    state.alertKeys.add(key);

    if (state.alertKeys.size > 2000) {
      const firstKey = state.alertKeys.values().next().value;
      state.alertKeys.delete(firstKey);
    }

    if (isLive && !ev.first_window) {
      if (ev.signal === 'BUY') state.totalBuys++;
      if (ev.signal === 'SELL') state.totalSells++;
    } else if (isLive && ev.first_window) {
      state.warmupAlerts++;
    }

    state.alerts.unshift(ev);
    if (state.alerts.length > 200) state.alerts.pop();

    if (isLive && !ev.first_window) playChime(ev.signal);
    if (isLive) {
      renderAlerts();
      renderTelemetry();
    }
  }

  // --- Rendering ---
  function renderKPI(p) {
    if (!p) return;
    dom.kpiPrice.textContent = fmtPrice(p.close);
    dom.kpiWindowTime.textContent = `w_${p.window_index || 0} · ${fmtTime(p.window_end || p.timestamp)}`;
    dom.kpiEMA38.textContent = fmtPrice(p.ema38);
    dom.kpiEMA100.textContent = fmtPrice(p.ema100);

    const spread = p.ema38 - p.ema100;
    dom.kpiSpread.textContent = (spread >= 0 ? '+' : '') + spread.toFixed(4);
    dom.kpiSpread.className = 'kpi-val' + (spread >= 0 ? ' green' : ' red');

    const badge = dom.kpiSignalBadge;
    if (p.signal === 'BUY') {
      badge.className = 'signal-chip buy';
      badge.textContent = p.first_window ? '▲ BUY (WARM-UP)' : '▲ BUY';
      dom.kpiSignalDesc.textContent = p.first_window ? 'Initial window (EMA0 = 0)' : 'Golden Cross detected';
    } else if (p.signal === 'SELL') {
      badge.className = 'signal-chip sell';
      badge.textContent = '▼ SELL';
      dom.kpiSignalDesc.textContent = 'Death Cross detected';
    } else if (spread >= 0) {
      badge.className = 'signal-chip buy';
      badge.textContent = 'BULLISH';
      dom.kpiSignalDesc.textContent = 'EMA38 above EMA100';
    } else {
      badge.className = 'signal-chip sell';
      badge.textContent = 'BEARISH';
      dom.kpiSignalDesc.textContent = 'EMA38 below EMA100';
    }

    dom.chartHeading.textContent = `${p.symbol} · ${getExchange(p.symbol)}`;
  }

  function renderTelemetry() {
    dom.telWindows.textContent = state.totalWindows.toLocaleString();
    dom.telSymbols.textContent = Math.max(state.activeSymbolsCount, state.symbols.size).toLocaleString();
    dom.telBuys.textContent = state.totalBuys.toLocaleString();
    dom.telSells.textContent = state.totalSells.toLocaleString();
    dom.alertCountPill.textContent = state.alerts.length ? `(${state.alerts.length})` : '';
  }

  function renderAlerts() {
    const filter = state.alertFilter;
    const list = state.alerts.filter(a => {
      if (filter === 'all') return true;
      if (filter === 'real') return !a.first_window;
      return a.signal === filter;
    });

    if (list.length === 0) {
      dom.alertsScrollBody.innerHTML = filter === 'real'
        ? '<div class="empty-msg">No post-warmup crossovers yet (single-day datasets mostly produce warm-up BUYs)…</div>'
        : '<div class="empty-msg">Waiting for crossover events…</div>';
      return;
    }

    dom.alertsScrollBody.innerHTML = '';
    list.slice(0, 60).forEach(a => {
      const isBuy = a.signal === 'BUY';
      const row = document.createElement('div');
      row.className = `alert-row ${isBuy ? 'buy' : 'sell'}`;

      const spread = (a.ema38 - a.ema100).toFixed(4);
      const time = fmtTime(a.window_end || a.timestamp);
      const warmupBadge = a.first_window ? '<span class="warmup-tag">warm-up</span>' : '';

      row.innerHTML = `
        <div class="alert-dir ${isBuy ? 'buy' : 'sell'}">${isBuy ? '▲' : '▼'}</div>
        <div class="alert-info">
          <div class="alert-info-top">
            <span>${esc(a.symbol)}</span>
            <span class="time">${time}</span>
          </div>
          <div class="alert-info-bottom">
            <span>Δ ${spread}</span>
            ${warmupBadge}
          </div>
        </div>
        <div class="alert-price">${fmtPrice(a.close)}</div>
      `;

      row.addEventListener('click', () => selectSymbol(a.symbol));
      dom.alertsScrollBody.appendChild(row);
    });
  }

  function renderHistory() {
    dom.tabHistSym.textContent = state.activeSymbol || '—';
    const pts = state.symbolData[state.activeSymbol] || [];

    if (pts.length === 0) {
      dom.historyTbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:20px;color:var(--text3)">No data</td></tr>';
      return;
    }

    dom.historyTbody.innerHTML = '';
    pts.slice(-40).reverse().forEach(p => {
      const tr = document.createElement('tr');
      const time = fmtTime(p.window_end || p.timestamp);
      let sig = '<span style="color:var(--text3)">—</span>';
      if (p.signal === 'BUY') sig = '<span style="color:var(--green);font-weight:700">BUY ▲</span>';
      if (p.signal === 'SELL') sig = '<span style="color:var(--red);font-weight:700">SELL ▼</span>';

      tr.innerHTML = `
        <td>${time}</td>
        <td>${fmtPrice(p.close)}</td>
        <td style="color:var(--cyan)">${p.ema38.toFixed(3)}</td>
        <td style="color:var(--amber)">${p.ema100.toFixed(3)}</td>
        <td>${sig}</td>
      `;
      dom.historyTbody.appendChild(tr);
    });
  }

  function addDebugLog(msg) {
    state.debugLogs.unshift({
      time: new Date().toLocaleTimeString(),
      type: msg.type || 'frame',
      symbol: msg.symbol || '',
      data: msg.data
    });
    if (state.debugLogs.length > 40) state.debugLogs.pop();
    if (state.activeTab === 'tab-debugger') {
      renderDebug();
    }
  }

  function renderDebug() {
    if (!dom.debugScrollBody) return;
    dom.debugScrollBody.innerHTML = '';

    if (state.debugLogs.length === 0) {
      dom.debugScrollBody.innerHTML = '<div class="empty-msg">No messages yet</div>';
      return;
    }

    state.debugLogs.slice(0, 25).forEach(l => {
      const el = document.createElement('div');
      el.className = 'debug-entry';
      const str = typeof l.data === 'object' ? JSON.stringify(l.data, null, 1) : String(l.data);
      el.innerHTML = `
        <div class="debug-head">
          <span class="debug-type">${esc(l.type.toUpperCase())}</span>
          ${l.symbol ? `<strong style="color:#fff">${esc(l.symbol)}</strong>` : ''}
          <span style="color:var(--text3)">${esc(l.time)}</span>
        </div>
        <div class="debug-json">${esc(str.slice(0, 400))}</div>
      `;
      dom.debugScrollBody.appendChild(el);
    });
  }

  // --- Canvas Chart ---
  function setupChart() {
    function resize() {
      const rect = dom.chartStage.getBoundingClientRect();
      if (rect.width <= 0 || rect.height <= 0) return;
      const dpr = window.devicePixelRatio || 1;
      dom.chartCanvas.width = Math.floor(rect.width * dpr);
      dom.chartCanvas.height = Math.floor(rect.height * dpr);
      dom.chartCanvas.style.width = rect.width + 'px';
      dom.chartCanvas.style.height = rect.height + 'px';
      renderChart();
    }

    if (typeof ResizeObserver !== 'undefined') {
      new ResizeObserver(resize).observe(dom.chartStage);
    } else {
      window.addEventListener('resize', resize);
    }
    resize();

    dom.chartCanvas.addEventListener('mousemove', e => {
      const rect = dom.chartCanvas.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const pts = state.symbolData[state.activeSymbol] || [];
      if (pts.length === 0) return;

      const padL = 24, padR = 90;
      const plotW = Math.max(1, rect.width - padL - padR);
      const ratio = Math.max(0, Math.min(1, (x - padL) / plotW));
      const idx = pts.length === 1 ? 0 : Math.round(ratio * (pts.length - 1));

      if (idx >= 0 && idx < pts.length) {
        state.hoverIndex = idx;
        const p = pts[idx];
        const time = fmtTime(p.window_end || p.timestamp);
        const sp = (p.ema38 - p.ema100).toFixed(4);
        dom.chartCursorInfo.innerHTML =
          `<strong>${time}</strong> · Close: <strong>${fmtPrice(p.close)}</strong> · ` +
          `<span style="color:var(--cyan)">EMA38: ${fmtPrice(p.ema38)}</span> · ` +
          `<span style="color:var(--amber)">EMA100: ${fmtPrice(p.ema100)}</span> · Δ ${sp}`;
        renderChart();
      }
    });

    dom.chartCanvas.addEventListener('mouseleave', () => {
      state.hoverIndex = -1;
      dom.chartCursorInfo.textContent = 'Hover chart to inspect values';
      renderChart();
    });
  }

  function renderChart() {
    const canvas = dom.chartCanvas;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.width;
    const h = canvas.height;

    ctx.clearRect(0, 0, w, h);

    const pts = state.symbolData[state.activeSymbol] || [];
    dom.chartWindowCount.textContent = pts.length ? `${pts.length} windows (5m)` : '5m windows';

    if (pts.length === 0) {
      ctx.fillStyle = '#475569';
      ctx.font = `${13 * dpr}px sans-serif`;
      ctx.textAlign = 'center';
      ctx.fillText(
        state.activeSymbol
          ? `Waiting for 5-minute windows for ${state.activeSymbol}…`
          : 'Waiting for stream data from Gateway…',
        w / 2,
        h / 2
      );
      return;
    }

    const padL = 24 * dpr, padR = 90 * dpr, padT = 24 * dpr, padB = 34 * dpr;
    const plotW = Math.max(10, w - padL - padR);
    const plotH = Math.max(10, h - padT - padB);

    // Compute Y-axis range
    let lo = Infinity, hi = -Infinity;
    pts.forEach(p => {
      lo = Math.min(lo, p.close);
      hi = Math.max(hi, p.close);
    });

    const fullScale = dom.chkFullScale && dom.chkFullScale.checked;
    if (fullScale) {
      pts.forEach(p => {
        if (dom.chkEMA38.checked) {
          lo = Math.min(lo, p.ema38);
          hi = Math.max(hi, p.ema38);
        }
        if (dom.chkEMA100.checked) {
          lo = Math.min(lo, p.ema100);
          hi = Math.max(hi, p.ema100);
        }
      });
    } else {
      // Include EMA values only once they are within 35% of the price band
      const pMid = (lo + hi) / 2 || 1;
      const band = Math.max(hi - lo, pMid * 0.05);
      const tol = band * 3 + pMid * 0.25;
      pts.forEach(p => {
        if (dom.chkEMA38.checked && Math.abs(p.ema38 - pMid) < tol) {
          lo = Math.min(lo, p.ema38);
          hi = Math.max(hi, p.ema38);
        }
        if (dom.chkEMA100.checked && Math.abs(p.ema100 - pMid) < tol) {
          lo = Math.min(lo, p.ema100);
          hi = Math.max(hi, p.ema100);
        }
      });
    }

    let range = hi - lo;
    if (range <= 0) {
      range = Math.max(Math.abs(hi) * 0.02, 0.01);
      lo -= range / 2;
      hi += range / 2;
    } else {
      lo -= range * 0.10;
      hi += range * 0.10;
    }
    if (lo < 0 && pts[0].close > 0) lo = 0;

    const gx = i => pts.length === 1 ? padL + plotW / 2 : padL + (i / (pts.length - 1)) * plotW;
    const gy = v => padT + plotH - ((v - lo) / (hi - lo)) * plotH;

    // Grid & Y-axis labels
    ctx.strokeStyle = '#141e30';
    ctx.lineWidth = 1 * dpr;
    for (let r = 0; r <= 5; r++) {
      const y = padT + (r / 5) * plotH;
      ctx.beginPath();
      ctx.moveTo(padL, y);
      ctx.lineTo(w - padR, y);
      ctx.stroke();

      const val = hi - (r / 5) * (hi - lo);
      ctx.fillStyle = '#64748b';
      ctx.font = `${10 * dpr}px monospace`;
      ctx.textAlign = 'left';
      ctx.fillText(fmtPrice(val), w - padR + 8 * dpr, y + 3 * dpr);
    }

    // Time labels
    const labelCount = Math.min(5, pts.length);
    for (let s = 0; s < labelCount; s++) {
      const idx = labelCount === 1 ? 0 : Math.floor((s / (labelCount - 1)) * (pts.length - 1));
      ctx.fillStyle = '#475569';
      ctx.font = `${9 * dpr}px monospace`;
      ctx.textAlign = 'center';
      ctx.fillText(fmtTime(pts[idx].window_end || pts[idx].timestamp), gx(idx), h - 10 * dpr);
    }

    // Clip series drawing inside the plot box so unconverged EMAs never spill out
    ctx.save();
    ctx.beginPath();
    ctx.rect(padL, padT, plotW, plotH);
    ctx.clip();

    if (pts.length >= 2) {
      // Area fill
      const grad = ctx.createLinearGradient(0, padT, 0, padT + plotH);
      grad.addColorStop(0, 'rgba(96,165,250,0.18)');
      grad.addColorStop(1, 'rgba(96,165,250,0.0)');

      ctx.beginPath();
      pts.forEach((p, i) => { i === 0 ? ctx.moveTo(gx(i), gy(p.close)) : ctx.lineTo(gx(i), gy(p.close)); });
      ctx.lineTo(gx(pts.length - 1), padT + plotH);
      ctx.lineTo(gx(0), padT + plotH);
      ctx.closePath();
      ctx.fillStyle = grad;
      ctx.fill();

      // Price line
      ctx.beginPath();
      pts.forEach((p, i) => { i === 0 ? ctx.moveTo(gx(i), gy(p.close)) : ctx.lineTo(gx(i), gy(p.close)); });
      ctx.strokeStyle = '#e2e8f0';
      ctx.lineWidth = 1.8 * dpr;
      ctx.stroke();

      // EMA 38
      if (dom.chkEMA38.checked) {
        ctx.beginPath();
        pts.forEach((p, i) => { i === 0 ? ctx.moveTo(gx(i), gy(p.ema38)) : ctx.lineTo(gx(i), gy(p.ema38)); });
        ctx.strokeStyle = '#22d3ee';
        ctx.lineWidth = 2 * dpr;
        ctx.stroke();
      }

      // EMA 100
      if (dom.chkEMA100.checked) {
        ctx.beginPath();
        pts.forEach((p, i) => { i === 0 ? ctx.moveTo(gx(i), gy(p.ema100)) : ctx.lineTo(gx(i), gy(p.ema100)); });
        ctx.strokeStyle = '#fb923c';
        ctx.lineWidth = 2 * dpr;
        ctx.stroke();
      }
    } else {
      // Single point
      ctx.beginPath();
      ctx.arc(gx(0), gy(pts[0].close), 4.5 * dpr, 0, Math.PI * 2);
      ctx.fillStyle = '#e2e8f0';
      ctx.fill();
    }

    // Crossover markers
    if (dom.chkMarkers.checked) {
      pts.forEach((p, i) => {
        if (p.signal !== 'BUY' && p.signal !== 'SELL') return;
        const x = gx(i), y = gy(p.close);
        const buy = p.signal === 'BUY';

        ctx.beginPath();
        ctx.arc(x, y, 4.5 * dpr, 0, Math.PI * 2);
        ctx.fillStyle = buy ? '#34d399' : '#f87171';
        ctx.fill();

        ctx.font = `bold ${8 * dpr}px sans-serif`;
        ctx.fillStyle = buy ? '#34d399' : '#f87171';
        ctx.textAlign = 'center';
        ctx.fillText(buy ? '▲ BUY' : '▼ SELL', x, buy ? Math.max(padT + 10 * dpr, y - 10 * dpr) : Math.min(padT + plotH - 6 * dpr, y + 16 * dpr));
      });
    }

    ctx.restore();

    // Hover crosshair
    if (state.hoverIndex >= 0 && state.hoverIndex < pts.length) {
      const hx = gx(state.hoverIndex);
      const hp = pts[state.hoverIndex];
      const hy = gy(hp.close);

      ctx.strokeStyle = 'rgba(255,255,255,0.2)';
      ctx.setLineDash([3 * dpr, 3 * dpr]);
      ctx.lineWidth = 1 * dpr;
      ctx.beginPath();
      ctx.moveTo(hx, padT);
      ctx.lineTo(hx, padT + plotH);
      ctx.moveTo(padL, hy);
      ctx.lineTo(w - padR, hy);
      ctx.stroke();
      ctx.setLineDash([]);

      ctx.beginPath();
      ctx.arc(hx, hy, 4 * dpr, 0, Math.PI * 2);
      ctx.fillStyle = '#fff';
      ctx.fill();
      ctx.strokeStyle = 'rgba(255,255,255,0.5)';
      ctx.lineWidth = 1 * dpr;
      ctx.stroke();
    }
  }

  // --- Symbol switching ---
  function scheduleSymbolSync() {
    state.symbolListDirty = true;
    if (state.symbolSyncTimer) return;
    state.symbolSyncTimer = setTimeout(() => {
      state.symbolSyncTimer = null;
      if (state.symbolListDirty) {
        state.symbolListDirty = false;
        syncSymbolsUI();
      }
    }, 1000);
  }

  function syncSymbolsUI() {
    // Don't replace options while the user has the select focused
    if (document.activeElement === dom.symbolSelect) return;

    const query = state.symbolFilter.trim().toUpperCase();
    const totalCount = Math.max(state.activeSymbolsCount, state.symbols.size);
    dom.symbolCount.textContent = `${totalCount.toLocaleString()} sym`;

    // Order by activity (most windows first), then alphabetically; cap dropdown at 250 matches for responsiveness
    let list = Array.from(state.symbols);
    if (query) {
      list = list.filter(s => s.toUpperCase().includes(query));
    }
    list.sort((a, b) => {
      const wa = (state.markets.get(a) && state.markets.get(a).windows) || 0;
      const wb = (state.markets.get(b) && state.markets.get(b).windows) || 0;
      if (wa !== wb) return wb - wa;
      return a.localeCompare(b);
    });

    const capped = list.slice(0, 250);
    if (state.activeSymbol && !capped.includes(state.activeSymbol) && (!query || state.activeSymbol.toUpperCase().includes(query))) {
      capped.unshift(state.activeSymbol);
    }

    dom.symbolSelect.innerHTML = '';
    if (capped.length === 0) {
      const opt = document.createElement('option');
      opt.value = '';
      opt.textContent = query ? 'No matching symbols' : 'Waiting for symbols…';
      dom.symbolSelect.appendChild(opt);
      return;
    }

    capped.forEach(s => {
      const m = state.markets.get(s);
      const opt = document.createElement('option');
      opt.value = s;
      opt.textContent = m && m.windows ? `${s} (${m.windows}w)` : s;
      if (s === state.activeSymbol) opt.selected = true;
      dom.symbolSelect.appendChild(opt);
    });
  }

  function selectSymbol(sym) {
    if (!sym || sym === state.activeSymbol) return;
    if (state.activeSymbol) {
      sendWS({ action: 'unsubscribe', symbol: state.activeSymbol });
    }
    state.activeSymbol = sym;
    state.symbols.add(sym);
    state.hoverIndex = -1;
    dom.chartHeading.textContent = `${sym} · ${getExchange(sym)}`;

    sendWS({ action: 'subscribe', symbol: sym });
    sendWS({ action: 'get_history', symbol: sym });

    syncSymbolsUI();
    const pts = state.symbolData[sym] || [];
    if (pts.length > 0) renderKPI(pts[pts.length - 1]);
    renderChart();
    renderHistory();
  }

  // --- Event Listeners ---
  function bindEvents() {
    dom.symbolSelect.addEventListener('change', e => {
      if (e.target.value) selectSymbol(e.target.value);
    });

    dom.symbolSearch.addEventListener('input', e => {
      state.symbolFilter = e.target.value;
      syncSymbolsUI();
    });

    dom.symbolSearch.addEventListener('keydown', e => {
      if (e.key === 'Enter' && dom.symbolSelect.value) {
        selectSymbol(dom.symbolSelect.value);
      }
    });

    dom.btnSound.addEventListener('click', () => {
      state.soundEnabled = !state.soundEnabled;
      dom.btnSound.textContent = state.soundEnabled ? '🔔' : '🔕';
    });

    dom.btnReconnect.addEventListener('click', () => connectWS());

    [dom.chkEMA38, dom.chkEMA100, dom.chkMarkers, dom.chkFullScale].forEach(el => {
      if (el) el.addEventListener('change', () => renderChart());
    });

    dom.btnFit.addEventListener('click', () => { state.hoverIndex = -1; renderChart(); });

    // Tabs
    document.querySelectorAll('.tab-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(v => v.classList.remove('active'));
        btn.classList.add('active');
        const tabId = btn.getAttribute('data-tab');
        state.activeTab = tabId;
        document.getElementById(tabId).classList.add('active');
        if (tabId === 'tab-debugger') renderDebug();
      });
    });

    // Alert filters
    document.querySelectorAll('.fpill').forEach(btn => {
      btn.addEventListener('click', e => {
        document.querySelectorAll('.fpill').forEach(b => b.classList.remove('active'));
        e.target.classList.add('active');
        state.alertFilter = e.target.getAttribute('data-filter');
        renderAlerts();
      });
    });

    dom.btnClearDebug.addEventListener('click', () => {
      state.debugLogs = [];
      renderDebug();
    });
  }

  // --- Init ---
  function init() {
    syncSymbolsUI();
    bindEvents();
    setupChart();
    renderAlerts();
    renderHistory();
    renderTelemetry();

    connectWS();
    state.statsTimer = setInterval(pollStats, 2000);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
