/**
 * Trading Trends Monitor - Frontend Application
 * CS-E4780 Scalable Systems & Data Management
 * Clean financial terminal with canvas chart, live alerts, and debug tools.
 */

(function () {
  'use strict';

  // --- Instruments ---
  const INSTRUMENTS = [
    { symbol: 'RDSA.NL', exchange: 'Amsterdam', basePrice: 19.85, volatility: 0.12, trend: 0.04 },
    { symbol: 'AIR.FR', exchange: 'Paris', basePrice: 112.40, volatility: 0.55, trend: -0.08 },
    { symbol: 'BMW.ETR', exchange: 'Frankfurt', basePrice: 89.10, volatility: 0.40, trend: 0.06 },
    { symbol: 'MC.FR', exchange: 'Paris', basePrice: 715.00, volatility: 2.20, trend: -0.15 },
    { symbol: 'ASML.NL', exchange: 'Amsterdam', basePrice: 680.00, volatility: 2.80, trend: 0.25 },
    { symbol: 'SAN.FR', exchange: 'Paris', basePrice: 88.50, volatility: 0.30, trend: -0.04 }
  ];

  // --- State ---
  const state = {
    ws: null,
    wsUrl: '',
    reconnectTimer: null,
    activeSymbol: 'RDSA.NL',
    symbols: new Set(INSTRUMENTS.map(i => i.symbol)),
    symbolData: {},
    alerts: [],
    debugLogs: [],
    soundEnabled: true,
    totalWindows: 0,
    totalBuys: 0,
    totalSells: 0,
    alertFilter: 'all',
    hoverIndex: -1,
  };

  const EXCHANGES = { NL: 'Amsterdam', FR: 'Paris', ETR: 'Frankfurt' };

  function getExchange(sym) {
    const ext = sym.split('.').pop();
    return EXCHANGES[ext] || 'Exchange';
  }

  // --- Pre-seed baseline data ---
  function seedData() {
    const now = Date.now();
    const a38 = 2 / 39;
    const a100 = 2 / 101;

    INSTRUMENTS.forEach(inst => {
      let p = inst.basePrice, ema38 = p, ema100 = p, prev38 = p, prev100 = p;
      const pts = [];

      for (let w = 35; w >= 0; w--) {
        const t = now - w * 300000;
        const wave = Math.sin(w / 4) * inst.volatility * 2.2;
        p = inst.basePrice + wave + Math.sin(w) * inst.volatility * 0.5;
        p = Math.round(p * 1000) / 1000;

        prev38 = ema38;
        prev100 = ema100;
        ema38 = p * a38 + prev38 * (1 - a38);
        ema100 = p * a100 + prev100 * (1 - a100);

        let signal = '';
        if (ema38 > ema100 && prev38 <= prev100) signal = 'BUY';
        else if (ema38 < ema100 && prev38 >= prev100) signal = 'SELL';

        const ev = {
          symbol: inst.symbol,
          close: p,
          ema38: Math.round(ema38 * 1000) / 1000,
          ema100: Math.round(ema100 * 1000) / 1000,
          signal: signal,
          window_index: 100 - w,
          timestamp: new Date(t).toISOString(),
          window_end: new Date(t).toISOString()
        };

        pts.push(ev);
        if (signal === 'BUY' || signal === 'SELL') {
          state.alerts.unshift(ev);
          if (signal === 'BUY') state.totalBuys++;
          if (signal === 'SELL') state.totalSells++;
        }
      }

      state.symbolData[inst.symbol] = pts;
    });

    state.totalWindows = 36 * INSTRUMENTS.length;
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
      gain.gain.setValueAtTime(0.1, now);
      gain.gain.exponentialRampToValueAtTime(0.001, now + 0.3);
      osc.start(now);
      osc.stop(now + 0.3);
    } catch (e) { /* ignore */ }
  }

  // --- DOM ---
  const $ = id => document.getElementById(id);

  const dom = {
    symbolSelect: $('symbol-select'),
    symbolPills: $('symbol-pills'),
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
    chkEMA38: $('chk-ema38'),
    chkEMA100: $('chk-ema100'),
    chkMarkers: $('chk-markers'),
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
  };

  // --- WebSocket ---
  function connectWS() {
    const host = window.location.hostname || 'localhost';
    let port = '8081';
    if (window.location.port && window.location.port !== '3000') {
      port = window.location.port;
    }
    state.wsUrl = `ws://${host}:${port}/ws`;
    if (dom.footerEndpoint) dom.footerEndpoint.textContent = state.wsUrl;

    setBadge('connecting', 'Connecting');

    if (state.ws) {
      try { state.ws.close(); } catch (e) {}
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
      sendWS({ action: 'subscribe', symbol: state.activeSymbol });
      sendWS({ action: 'get_history', symbol: state.activeSymbol });
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

  function handleMsg(msg) {
    switch (msg.type) {
      case 'info':
        if (msg.data && Array.isArray(msg.data.symbols)) {
          msg.data.symbols.forEach(s => state.symbols.add(s));
          syncPills();
        }
        if (msg.data && msg.data.stats) {
          state.totalWindows = msg.data.stats.total_signals || state.totalWindows;
          state.totalBuys = msg.data.stats.total_buy_alerts || state.totalBuys;
          state.totalSells = msg.data.stats.total_sell_alerts || state.totalSells;
          renderTelemetry();
        }
        break;

      case 'history':
        if (msg.symbol && Array.isArray(msg.data) && msg.data.length > 0) {
          state.symbolData[msg.symbol] = msg.data;
          if (msg.symbol === state.activeSymbol) {
            renderChart();
            renderHistory();
            renderKPI(msg.data[msg.data.length - 1]);
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

  function onSignal(ev) {
    state.totalWindows++;
    state.symbols.add(ev.symbol);

    if (!state.symbolData[ev.symbol]) state.symbolData[ev.symbol] = [];
    state.symbolData[ev.symbol].push(ev);
    if (state.symbolData[ev.symbol].length > 500) state.symbolData[ev.symbol].shift();

    if (ev.signal === 'BUY' || ev.signal === 'SELL') addAlert(ev, true);

    if (ev.symbol === state.activeSymbol) {
      renderChart();
      renderHistory();
      renderKPI(ev);
    }

    renderTelemetry();
    syncPills();
  }

  function addAlert(ev, isLive) {
    if (!ev || !ev.signal) return;
    if (ev.signal === 'BUY') state.totalBuys++;
    if (ev.signal === 'SELL') state.totalSells++;
    state.alerts.unshift(ev);
    if (state.alerts.length > 100) state.alerts.pop();
    if (isLive) playChime(ev.signal);
    renderAlerts();
    renderTelemetry();
  }

  // --- Rendering ---
  function renderKPI(p) {
    if (!p) return;
    dom.kpiPrice.textContent = `€${p.close.toFixed(3)}`;
    dom.kpiWindowTime.textContent = `w_${p.window_index || 0} · ${fmtTime(p.timestamp || p.window_end)}`;
    dom.kpiEMA38.textContent = `€${p.ema38.toFixed(3)}`;
    dom.kpiEMA100.textContent = `€${p.ema100.toFixed(3)}`;

    const spread = p.ema38 - p.ema100;
    dom.kpiSpread.textContent = (spread >= 0 ? '+' : '') + spread.toFixed(4);
    dom.kpiSpread.className = 'kpi-val' + (spread >= 0 ? ' green' : ' red');

    const badge = dom.kpiSignalBadge;
    if (p.signal === 'BUY') {
      badge.className = 'signal-chip buy';
      badge.textContent = '▲ BUY';
      dom.kpiSignalDesc.textContent = 'Golden Cross detected';
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
    dom.telWindows.textContent = state.totalWindows;
    dom.telSymbols.textContent = state.symbols.size;
    dom.telBuys.textContent = state.totalBuys;
    dom.telSells.textContent = state.totalSells;
    dom.alertCountPill.textContent = state.alerts.length ? `(${state.alerts.length})` : '';
  }

  function renderAlerts() {
    const filter = state.alertFilter;
    const list = state.alerts.filter(a => filter === 'all' || a.signal === filter);

    if (list.length === 0) {
      dom.alertsScrollBody.innerHTML = '<div class="empty-msg">Waiting for crossover events…</div>';
      return;
    }

    dom.alertsScrollBody.innerHTML = '';
    list.slice(0, 40).forEach(a => {
      const isBuy = a.signal === 'BUY';
      const row = document.createElement('div');
      row.className = `alert-row ${isBuy ? 'buy' : 'sell'}`;

      const spread = (a.ema38 - a.ema100).toFixed(4);
      const time = fmtTime(a.timestamp || a.window_end);

      row.innerHTML = `
        <div class="alert-dir ${isBuy ? 'buy' : 'sell'}">${isBuy ? '▲' : '▼'}</div>
        <div class="alert-info">
          <div class="alert-info-top">
            <span>${a.symbol}</span>
            <span class="time">${time}</span>
          </div>
          <div class="alert-info-bottom">Δ ${spread}</div>
        </div>
        <div class="alert-price">€${a.close.toFixed(3)}</div>
      `;

      row.addEventListener('click', () => selectSymbol(a.symbol));
      dom.alertsScrollBody.appendChild(row);
    });
  }

  function renderHistory() {
    dom.tabHistSym.textContent = state.activeSymbol;
    const pts = state.symbolData[state.activeSymbol] || [];

    if (pts.length === 0) {
      dom.historyTbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:20px;color:var(--text3)">No data</td></tr>';
      return;
    }

    dom.historyTbody.innerHTML = '';
    pts.slice(-30).reverse().forEach(p => {
      const tr = document.createElement('tr');
      const time = fmtTime(p.timestamp || p.window_end);
      let sig = '<span style="color:var(--text3)">—</span>';
      if (p.signal === 'BUY') sig = '<span style="color:var(--green);font-weight:700">BUY ▲</span>';
      if (p.signal === 'SELL') sig = '<span style="color:var(--red);font-weight:700">SELL ▼</span>';

      tr.innerHTML = `
        <td>${time}</td>
        <td>€${p.close.toFixed(3)}</td>
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
    renderDebug();
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
          <span class="debug-type">${l.type.toUpperCase()}</span>
          ${l.symbol ? `<strong style="color:#fff">${l.symbol}</strong>` : ''}
          <span style="color:var(--text3)">${l.time}</span>
        </div>
        <div class="debug-json">${esc(str)}</div>
      `;
      dom.debugScrollBody.appendChild(el);
    });
  }

  function esc(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  // --- Canvas Chart ---
  function setupChart() {
    function resize() {
      const rect = dom.chartStage.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      dom.chartCanvas.width = rect.width * dpr;
      dom.chartCanvas.height = rect.height * dpr;
      dom.chartCanvas.style.width = rect.width + 'px';
      dom.chartCanvas.style.height = rect.height + 'px';
      renderChart();
    }

    window.addEventListener('resize', resize);
    resize();

    dom.chartCanvas.addEventListener('mousemove', e => {
      const rect = dom.chartCanvas.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const pts = state.symbolData[state.activeSymbol] || [];
      if (pts.length < 2) return;

      const padL = 50, padR = 80;
      const plotW = rect.width - padL - padR;
      const ratio = Math.max(0, Math.min(1, (x - padL) / plotW));
      const idx = Math.round(ratio * (pts.length - 1));

      if (idx >= 0 && idx < pts.length) {
        state.hoverIndex = idx;
        const p = pts[idx];
        const time = fmtTime(p.timestamp || p.window_end);
        const sp = (p.ema38 - p.ema100).toFixed(4);
        dom.chartCursorInfo.innerHTML =
          `<strong>${time}</strong> · Close: <strong>€${p.close.toFixed(3)}</strong> · ` +
          `<span style="color:var(--cyan)">EMA38: ${p.ema38.toFixed(3)}</span> · ` +
          `<span style="color:var(--amber)">EMA100: ${p.ema100.toFixed(3)}</span> · Δ ${sp}`;
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
    if (pts.length < 2) {
      ctx.fillStyle = '#334155';
      ctx.font = `${13 * dpr}px sans-serif`;
      ctx.textAlign = 'center';
      ctx.fillText('Waiting for data…', w / 2, h / 2);
      return;
    }

    const padL = 50 * dpr, padR = 80 * dpr, padT = 24 * dpr, padB = 36 * dpr;
    const plotW = w - padL - padR;
    const plotH = h - padT - padB;

    let lo = Infinity, hi = -Infinity;
    pts.forEach(p => {
      lo = Math.min(lo, p.close, p.ema38, p.ema100);
      hi = Math.max(hi, p.close, p.ema38, p.ema100);
    });
    const range = hi - lo || 1;
    lo -= range * 0.08;
    hi += range * 0.08;

    const gx = i => padL + (i / (pts.length - 1)) * plotW;
    const gy = v => padT + plotH - ((v - lo) / (hi - lo)) * plotH;

    // Grid
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
      ctx.fillText(`€${val.toFixed(2)}`, w - padR + 8 * dpr, y + 3 * dpr);
    }

    // Time labels
    for (let s = 0; s <= 4; s++) {
      const idx = Math.floor((s / 4) * (pts.length - 1));
      ctx.fillStyle = '#475569';
      ctx.font = `${9 * dpr}px monospace`;
      ctx.textAlign = 'center';
      ctx.fillText(fmtTime(pts[idx].timestamp || pts[idx].window_end), gx(idx), h - 10 * dpr);
    }

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
      ctx.shadowColor = 'rgba(34,211,238,0.4)';
      ctx.shadowBlur = 6 * dpr;
      ctx.stroke();
      ctx.shadowBlur = 0;
    }

    // EMA 100
    if (dom.chkEMA100.checked) {
      ctx.beginPath();
      pts.forEach((p, i) => { i === 0 ? ctx.moveTo(gx(i), gy(p.ema100)) : ctx.lineTo(gx(i), gy(p.ema100)); });
      ctx.strokeStyle = '#fb923c';
      ctx.lineWidth = 2 * dpr;
      ctx.shadowColor = 'rgba(251,146,60,0.4)';
      ctx.shadowBlur = 6 * dpr;
      ctx.stroke();
      ctx.shadowBlur = 0;
    }

    // Crossover markers
    if (dom.chkMarkers.checked) {
      pts.forEach((p, i) => {
        if (p.signal !== 'BUY' && p.signal !== 'SELL') return;
        const x = gx(i), y = gy(p.close);
        const buy = p.signal === 'BUY';

        // Circle marker
        ctx.beginPath();
        ctx.arc(x, y, 5 * dpr, 0, Math.PI * 2);
        ctx.fillStyle = buy ? '#34d399' : '#f87171';
        ctx.fill();

        // Arrow
        ctx.beginPath();
        if (buy) {
          ctx.moveTo(x, y - 10 * dpr);
          ctx.lineTo(x - 5 * dpr, y - 4 * dpr);
          ctx.lineTo(x + 5 * dpr, y - 4 * dpr);
        } else {
          ctx.moveTo(x, y + 10 * dpr);
          ctx.lineTo(x - 5 * dpr, y + 4 * dpr);
          ctx.lineTo(x + 5 * dpr, y + 4 * dpr);
        }
        ctx.fillStyle = buy ? '#34d399' : '#f87171';
        ctx.fill();

        // Label
        ctx.font = `bold ${8 * dpr}px sans-serif`;
        ctx.fillStyle = buy ? '#34d399' : '#f87171';
        ctx.textAlign = 'center';
        ctx.fillText(p.signal, x, buy ? y - 14 * dpr : y + 18 * dpr);
      });
    }

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

      // Dot
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
  function syncPills() {
    const sorted = Array.from(state.symbols).sort();

    // Hidden select
    dom.symbolSelect.innerHTML = '';
    sorted.forEach(s => {
      const opt = document.createElement('option');
      opt.value = s;
      opt.textContent = s;
      if (s === state.activeSymbol) opt.selected = true;
      dom.symbolSelect.appendChild(opt);
    });

    // Pill buttons
    dom.symbolPills.innerHTML = '';
    sorted.forEach(s => {
      const btn = document.createElement('button');
      btn.className = `sym-btn${s === state.activeSymbol ? ' active' : ''}`;
      btn.textContent = s;
      btn.addEventListener('click', () => selectSymbol(s));
      dom.symbolPills.appendChild(btn);
    });
  }

  function selectSymbol(sym) {
    if (!sym || sym === state.activeSymbol) return;
    sendWS({ action: 'unsubscribe', symbol: state.activeSymbol });
    state.activeSymbol = sym;
    dom.symbolSelect.value = sym;
    sendWS({ action: 'subscribe', symbol: sym });
    sendWS({ action: 'get_history', symbol: sym });

    syncPills();
    const pts = state.symbolData[sym] || [];
    if (pts.length > 0) renderKPI(pts[pts.length - 1]);
    renderChart();
    renderHistory();
  }

  function fmtTime(val) {
    if (!val) return '--:--:--';
    const d = new Date(val);
    if (isNaN(d.getTime())) return String(val).substring(11, 19) || String(val);
    return d.toTimeString().split(' ')[0];
  }

  // --- Event Listeners ---
  function bindEvents() {
    dom.symbolSelect.addEventListener('change', e => selectSymbol(e.target.value));

    dom.btnSound.addEventListener('click', () => {
      state.soundEnabled = !state.soundEnabled;
      dom.btnSound.textContent = state.soundEnabled ? '🔔' : '🔕';
    });

    dom.btnReconnect.addEventListener('click', () => connectWS());

    [dom.chkEMA38, dom.chkEMA100, dom.chkMarkers].forEach(el => {
      el.addEventListener('change', () => renderChart());
    });

    dom.btnFit.addEventListener('click', () => { state.hoverIndex = -1; renderChart(); });

    // Tabs
    document.querySelectorAll('.tab-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(v => v.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById(btn.getAttribute('data-tab')).classList.add('active');
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
    seedData();
    syncPills();
    bindEvents();
    setupChart();

    const initialPts = state.symbolData[state.activeSymbol];
    if (initialPts && initialPts.length > 0) {
      renderKPI(initialPts[initialPts.length - 1]);
      renderHistory();
      renderAlerts();
      renderTelemetry();
    }

    connectWS();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
