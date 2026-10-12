/**
 * Trading Trends Monitor - Frontend Application
 * CS-E4780 Scalable Systems & Data Management
 * Rich financial terminal dashboard with instant baseline visualization,
 * dual EMA indicators, interactive canvas chart, and real-time breakout alerts.
 */

(function () {
  'use strict';

  // --- Initial Instruments Baseline ---
  const INITIAL_INSTRUMENTS = [
    { symbol: 'RDSA.NL', exchange: 'Amsterdam (NL)', basePrice: 19.85, volatility: 0.12, trend: 0.04 },
    { symbol: 'AIR.FR', exchange: 'Paris (FR)', basePrice: 112.40, volatility: 0.55, trend: -0.08 },
    { symbol: 'BMW.ETR', exchange: 'Frankfurt (ETR)', basePrice: 89.10, volatility: 0.40, trend: 0.06 },
    { symbol: 'MC.FR', exchange: 'Paris (FR)', basePrice: 715.00, volatility: 2.20, trend: -0.15 },
    { symbol: 'ASML.NL', exchange: 'Amsterdam (NL)', basePrice: 680.00, volatility: 2.80, trend: 0.25 },
    { symbol: 'SAN.FR', exchange: 'Paris (FR)', basePrice: 88.50, volatility: 0.30, trend: -0.04 }
  ];

  // --- Application State ---
  const state = {
    ws: null,
    wsUrl: '',
    reconnectTimer: null,
    activeSymbol: 'RDSA.NL',
    symbols: new Set(INITIAL_INSTRUMENTS.map(i => i.symbol)),
    symbolData: {},     // symbol -> array of window points
    alerts: [],         // array of Query 2 breakout events
    debugLogs: [],      // raw frames log
    soundEnabled: true,
    totalWindows: 0,
    totalBuys: 0,
    totalSells: 0,
    alertFilter: 'all',
    hoverIndex: -1,
  };

  const EXCHANGE_NAMES = {
    NL: 'Amsterdam (NL)',
    FR: 'Paris (FR)',
    ETR: 'Frankfurt/Xetra (ETR)',
  };

  function getExchange(sym) {
    const parts = sym.split('.');
    return parts.length > 1 && EXCHANGE_NAMES[parts[1]] ? EXCHANGE_NAMES[parts[1]] : 'Exchange';
  }

  // --- Pre-seed baseline window points so the UI is NEVER empty ---
  function seedInitialData() {
    const now = Date.now();
    const alpha38 = 2.0 / (1.0 + 38.0);
    const alpha100 = 2.0 / (1.0 + 100.0);

    INITIAL_INSTRUMENTS.forEach(inst => {
      let p = inst.basePrice;
      let ema38 = p;
      let ema100 = p;
      let prev38 = p;
      let prev100 = p;
      const points = [];

      // Generate 35 baseline 5-minute windows
      for (let w = 35; w >= 0; w--) {
        const timeMs = now - (w * 5 * 60 * 1000);
        // Sinusoidal wave to create natural crossovers
        const wave = Math.sin(w / 4.0) * inst.volatility * 2.2;
        p = inst.basePrice + wave + (Math.sin(w) * inst.volatility * 0.5);
        p = Math.round(p * 1000) / 1000;

        prev38 = ema38;
        prev100 = ema100;
        ema38 = p * alpha38 + prev38 * (1.0 - alpha38);
        ema100 = p * alpha100 + prev100 * (1.0 - alpha100);

        let signal = '';
        if (ema38 > ema100 && prev38 <= prev100) {
          signal = 'BUY';
        } else if (ema38 < ema100 && prev38 >= prev100) {
          signal = 'SELL';
        }

        const event = {
          symbol: inst.symbol,
          close: p,
          ema38: Math.round(ema38 * 1000) / 1000,
          ema100: Math.round(ema100 * 1000) / 1000,
          signal: signal,
          window_index: 100 - w,
          timestamp: new Date(timeMs).toISOString(),
          window_end: new Date(timeMs).toISOString()
        };

        points.push(event);

        if (signal === 'BUY' || signal === 'SELL') {
          state.alerts.unshift(event);
          if (signal === 'BUY') state.totalBuys++;
          if (signal === 'SELL') state.totalSells++;
        }
      }

      state.symbolData[inst.symbol] = points;
    });

    state.totalWindows = 35 * INITIAL_INSTRUMENTS.length;
  }

  // --- Web Audio Chime ---
  let audioCtx = null;
  function playChime(direction) {
    if (!state.soundEnabled) return;
    try {
      if (!audioCtx) {
        audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      }
      if (audioCtx.state === 'suspended') {
        audioCtx.resume();
      }
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      osc.connect(gain);
      gain.connect(audioCtx.destination);

      const now = audioCtx.currentTime;
      if (direction === 'BUY') {
        osc.frequency.setValueAtTime(659.25, now);
        osc.frequency.exponentialRampToValueAtTime(1046.5, now + 0.18);
        gain.gain.setValueAtTime(0.12, now);
        gain.gain.exponentialRampToValueAtTime(0.001, now + 0.32);
      } else {
        osc.frequency.setValueAtTime(587.33, now);
        osc.frequency.exponentialRampToValueAtTime(369.99, now + 0.18);
        gain.gain.setValueAtTime(0.12, now);
        gain.gain.exponentialRampToValueAtTime(0.001, now + 0.32);
      }
      osc.start(now);
      osc.stop(now + 0.32);
    } catch (e) {
      console.warn('Audio chime note:', e);
    }
  }

  // --- DOM Elements ---
  const dom = {
    symbolSelect: document.getElementById('symbol-select'),
    symbolPills: document.getElementById('symbol-pills'),
    statusBadge: document.getElementById('status-badge'),
    statusText: document.getElementById('status-text'),
    btnSound: document.getElementById('btn-sound'),
    btnReconnect: document.getElementById('btn-reconnect'),
    kpiExchange: document.getElementById('kpi-exchange'),
    kpiPrice: document.getElementById('kpi-price'),
    kpiWindowTime: document.getElementById('kpi-window-time'),
    kpiEMA38: document.getElementById('kpi-ema38'),
    kpiEMA100: document.getElementById('kpi-ema100'),
    kpiSignalBadge: document.getElementById('kpi-signal-badge'),
    kpiSpread: document.getElementById('kpi-spread'),
    kpiSignalDesc: document.getElementById('kpi-signal-desc'),
    telWindows: document.getElementById('tel-windows'),
    telSymbols: document.getElementById('tel-symbols'),
    telBuys: document.getElementById('tel-buys'),
    telSells: document.getElementById('tel-sells'),
    telRate: document.getElementById('tel-rate'),
    chartHeading: document.getElementById('chart-heading'),
    chkEMA38: document.getElementById('chk-ema38'),
    chkEMA100: document.getElementById('chk-ema100'),
    chkMarkers: document.getElementById('chk-markers'),
    btnFit: document.getElementById('btn-fit'),
    chartStage: document.getElementById('chart-stage'),
    chartCanvas: document.getElementById('chart-canvas'),
    chartCursorInfo: document.getElementById('chart-cursor-info'),
    alertCountPill: document.getElementById('alert-count-pill'),
    alertsScrollBody: document.getElementById('alerts-scroll-body'),
    tabHistSym: document.getElementById('tab-hist-sym'),
    historyTbody: document.getElementById('history-tbody'),
    btnClearDebug: document.getElementById('btn-clear-debug'),
    debugScrollBody: document.getElementById('debug-scroll-body'),
    footerEndpoint: document.getElementById('footer-endpoint'),
  };

  // --- WebSocket Connection ---
  function connectWebSocket() {
    const host = window.location.hostname || 'localhost';
    // Auto-detect port: use page port if not 3000, otherwise 8081
    let port = '8081';
    if (window.location.port && window.location.port !== '3000') {
      port = window.location.port;
    }
    state.wsUrl = `ws://${host}:${port}/ws`;
    dom.footerEndpoint.textContent = state.wsUrl;

    updateBadge('connecting', 'Connecting...');

    if (state.ws) {
      try { state.ws.close(); } catch (e) {}
    }

    try {
      state.ws = new WebSocket(state.wsUrl);
    } catch (err) {
      console.warn('WS error:', err);
      updateBadge('connecting', 'Reconnecting...');
      scheduleRetry();
      return;
    }

    state.ws.onopen = function () {
      console.log('[WS] Connected to Gateway:', state.wsUrl);
      updateBadge('live', `Live (${port})`);
      dom.telRate.textContent = 'Live stream connected to Redpanda';

      sendWS({ action: 'subscribe', symbol: state.activeSymbol });
      sendWS({ action: 'get_history', symbol: state.activeSymbol });
    };

    state.ws.onmessage = function (e) {
      try {
        const lines = e.data.split('\n');
        for (const line of lines) {
          if (!line.trim()) continue;
          const msg = JSON.parse(line);
          pushDebugLog(msg);
          processServerMessage(msg);
        }
      } catch (err) {
        console.error('WS parse error:', err);
      }
    };

    state.ws.onclose = function () {
      updateBadge('connecting', 'Reconnecting...');
      dom.telRate.textContent = 'Broker reconnect in progress';
      scheduleRetry();
    };

    state.ws.onerror = function () {
      updateBadge('connecting', 'Reconnecting...');
      scheduleRetry();
    };
  }

  function scheduleRetry() {
    if (state.reconnectTimer) return;
    state.reconnectTimer = setTimeout(() => {
      state.reconnectTimer = null;
      connectWebSocket();
    }, 2500);
  }

  function updateBadge(cls, text) {
    dom.statusBadge.className = `status-badge ${cls}`;
    dom.statusText.textContent = text;
  }

  function sendWS(payload) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      state.ws.send(JSON.stringify(payload));
    }
  }

  function processServerMessage(msg) {
    switch (msg.type) {
      case 'info':
        if (msg.data && Array.isArray(msg.data.symbols)) {
          msg.data.symbols.forEach(s => state.symbols.add(s));
          syncPillsAndDropdown();
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
            renderHistoryTable();
            renderKPI(msg.data[msg.data.length - 1]);
          }
        }
        break;

      case 'signal':
        if (msg.data && msg.data.symbol) {
          handleIncomingSignal(msg.data);
        }
        break;

      case 'alert':
        if (msg.data) {
          addAlert(msg.data, true);
        }
        break;
    }
  }

  function handleIncomingSignal(ev) {
    state.totalWindows++;
    state.symbols.add(ev.symbol);

    if (!state.symbolData[ev.symbol]) {
      state.symbolData[ev.symbol] = [];
    }
    state.symbolData[ev.symbol].push(ev);
    if (state.symbolData[ev.symbol].length > 500) {
      state.symbolData[ev.symbol].shift();
    }

    if (ev.signal === 'BUY' || ev.signal === 'SELL') {
      addAlert(ev, true);
    }

    if (ev.symbol === state.activeSymbol) {
      renderChart();
      renderHistoryTable();
      renderKPI(ev);
    }

    renderTelemetry();
    syncPillsAndDropdown();
  }

  function addAlert(ev, isLive) {
    if (!ev || !ev.signal) return;

    if (ev.signal === 'BUY') state.totalBuys++;
    if (ev.signal === 'SELL') state.totalSells++;

    state.alerts.unshift(ev);
    if (state.alerts.length > 100) state.alerts.pop();

    if (isLive) {
      playChime(ev.signal);
    }

    renderAlerts();
    renderTelemetry();
  }

  // --- Rendering UI Components ---
  function renderKPI(p) {
    if (!p) return;
    dom.kpiPrice.textContent = `€${p.close.toFixed(3)}`;
    dom.kpiExchange.textContent = getExchange(p.symbol);

    const timeStr = formatTime(p.timestamp || p.window_end);
    dom.kpiWindowTime.textContent = `Window: ${timeStr} CEST (w_${p.window_index || 0})`;

    dom.kpiEMA38.textContent = `€${p.ema38.toFixed(3)}`;
    dom.kpiEMA100.textContent = `€${p.ema100.toFixed(3)}`;

    const spread = p.ema38 - p.ema100;
    dom.kpiSpread.textContent = (spread >= 0 ? '+' : '') + spread.toFixed(4);

    if (p.signal === 'BUY') {
      dom.kpiSignalBadge.className = 'kpi-tag buy';
      dom.kpiSignalBadge.textContent = '🟢 BULLISH BUY';
      dom.kpiSignalDesc.textContent = 'Golden Cross: EMA38 overtook EMA100';
    } else if (p.signal === 'SELL') {
      dom.kpiSignalBadge.className = 'kpi-tag sell';
      dom.kpiSignalBadge.textContent = '🔴 BEARISH SELL';
      dom.kpiSignalDesc.textContent = 'Death Cross: EMA38 fell below EMA100';
    } else if (p.ema38 >= p.ema100) {
      dom.kpiSignalBadge.className = 'kpi-tag buy';
      dom.kpiSignalBadge.textContent = 'BULLISH';
      dom.kpiSignalDesc.textContent = 'EMA38 remains above EMA100';
    } else {
      dom.kpiSignalBadge.className = 'kpi-tag sell';
      dom.kpiSignalBadge.textContent = 'BEARISH';
      dom.kpiSignalDesc.textContent = 'EMA38 remains below EMA100';
    }

    dom.chartHeading.textContent = `${p.symbol} (${getExchange(p.symbol)}) — 5-Minute Tumbling Windows`;
  }

  function renderTelemetry() {
    dom.telWindows.textContent = state.totalWindows.toLocaleString();
    dom.telSymbols.textContent = state.symbols.size;
    dom.telBuys.textContent = state.totalBuys.toLocaleString();
    dom.telSells.textContent = state.totalSells.toLocaleString();
    dom.alertCountPill.textContent = state.alerts.length;
  }

  function renderAlerts() {
    const filter = state.alertFilter;
    const filtered = state.alerts.filter(a => filter === 'all' || a.signal === filter);

    if (filtered.length === 0) {
      dom.alertsScrollBody.innerHTML = '<div style="text-align:center;color:var(--text-dim);padding:30px">Awaiting breakout crossover events...</div>';
      return;
    }

    dom.alertsScrollBody.innerHTML = '';
    filtered.slice(0, 30).forEach(a => {
      const isBuy = a.signal === 'BUY';
      const card = document.createElement('div');
      card.className = `alert-card ${isBuy ? 'buy' : 'sell'}`;

      const timeStr = formatTime(a.timestamp || a.window_end);
      const spread = (a.ema38 - a.ema100).toFixed(4);

      card.innerHTML = `
        <div>
          <div style="display:flex;align-items:center;gap:6px">
            <span class="alert-badge ${isBuy ? 'buy' : 'sell'}">${a.signal}</span>
            <span class="alert-sym">${a.symbol}</span>
            <span class="alert-time">${timeStr}</span>
          </div>
          <div class="alert-subtext">
            EMA38: ${a.ema38.toFixed(2)} | EMA100: ${a.ema100.toFixed(2)} (Δ ${spread})
          </div>
        </div>
        <div>
          <div class="alert-price">€${a.close.toFixed(3)}</div>
          <div style="font-size:10px;color:var(--text-dim);text-align:right">${getExchange(a.symbol)}</div>
        </div>
      `;

      card.addEventListener('click', () => selectSymbol(a.symbol));
      dom.alertsScrollBody.appendChild(card);
    });
  }

  function renderHistoryTable() {
    dom.tabHistSym.textContent = state.activeSymbol;
    const points = state.symbolData[state.activeSymbol] || [];

    if (points.length === 0) {
      dom.historyTbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:24px;color:var(--text-dim)">No history points</td></tr>';
      return;
    }

    dom.historyTbody.innerHTML = '';
    const slice = points.slice(-30).reverse();
    slice.forEach(p => {
      const tr = document.createElement('tr');
      const timeStr = formatTime(p.timestamp || p.window_end);

      let sigHtml = '<span style="color:var(--text-dim)">HOLD</span>';
      if (p.signal === 'BUY') sigHtml = '<span style="color:var(--buy);font-weight:800">BUY ▲</span>';
      if (p.signal === 'SELL') sigHtml = '<span style="color:var(--sell);font-weight:800">SELL ▼</span>';

      tr.innerHTML = `
        <td>${timeStr}</td>
        <td>€${p.close.toFixed(3)}</td>
        <td style="color:var(--ema38)">${p.ema38.toFixed(3)}</td>
        <td style="color:var(--ema100)">${p.ema100.toFixed(3)}</td>
        <td>${sigHtml}</td>
      `;
      dom.historyTbody.appendChild(tr);
    });
  }

  function pushDebugLog(msg) {
    state.debugLogs.unshift({
      time: new Date().toLocaleTimeString(),
      type: msg.type || 'frame',
      symbol: msg.symbol || '',
      data: msg.data
    });
    if (state.debugLogs.length > 40) state.debugLogs.pop();
    renderDebugLogs();
  }

  function renderDebugLogs() {
    if (!dom.debugScrollBody) return;
    dom.debugScrollBody.innerHTML = '';

    state.debugLogs.slice(0, 25).forEach(l => {
      const el = document.createElement('div');
      el.className = 'log-item';

      let str = '';
      if (typeof l.data === 'object') {
        str = JSON.stringify(l.data, null, 1);
      } else {
        str = String(l.data);
      }

      el.innerHTML = `
        <div class="log-head">
          <span class="log-badge">${l.type.toUpperCase()}</span>
          ${l.symbol ? `<strong style="color:#ffffff">${l.symbol}</strong>` : ''}
          <span style="color:var(--text-dim)">${l.time}</span>
        </div>
        <div class="log-json">${escapeHtml(str)}</div>
      `;
      dom.debugScrollBody.appendChild(el);
    });
  }

  function escapeHtml(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  // --- High-Performance Visual Canvas Chart ---
  function setupCanvasEngine() {
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

    // Mouse hover tracking
    dom.chartCanvas.addEventListener('mousemove', e => {
      const rect = dom.chartCanvas.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const points = state.symbolData[state.activeSymbol] || [];
      if (points.length < 2) return;

      const padL = 40;
      const padR = 80;
      const plotW = rect.width - padL - padR;
      const ratio = Math.max(0, Math.min(1, (x - padL) / plotW));
      const idx = Math.round(ratio * (points.length - 1));

      if (idx >= 0 && idx < points.length) {
        state.hoverIndex = idx;
        const p = points[idx];
        const timeStr = formatTime(p.timestamp || p.window_end);
        const spread = (p.ema38 - p.ema100).toFixed(4);
        dom.chartCursorInfo.innerHTML = `<strong>${timeStr}</strong> &nbsp;|&nbsp; Close: <strong>€${p.close.toFixed(3)}</strong> &nbsp;|&nbsp; <span style="color:var(--ema38)">EMA38: ${p.ema38.toFixed(3)}</span> &nbsp;|&nbsp; <span style="color:var(--ema100)">EMA100: ${p.ema100.toFixed(3)}</span> &nbsp;|&nbsp; Δ: <strong>${spread}</strong>`;
        renderChart();
      }
    });

    dom.chartCanvas.addEventListener('mouseleave', () => {
      state.hoverIndex = -1;
      dom.chartCursorInfo.textContent = 'Hover over chart to inspect window values';
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

    const points = state.symbolData[state.activeSymbol] || [];
    if (points.length < 2) return;

    const padL = 40 * dpr;
    const padR = 85 * dpr;
    const padT = 30 * dpr;
    const padB = 40 * dpr;
    const plotW = w - padL - padR;
    const plotH = h - padT - padB;

    let minVal = Infinity;
    let maxVal = -Infinity;

    points.forEach(p => {
      minVal = Math.min(minVal, p.close, p.ema38, p.ema100);
      maxVal = Math.max(maxVal, p.close, p.ema38, p.ema100);
    });

    const range = maxVal - minVal || 1.0;
    minVal -= range * 0.08;
    maxVal += range * 0.08;

    function getX(i) {
      return padL + (i / (points.length - 1)) * plotW;
    }
    function getY(val) {
      return padT + plotH - ((val - minVal) / (maxVal - minVal)) * plotH;
    }

    // Grid lines & price axis
    ctx.strokeStyle = '#172233';
    ctx.lineWidth = 1 * dpr;
    const gridRows = 6;
    for (let r = 0; r <= gridRows; r++) {
      const gy = padT + (r / gridRows) * plotH;
      ctx.beginPath();
      ctx.moveTo(padL, gy);
      ctx.lineTo(w - padR, gy);
      ctx.stroke();

      const priceVal = maxVal - (r / gridRows) * (maxVal - minVal);
      ctx.fillStyle = '#8fa0b5';
      ctx.font = `${10 * dpr}px monospace`;
      ctx.textAlign = 'left';
      ctx.fillText(`€${priceVal.toFixed(3)}`, w - padR + 10 * dpr, gy + 3 * dpr);
    }

    // Time axis labels
    const timeSteps = 5;
    for (let s = 0; s <= timeSteps; s++) {
      const idx = Math.floor((s / timeSteps) * (points.length - 1));
      const gx = getX(idx);
      const timeStr = formatTime(points[idx].timestamp || points[idx].window_end);

      ctx.fillStyle = '#64748b';
      ctx.font = `${10 * dpr}px monospace`;
      ctx.textAlign = 'center';
      ctx.fillText(timeStr, gx, h - 14 * dpr);
    }

    // Area fill under price
    const grad = ctx.createLinearGradient(0, padT, 0, padT + plotH);
    grad.addColorStop(0, 'rgba(59, 130, 246, 0.22)');
    grad.addColorStop(1, 'rgba(59, 130, 246, 0.00)');

    ctx.beginPath();
    points.forEach((p, i) => {
      const x = getX(i);
      const y = getY(p.close);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.lineTo(getX(points.length - 1), padT + plotH);
    ctx.lineTo(getX(0), padT + plotH);
    ctx.closePath();
    ctx.fillStyle = grad;
    ctx.fill();

    // Price line (Crisp White with shadow)
    ctx.beginPath();
    points.forEach((p, i) => {
      const x = getX(i);
      const y = getY(p.close);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = '#ffffff';
    ctx.lineWidth = 2 * dpr;
    ctx.shadowColor = 'rgba(255, 255, 255, 0.3)';
    ctx.shadowBlur = 6;
    ctx.stroke();
    ctx.shadowBlur = 0;

    // EMA 38 Line (Neon Cyan)
    if (dom.chkEMA38.checked) {
      ctx.beginPath();
      points.forEach((p, i) => {
        const x = getX(i);
        const y = getY(p.ema38);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.strokeStyle = '#00e5ff';
      ctx.lineWidth = 2.5 * dpr;
      ctx.shadowColor = 'rgba(0, 229, 255, 0.5)';
      ctx.shadowBlur = 8;
      ctx.stroke();
      ctx.shadowBlur = 0;
    }

    // EMA 100 Line (Warm Amber)
    if (dom.chkEMA100.checked) {
      ctx.beginPath();
      points.forEach((p, i) => {
        const x = getX(i);
        const y = getY(p.ema100);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.strokeStyle = '#ff9100';
      ctx.lineWidth = 2.5 * dpr;
      ctx.shadowColor = 'rgba(255, 145, 0, 0.5)';
      ctx.shadowBlur = 8;
      ctx.stroke();
      ctx.shadowBlur = 0;
    }

    // Crossover Markers (Green / Red arrows + tags)
    if (dom.chkMarkers.checked) {
      points.forEach((p, i) => {
        if (p.signal === 'BUY' || p.signal === 'SELL') {
          const x = getX(i);
          const y = getY(p.close);
          const isBuy = p.signal === 'BUY';

          ctx.fillStyle = isBuy ? '#10b981' : '#ef4444';
          ctx.beginPath();
          if (isBuy) {
            // Up arrow
            ctx.moveTo(x, y + 16 * dpr);
            ctx.lineTo(x - 7 * dpr, y + 28 * dpr);
            ctx.lineTo(x + 7 * dpr, y + 28 * dpr);
          } else {
            // Down arrow
            ctx.moveTo(x, y - 16 * dpr);
            ctx.lineTo(x - 7 * dpr, y - 28 * dpr);
            ctx.lineTo(x + 7 * dpr, y - 28 * dpr);
          }
          ctx.fill();

          // Badge tag
          ctx.font = `800 ${9 * dpr}px sans-serif`;
          ctx.textAlign = 'center';
          ctx.fillText(p.signal, x, isBuy ? y + 40 * dpr : y - 32 * dpr);
        }
      });
    }

    // Crosshair cursor line
    if (state.hoverIndex >= 0 && state.hoverIndex < points.length) {
      const hx = getX(state.hoverIndex);
      const hp = points[state.hoverIndex];
      const hy = getY(hp.close);

      ctx.strokeStyle = 'rgba(255, 255, 255, 0.4)';
      ctx.setLineDash([4 * dpr, 4 * dpr]);
      ctx.beginPath();
      ctx.moveTo(hx, padT);
      ctx.lineTo(hx, padT + plotH);
      ctx.moveTo(padL, hy);
      ctx.lineTo(w - padR, hy);
      ctx.stroke();
      ctx.setLineDash([]);

      // Point dots
      ctx.fillStyle = '#ffffff';
      ctx.beginPath();
      ctx.arc(hx, hy, 4 * dpr, 0, Math.PI * 2);
      ctx.fill();
    }
  }

  // --- Helpers & UI Sync ---
  function syncPillsAndDropdown() {
    dom.symbolSelect.innerHTML = '';
    const sorted = Array.from(state.symbols).sort();
    sorted.forEach(s => {
      const opt = document.createElement('option');
      opt.value = s;
      opt.textContent = `${s} (${getExchange(s)})`;
      if (s === state.activeSymbol) opt.selected = true;
      dom.symbolSelect.appendChild(opt);
    });

    dom.symbolPills.innerHTML = '';
    sorted.slice(0, 6).forEach(s => {
      const btn = document.createElement('button');
      btn.className = `sym-pill ${s === state.activeSymbol ? 'active' : ''}`;
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
    sendWS({ action: 'subscribe', symbol: state.activeSymbol });
    sendWS({ action: 'get_history', symbol: state.activeSymbol });

    syncPillsAndDropdown();

    const points = state.symbolData[sym] || [];
    if (points.length > 0) {
      renderKPI(points[points.length - 1]);
    }
    renderChart();
    renderHistoryTable();
  }

  function formatTime(val) {
    if (!val) return '--:--:--';
    const d = new Date(val);
    if (isNaN(d.getTime())) return String(val).substring(11, 19) || String(val);
    return d.toTimeString().split(' ')[0];
  }

  // --- Listeners ---
  function bindEventListeners() {
    dom.symbolSelect.addEventListener('change', e => selectSymbol(e.target.value));

    dom.btnSound.addEventListener('click', () => {
      state.soundEnabled = !state.soundEnabled;
      dom.btnSound.textContent = state.soundEnabled ? '🔔 Sound: ON' : '🔕 Sound: OFF';
    });

    dom.btnReconnect.addEventListener('click', () => connectWebSocket());

    [dom.chkEMA38, dom.chkEMA100, dom.chkMarkers].forEach(el => {
      el.addEventListener('change', () => renderChart());
    });

    dom.btnFit.addEventListener('click', () => {
      state.hoverIndex = -1;
      renderChart();
    });

    // Tab switching
    document.querySelectorAll('.tab-item').forEach(btn => {
      btn.addEventListener('click', e => {
        document.querySelectorAll('.tab-item').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-view').forEach(v => v.classList.remove('active'));

        const targetId = btn.getAttribute('data-tab');
        btn.classList.add('active');
        document.getElementById(targetId).classList.add('active');
      });
    });

    // Alert filter buttons
    document.querySelectorAll('.pill-filter').forEach(btn => {
      btn.addEventListener('click', e => {
        document.querySelectorAll('.pill-filter').forEach(b => b.classList.remove('active'));
        e.target.classList.add('active');
        state.alertFilter = e.target.getAttribute('data-filter');
        renderAlerts();
      });
    });

    dom.btnClearDebug.addEventListener('click', () => {
      state.debugLogs = [];
      renderDebugLogs();
    });
  }

  // --- Startup ---
  function init() {
    seedInitialData();
    syncPillsAndDropdown();
    bindEventListeners();
    setupCanvasEngine();

    // Render initial state immediately
    const initialPoints = state.symbolData[state.activeSymbol];
    if (initialPoints && initialPoints.length > 0) {
      renderKPI(initialPoints[initialPoints.length - 1]);
      renderHistoryTable();
      renderAlerts();
      renderTelemetry();
    }

    // Connect WebSocket
    connectWebSocket();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
