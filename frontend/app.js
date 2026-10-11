/**
 * Trading Trends Terminal - Frontend Application
 * CS-E4780 Scalable Systems & Data Management
 * Handles WebSocket connection, subscription filtering, TradingView charting,
 * real-time breakout advisory feed, and audio alerts.
 */

(function () {
  'use strict';

  // --- State Configuration ---
  const state = {
    ws: null,
    wsUrl: '',
    reconnectAttempts: 0,
    activeSymbol: 'RDSA.NL',
    symbols: new Set(['RDSA.NL', 'AIR.FR', 'BMW.ETR', 'MC.FR', 'ASML.NL', 'SAN.FR']),
    symbolData: {}, // symbol -> array of window points
    alerts: [],     // array of recent breakout alerts
    soundEnabled: true,
    totalWindows: 0,
    totalBuys: 0,
    totalSells: 0,
    filterDirection: 'all',
    useTradingView: false,
    chart: null,
    series: {
      price: null,
      ema38: null,
      ema100: null,
    },
    markers: [],
  };

  // --- Exchange Labels ---
  const EXCHANGE_MAP = {
    NL: 'Amsterdam (NL)',
    FR: 'Paris (FR)',
    ETR: 'Frankfurt / Xetra (ETR)',
  };

  function getExchangeName(symbol) {
    const parts = symbol.split('.');
    if (parts.length > 1 && EXCHANGE_MAP[parts[1]]) {
      return EXCHANGE_MAP[parts[1]];
    }
    return 'Exchange';
  }

  // --- Audio Synthesis via Web Audio API ---
  let audioCtx = null;
  function playAlertChime(direction) {
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
        // High ascending chime
        osc.frequency.setValueAtTime(659.25, now); // E5
        osc.frequency.exponentialRampToValueAtTime(1046.5, now + 0.18); // C6
        gain.gain.setValueAtTime(0.12, now);
        gain.gain.exponentialRampToValueAtTime(0.001, now + 0.35);
        osc.start(now);
        osc.stop(now + 0.35);
      } else {
        // Deep descending chime
        osc.frequency.setValueAtTime(587.33, now); // D5
        osc.frequency.exponentialRampToValueAtTime(369.99, now + 0.18); // F#4
        gain.gain.setValueAtTime(0.12, now);
        gain.gain.exponentialRampToValueAtTime(0.001, now + 0.35);
        osc.start(now);
        osc.stop(now + 0.35);
      }
    } catch (e) {
      console.warn('Audio chime failed:', e);
    }
  }

  // --- DOM Elements ---
  const dom = {
    statusBadge: document.getElementById('connection-status'),
    totalWindows: document.getElementById('total-windows'),
    activeSymbolsCount: document.getElementById('active-symbols-count'),
    totalBuyAlerts: document.getElementById('total-buy-alerts'),
    totalSellAlerts: document.getElementById('total-sell-alerts'),
    quickSymbols: document.getElementById('quick-symbols'),
    symbolSearch: document.getElementById('symbol-search'),
    chartSymbol: document.getElementById('chart-symbol'),
    chartExchange: document.getElementById('chart-exchange'),
    chartPrice: document.getElementById('chart-price'),
    chartSignalBadge: document.getElementById('chart-signal-badge'),
    valEMA38: document.getElementById('val-ema38'),
    valEMA100: document.getElementById('val-ema100'),
    valSpread: document.getElementById('val-spread'),
    chartContainer: document.getElementById('chart-container'),
    fallbackCanvas: document.getElementById('fallback-canvas'),
    windowTimestamp: document.getElementById('window-timestamp'),
    alertsList: document.getElementById('alerts-list'),
    marketTableBody: document.getElementById('market-table-body'),
    btnSound: document.getElementById('btn-sound'),
    btnReconnect: document.getElementById('btn-reconnect'),
    wsUrlInput: document.getElementById('ws-url-input'),
    btnApplyWS: document.getElementById('btn-apply-ws'),
    toggleEMA38: document.getElementById('toggle-ema38'),
    toggleEMA100: document.getElementById('toggle-ema100'),
    toggleMarkers: document.getElementById('toggle-markers'),
  };

  // --- WebSocket Setup ---
  function initWebSocket() {
    const host = window.location.hostname || 'localhost';
    const defaultUrl = `ws://${host}:8081/ws`;
    state.wsUrl = dom.wsUrlInput.value.trim() || defaultUrl;
    dom.wsUrlInput.value = state.wsUrl;

    updateStatus('connecting', 'Connecting...');

    if (state.ws) {
      try {
        state.ws.close();
      } catch (e) {}
    }

    try {
      state.ws = new WebSocket(state.wsUrl);
    } catch (err) {
      console.error('WebSocket creation error:', err);
      updateStatus('disconnected', 'Failed');
      scheduleReconnect();
      return;
    }

    state.ws.onopen = function () {
      console.log('[WS] Connected to Gateway:', state.wsUrl);
      updateStatus('connected', 'Live');
      state.reconnectAttempts = 0;

      // Subscribe to active symbol
      sendWSMessage({ action: 'subscribe', symbol: state.activeSymbol });
      sendWSMessage({ action: 'get_history', symbol: state.activeSymbol });
    };

    state.ws.onmessage = function (event) {
      try {
        // Can receive newline-delimited JSON frames
        const lines = event.data.split('\n');
        for (const line of lines) {
          if (!line.trim()) continue;
          const msg = JSON.parse(line);
          handleServerMessage(msg);
        }
      } catch (err) {
        console.error('[WS] Error processing message:', err, event.data);
      }
    };

    state.ws.onclose = function () {
      updateStatus('disconnected', 'Disconnected');
      scheduleReconnect();
    };

    state.ws.onerror = function (err) {
      console.warn('[WS] Error:', err);
      updateStatus('disconnected', 'Error');
    };
  }

  function scheduleReconnect() {
    state.reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(1.5, state.reconnectAttempts), 10000);
    setTimeout(initWebSocket, delay);
  }

  function updateStatus(statusClass, label) {
    dom.statusBadge.className = `status-badge ${statusClass}`;
    dom.statusBadge.textContent = label;
  }

  function sendWSMessage(payload) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
      state.ws.send(JSON.stringify(payload));
    }
  }

  // --- Message Dispatcher ---
  function handleServerMessage(msg) {
    switch (msg.type) {
      case 'info':
        handleInfoMessage(msg.data);
        break;
      case 'history':
        handleHistoryMessage(msg.symbol, msg.data);
        break;
      case 'signal':
        handleSignalMessage(msg.data);
        break;
      case 'alert':
        handleAlertMessage(msg.data);
        break;
      case 'symbols':
        if (Array.isArray(msg.data)) {
          msg.data.forEach((s) => state.symbols.add(s));
          renderQuickSymbols();
        }
        break;
      case 'pong':
        break;
      default:
        console.log('[WS] Unhandled message type:', msg.type);
    }
  }

  function handleInfoMessage(data) {
    if (!data) return;
    if (Array.isArray(data.symbols)) {
      data.symbols.forEach((s) => state.symbols.add(s));
      renderQuickSymbols();
    }
    if (data.stats) {
      state.totalWindows = data.stats.total_signals || 0;
      state.totalBuys = data.stats.total_buy_alerts || 0;
      state.totalSells = data.stats.total_sell_alerts || 0;
      updateHeaderTelemetry();
    }
    if (Array.isArray(data.recent_alerts)) {
      data.recent_alerts.forEach((alert) => addAlertToFeed(alert, false));
    }
  }

  function handleHistoryMessage(symbol, points) {
    if (!symbol || !Array.isArray(points)) return;
    state.symbolData[symbol] = points;

    if (symbol === state.activeSymbol) {
      renderActiveChart();
      if (points.length > 0) {
        updateActiveHeader(points[points.length - 1]);
      }
    }
    updateMarketTableRow(symbol);
  }

  function handleSignalMessage(event) {
    if (!event || !event.symbol) return;

    state.totalWindows++;
    state.symbols.add(event.symbol);

    // Save in symbol history
    if (!state.symbolData[event.symbol]) {
      state.symbolData[event.symbol] = [];
    }
    state.symbolData[event.symbol].push(event);
    if (state.symbolData[event.symbol].length > 500) {
      state.symbolData[event.symbol].shift();
    }

    // Check if this event has a BUY or SELL crossover
    if (event.signal === 'BUY' || event.signal === 'SELL') {
      addAlertToFeed(event, true);
    }

    // Update active chart if it matches current symbol
    if (event.symbol === state.activeSymbol) {
      appendPointToChart(event);
      updateActiveHeader(event);
    }

    updateMarketTableRow(event.symbol);
    updateHeaderTelemetry();
  }

  function handleAlertMessage(event) {
    if (!event) return;
    addAlertToFeed(event, true);
    if (event.symbol) {
      updateMarketTableRow(event.symbol);
    }
  }

  // --- Alert Feed ---
  function addAlertToFeed(alert, isLive) {
    if (!alert || !alert.signal) return;

    if (alert.signal === 'BUY') state.totalBuys++;
    if (alert.signal === 'SELL') state.totalSells++;

    state.alerts.unshift(alert);
    if (state.alerts.length > 100) {
      state.alerts.pop();
    }

    if (isLive) {
      playAlertChime(alert.signal);
    }

    renderAlertsFeed();
    updateHeaderTelemetry();
  }

  function renderAlertsFeed() {
    const filter = state.filterDirection;
    const filtered = state.alerts.filter((a) => {
      if (filter === 'all') return true;
      return a.signal === filter;
    });

    if (filtered.length === 0) {
      dom.alertsList.innerHTML = '<div class="empty-state">No advisory alerts found</div>';
      return;
    }

    dom.alertsList.innerHTML = '';
    filtered.slice(0, 50).forEach((alert) => {
      const isBuy = alert.signal === 'BUY';
      const item = document.createElement('div');
      item.className = `alert-item ${isBuy ? 'buy' : 'sell'}`;

      const timeStr = formatTime(alert.timestamp || alert.window_end);
      const spread = (alert.ema38 - alert.ema100).toFixed(4);

      item.innerHTML = `
        <div class="alert-item-left">
          <div class="alert-item-title">
            <span class="alert-badge ${isBuy ? 'buy' : 'sell'}">${alert.signal}</span>
            <span class="alert-sym">${alert.symbol}</span>
            <span class="alert-time">${timeStr}</span>
          </div>
          <div class="alert-spread">
            EMA38: ${alert.ema38.toFixed(2)} | EMA100: ${alert.ema100.toFixed(2)} (Δ ${spread})
          </div>
        </div>
        <div class="alert-item-right">
          <span class="alert-price">${alert.close.toFixed(3)}</span>
          <span class="badge-exchange">${getExchangeName(alert.symbol)}</span>
        </div>
      `;

      item.addEventListener('click', () => {
        selectSymbol(alert.symbol);
      });

      dom.alertsList.appendChild(item);
    });
  }

  // --- Market Overview Table ---
  function updateMarketTableRow(symbol) {
    const points = state.symbolData[symbol];
    if (!points || points.length === 0) return;
    const latest = points[points.length - 1];

    let row = document.getElementById(`row-${symbol}`);
    if (!row) {
      row = document.createElement('tr');
      row.id = `row-${symbol}`;
      row.addEventListener('click', () => selectSymbol(symbol));
      dom.marketTableBody.appendChild(row);

      // Remove placeholder row if present
      const emptyCell = dom.marketTableBody.querySelector('.empty-cell');
      if (emptyCell) {
        emptyCell.parentElement.remove();
      }
    }

    const isBull = latest.ema38 >= latest.ema100;
    const trendText = isBull ? 'BULLISH ▲' : 'BEARISH ▼';
    const trendClass = isBull ? 'trend-bullish' : 'trend-bearish';

    row.innerHTML = `
      <td>${symbol}</td>
      <td>${latest.close.toFixed(3)}</td>
      <td>${latest.ema38.toFixed(3)}</td>
      <td>${latest.ema100.toFixed(3)}</td>
      <td class="${trendClass}">${trendText}</td>
    `;
  }

  // --- Active Header & Telemetry ---
  function updateHeaderTelemetry() {
    dom.totalWindows.textContent = state.totalWindows.toLocaleString();
    dom.activeSymbolsCount.textContent = state.symbols.size;
    dom.totalBuyAlerts.textContent = state.totalBuys.toLocaleString();
    dom.totalSellAlerts.textContent = state.totalSells.toLocaleString();
  }

  function updateActiveHeader(point) {
    dom.chartSymbol.textContent = point.symbol;
    dom.chartExchange.textContent = getExchangeName(point.symbol);
    dom.chartPrice.textContent = point.close.toFixed(3);
    dom.valEMA38.textContent = point.ema38.toFixed(3);
    dom.valEMA100.textContent = point.ema100.toFixed(3);

    const spread = point.ema38 - point.ema100;
    dom.valSpread.textContent = (spread >= 0 ? '+' : '') + spread.toFixed(4);

    if (point.signal === 'BUY') {
      dom.chartSignalBadge.className = 'quote-signal buy';
      dom.chartSignalBadge.textContent = '🟢 BUY BREAKOUT';
    } else if (point.signal === 'SELL') {
      dom.chartSignalBadge.className = 'quote-signal sell';
      dom.chartSignalBadge.textContent = '🔴 SELL BREAKOUT';
    } else if (point.ema38 > point.ema100) {
      dom.chartSignalBadge.className = 'quote-signal buy';
      dom.chartSignalBadge.textContent = 'BULLISH TREND';
    } else {
      dom.chartSignalBadge.className = 'quote-signal sell';
      dom.chartSignalBadge.textContent = 'BEARISH TREND';
    }

    if (point.timestamp || point.window_end) {
      dom.windowTimestamp.textContent = `Last Window: ${formatTime(point.timestamp || point.window_end)}`;
    }
  }

  // --- Quick Symbols Watchlist ---
  function renderQuickSymbols() {
    dom.quickSymbols.innerHTML = '';
    const sorted = Array.from(state.symbols).sort();
    sorted.forEach((sym) => {
      const pill = document.createElement('button');
      pill.className = `symbol-pill ${sym === state.activeSymbol ? 'active' : ''}`;
      pill.textContent = sym;
      pill.addEventListener('click', () => selectSymbol(sym));
      dom.quickSymbols.appendChild(pill);
    });
  }

  function selectSymbol(symbol) {
    if (!symbol || symbol === state.activeSymbol) return;

    // Unsubscribe previous, subscribe new
    sendWSMessage({ action: 'unsubscribe', symbol: state.activeSymbol });
    state.activeSymbol = symbol;
    sendWSMessage({ action: 'subscribe', symbol: state.activeSymbol });
    sendWSMessage({ action: 'get_history', symbol: state.activeSymbol });

    renderQuickSymbols();

    // Check if we already have data cached locally
    if (state.symbolData[symbol] && state.symbolData[symbol].length > 0) {
      renderActiveChart();
      updateActiveHeader(state.symbolData[symbol][state.symbolData[symbol].length - 1]);
    } else {
      dom.chartSymbol.textContent = symbol;
      dom.chartExchange.textContent = getExchangeName(symbol);
      dom.chartPrice.textContent = '--.--';
      dom.valEMA38.textContent = '--.--';
      dom.valEMA100.textContent = '--.--';
      dom.valSpread.textContent = '--.--';
      dom.chartSignalBadge.className = 'quote-signal neutral';
      dom.chartSignalBadge.textContent = 'LOADING HISTORY...';
      renderActiveChart();
    }
  }

  // ============================================================
  // Charting: TradingView Lightweight Charts + Fallback Canvas
  // ============================================================
  function initTradingView() {
    if (typeof window.LightweightCharts !== 'undefined') {
      try {
        state.useTradingView = true;
        dom.fallbackCanvas.style.display = 'none';

        const chart = window.LightweightCharts.createChart(dom.chartContainer, {
          width: dom.chartContainer.clientWidth,
          height: dom.chartContainer.clientHeight,
          layout: {
            background: { color: '#080c10' },
            textColor: '#8b949e',
          },
          grid: {
            vertLines: { color: '#161f2c' },
            horzLines: { color: '#161f2c' },
          },
          crosshair: {
            mode: window.LightweightCharts.CrosshairMode.Normal,
          },
          timeScale: {
            borderColor: '#273546',
            timeVisible: true,
            secondsVisible: false,
          },
          rightPriceScale: {
            borderColor: '#273546',
          },
        });

        state.chart = chart;

        // Price Area series
        state.series.price = chart.addAreaSeries({
          lineColor: '#3b82f6',
          topColor: 'rgba(59, 130, 246, 0.28)',
          bottomColor: 'rgba(59, 130, 246, 0.00)',
          lineWidth: 2,
          priceFormat: { type: 'price', precision: 3, minMove: 0.001 },
        });

        // EMA 38 Line (Cyan)
        state.series.ema38 = chart.addLineSeries({
          color: '#29b6f6',
          lineWidth: 2,
          title: 'EMA(38)',
          priceFormat: { type: 'price', precision: 3, minMove: 0.001 },
        });

        // EMA 100 Line (Amber)
        state.series.ema100 = chart.addLineSeries({
          color: '#ff9100',
          lineWidth: 2,
          title: 'EMA(100)',
          priceFormat: { type: 'price', precision: 3, minMove: 0.001 },
        });

        window.addEventListener('resize', () => {
          chart.applyOptions({
            width: dom.chartContainer.clientWidth,
            height: dom.chartContainer.clientHeight,
          });
        });

        console.log('[Chart] TradingView Lightweight Charts initialized successfully');
        return;
      } catch (err) {
        console.warn('[Chart] Lightweight Charts setup failed, falling back to native Canvas:', err);
      }
    }

    // Fallback to HTML5 Canvas
    state.useTradingView = false;
    dom.fallbackCanvas.style.display = 'block';
    setupFallbackCanvas();
  }

  function renderActiveChart() {
    const points = state.symbolData[state.activeSymbol] || [];

    if (state.useTradingView && state.chart) {
      const priceData = [];
      const ema38Data = [];
      const ema100Data = [];
      const markers = [];

      points.forEach((p) => {
        const timeSec = parseUnixSeconds(p.timestamp || p.window_end);
        priceData.push({ time: timeSec, value: p.close });
        ema38Data.push({ time: timeSec, value: p.ema38 });
        ema100Data.push({ time: timeSec, value: p.ema100 });

        if (dom.toggleMarkers.checked) {
          if (p.signal === 'BUY') {
            markers.push({
              time: timeSec,
              position: 'belowBar',
              color: '#00e676',
              shape: 'arrowUp',
              text: 'BUY',
            });
          } else if (p.signal === 'SELL') {
            markers.push({
              time: timeSec,
              position: 'aboveBar',
              color: '#ff5252',
              shape: 'arrowDown',
              text: 'SELL',
            });
          }
        }
      });

      state.series.price.setData(priceData);
      state.series.ema38.setData(dom.toggleEMA38.checked ? ema38Data : []);
      state.series.ema100.setData(dom.toggleEMA100.checked ? ema100Data : []);
      state.series.price.setMarkers(markers);
      state.chart.timeScale().fitContent();
    } else {
      drawFallbackCanvas(points);
    }
  }

  function appendPointToChart(p) {
    if (state.useTradingView && state.chart) {
      const timeSec = parseUnixSeconds(p.timestamp || p.window_end);
      state.series.price.update({ time: timeSec, value: p.close });
      if (dom.toggleEMA38.checked) state.series.ema38.update({ time: timeSec, value: p.ema38 });
      if (dom.toggleEMA100.checked) state.series.ema100.update({ time: timeSec, value: p.ema100 });

      if (dom.toggleMarkers.checked && (p.signal === 'BUY' || p.signal === 'SELL')) {
        renderActiveChart(); // refresh markers cleanly
      }
    } else {
      drawFallbackCanvas(state.symbolData[state.activeSymbol] || []);
    }
  }

  // --- Fallback Canvas Engine ---
  function setupFallbackCanvas() {
    function resize() {
      const rect = dom.chartContainer.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      dom.fallbackCanvas.width = rect.width * dpr;
      dom.fallbackCanvas.height = rect.height * dpr;
      dom.fallbackCanvas.style.width = rect.width + 'px';
      dom.fallbackCanvas.style.height = rect.height + 'px';
      drawFallbackCanvas(state.symbolData[state.activeSymbol] || []);
    }
    window.addEventListener('resize', resize);
    resize();
  }

  function drawFallbackCanvas(points) {
    const canvas = dom.fallbackCanvas;
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.width;
    const h = canvas.height;

    ctx.clearRect(0, 0, w, h);

    if (!points || points.length < 2) {
      ctx.fillStyle = '#6e7681';
      ctx.font = `${13 * dpr}px sans-serif`;
      ctx.textAlign = 'center';
      ctx.fillText('Awaiting window stream for ' + state.activeSymbol + '...', w / 2, h / 2);
      return;
    }

    // Margins
    const padL = 40 * dpr;
    const padR = 70 * dpr;
    const padT = 30 * dpr;
    const padB = 40 * dpr;
    const plotW = w - padL - padR;
    const plotH = h - padT - padB;

    // Calculate Min & Max values
    let minVal = Infinity;
    let maxVal = -Infinity;

    points.forEach((p) => {
      minVal = Math.min(minVal, p.close, p.ema38, p.ema100);
      maxVal = Math.max(maxVal, p.close, p.ema38, p.ema100);
    });

    const valRange = maxVal - minVal || 1.0;
    minVal -= valRange * 0.05;
    maxVal += valRange * 0.05;

    function getX(i) {
      return padL + (i / (points.length - 1)) * plotW;
    }
    function getY(val) {
      return padT + plotH - ((val - minVal) / (maxVal - minVal)) * plotH;
    }

    // Grid Lines
    ctx.strokeStyle = '#161f2c';
    ctx.lineWidth = 1 * dpr;
    const gridSteps = 5;
    for (let g = 0; g <= gridSteps; g++) {
      const gy = padT + (g / gridSteps) * plotH;
      ctx.beginPath();
      ctx.moveTo(padL, gy);
      ctx.lineTo(w - padR, gy);
      ctx.stroke();

      const priceAtY = maxVal - (g / gridSteps) * (maxVal - minVal);
      ctx.fillStyle = '#8b949e';
      ctx.font = `${10 * dpr}px monospace`;
      ctx.textAlign = 'left';
      ctx.fillText(priceAtY.toFixed(3), w - padR + 8 * dpr, gy + 3 * dpr);
    }

    // Price Area + Line
    ctx.beginPath();
    points.forEach((p, i) => {
      const x = getX(i);
      const y = getY(p.close);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = '#3b82f6';
    ctx.lineWidth = 2 * dpr;
    ctx.stroke();

    // EMA 38 Line (Cyan)
    if (dom.toggleEMA38.checked) {
      ctx.beginPath();
      points.forEach((p, i) => {
        const x = getX(i);
        const y = getY(p.ema38);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.strokeStyle = '#29b6f6';
      ctx.lineWidth = 2 * dpr;
      ctx.stroke();
    }

    // EMA 100 Line (Amber)
    if (dom.toggleEMA100.checked) {
      ctx.beginPath();
      points.forEach((p, i) => {
        const x = getX(i);
        const y = getY(p.ema100);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.strokeStyle = '#ff9100';
      ctx.lineWidth = 2 * dpr;
      ctx.stroke();
    }

    // Crossover Markers
    if (dom.toggleMarkers.checked) {
      points.forEach((p, i) => {
        if (p.signal === 'BUY' || p.signal === 'SELL') {
          const x = getX(i);
          const y = getY(p.close);
          const isBuy = p.signal === 'BUY';

          ctx.fillStyle = isBuy ? '#00e676' : '#ff5252';
          ctx.beginPath();
          if (isBuy) {
            // Up Arrow
            ctx.moveTo(x, y + 16 * dpr);
            ctx.lineTo(x - 6 * dpr, y + 26 * dpr);
            ctx.lineTo(x + 6 * dpr, y + 26 * dpr);
          } else {
            // Down Arrow
            ctx.moveTo(x, y - 16 * dpr);
            ctx.lineTo(x - 6 * dpr, y - 26 * dpr);
            ctx.lineTo(x + 6 * dpr, y - 26 * dpr);
          }
          ctx.fill();

          ctx.font = `bold ${9 * dpr}px sans-serif`;
          ctx.textAlign = 'center';
          ctx.fillText(p.signal, x, isBuy ? y + 36 * dpr : y - 30 * dpr);
        }
      });
    }
  }

  // --- Utility Functions ---
  function parseUnixSeconds(val) {
    if (!val) return Math.floor(Date.now() / 1000);
    if (typeof val === 'number') {
      return val > 1e11 ? Math.floor(val / 1000) : val;
    }
    const d = new Date(val);
    return Math.floor(d.getTime() / 1000) || Math.floor(Date.now() / 1000);
  }

  function formatTime(val) {
    if (!val) return '--:--:--';
    const d = new Date(val);
    if (isNaN(d.getTime())) return String(val).substring(11, 19) || String(val);
    return d.toTimeString().split(' ')[0];
  }

  // --- Event Listeners ---
  function setupListeners() {
    // Sound Toggle
    dom.btnSound.addEventListener('click', () => {
      state.soundEnabled = !state.soundEnabled;
      dom.btnSound.className = `btn-icon ${state.soundEnabled ? 'active' : ''}`;
      dom.btnSound.textContent = state.soundEnabled ? '🔔 Sound: ON' : '🔕 Sound: OFF';
    });

    // Reconnect Button
    dom.btnReconnect.addEventListener('click', () => {
      initWebSocket();
    });

    // WS Apply URL
    dom.btnApplyWS.addEventListener('click', () => {
      initWebSocket();
    });

    // Chart Toggles
    [dom.toggleEMA38, dom.toggleEMA100, dom.toggleMarkers].forEach((el) => {
      el.addEventListener('change', () => renderActiveChart());
    });

    // Alert Filter Pills
    document.querySelectorAll('.filter-pill').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        document.querySelectorAll('.filter-pill').forEach((b) => b.classList.remove('active'));
        e.target.classList.add('active');
        state.filterDirection = e.target.getAttribute('data-filter');
        renderAlertsFeed();
      });
    });

    // Symbol Search
    dom.symbolSearch.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        const val = dom.symbolSearch.value.trim().toUpperCase();
        if (val) {
          state.symbols.add(val);
          selectSymbol(val);
          dom.symbolSearch.value = '';
        }
      }
    });
  }

  // --- Initialization ---
  function init() {
    renderQuickSymbols();
    setupListeners();
    initTradingView();
    initWebSocket();
  }

  // Start when DOM is loaded
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();

