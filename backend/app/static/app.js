const state = {
  view: "events",
  devices: [],
  deviceId: "",
  events: { limit: 50, offset: 0, total: 0, items: [] },
  codes: { limit: 100, offset: 0, total: 0, items: [] },
  q: "",
  direction: "",
  passesOnly: false,
};

const PASS_CODES = new Set([11, 96, 97, 98]);

const EVENT_TEXT = {
  1: "Отказ: карта не зарегистрирована",
  2: "Отказ: карта недействительна",
  3: "Попытка доступа с нарушением",
  4: "Запрет прохода",
  5: "Запрет прохода, смена РКД",
  6: "Запрет прохода, нарушение РКД",
  7: "Запрет: неисправны датчики ИУ",
  8: "Запрет по команде оператора",
  9: "Запрет по команде от ДУ",
  10: "Отказ от прохода",
  11: "Проход",
  12: "Ошибка идентификации",
  13: "Ошибка идентификации (отпечаток)",
  14: "Ошибка идентификации (таймаут)",
  15: "Ошибка анализа прав доступа",
  16: "Тревожный отпечаток",
  17: "ИУ не закрыто после прохода",
  18: "Запрет: не задано время разблокировки",
  24: "Доступ запрещён: направление занято",
  96: "Проход по команде от ДУ",
  97: "Проход по команде оператора",
  98: "Проход по команде ИК-пульта",
  99: "Несанкционированный проход",
};

const ID_TYPE_TEXT = {
  card: "карта",
  finger: "палец",
  finger_anxiety: "тревожный палец",
  vehicle_number: "номер ТС",
  face: "лицо",
};

const USER_TYPE_TEXT = {
  visitor: "посетитель",
  employee: "сотрудник",
  vehicle: "ТС",
};

const $ = (id) => document.getElementById(id);
const esc = (value) =>
  String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[ch]));

function fmtTime(ts, tzOffset, withSeconds = true) {
  if (ts == null) return "—";
  const local = new Date((Number(ts) - Number(tzOffset || 0)) * 1000);
  return local.toLocaleString("ru-RU", {
    timeZone: "UTC",
    day: "2-digit", month: "2-digit", year: "numeric",
    hour: "2-digit", minute: "2-digit",
    ...(withSeconds ? { second: "2-digit" } : {}),
  });
}

function fmtTimeShort(ts, tzOffset) {
  if (ts == null) return "—";
  const local = new Date((Number(ts) - Number(tzOffset || 0)) * 1000);
  return local.toLocaleTimeString("ru-RU", { timeZone: "UTC", hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function tzLabel(tzOffset) {
  const hours = -(Number(tzOffset || 0) / 3600);
  const sign = hours > 0 ? "+" : "";
  return `GMT${sign}${Number.isInteger(hours) ? hours : hours.toFixed(1)}`;
}

function eventLabel(row) {
  let text = EVENT_TEXT[Number(row.code)];
  if (!text) text = `Событие ${row.code}`;
  if (row.number != null) text += ` · ИУ ${Number(row.number) + 1}`;
  if (row.verification) text += ` · ${row.verification}`;
  if (row.reason) text += ` · ${row.reason}`;
  return text;
}

function dirLabel(dir) {
  if (dir === 0) return '<span class="badge badge-in">Вход</span>';
  if (dir === 1) return '<span class="badge badge-out">Выход</span>';
  return '<span class="badge badge-muted">—</span>';
}

function eventBadge(row) {
  const code = Number(row.code);
  if (PASS_CODES.has(code)) return "badge-pass";
  if (code < 100) return "badge-deny";
  return "badge-info";
}

function userLabel(row) {
  const parts = [];
  if (row.user_type) parts.push(USER_TYPE_TEXT[row.user_type] || row.user_type);
  if (row.id_type) parts.push(ID_TYPE_TEXT[row.id_type] || row.id_type);
  return parts.length ? esc(parts.join(" · ")) : '<span class="muted">—</span>';
}

function codeCell(row) {
  const code = row.card || row.uid;
  if (!code) return '<span class="muted">—</span>';
  let html = `<span class="code" title="Код идентификатора">${esc(code)}</span>`;
  if (row.card && row.uid && row.uid !== row.card) {
    html += `<span class="code-sub">${esc(row.uid)}</span>`;
  }
  return html;
}

function deviceAlias(id) {
  const device = state.devices.find((item) => String(item.id) === String(id));
  return device ? device.name : "—";
}

function devicePill(online) {
  return online
    ? '<span class="pill pill-ok"><i class="dot"></i>На связи</span>'
    : '<span class="pill pill-bad"><i class="dot"></i>Нет связи</span>';
}

async function fetchJson(url, options) {
  const response = await fetch(url, { headers: { Accept: "application/json" }, ...options });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const payload = await response.json();
      if (payload && payload.detail) detail = typeof payload.detail === "string" ? payload.detail : detail;
    } catch { /* ignore */ }
    throw new Error(detail);
  }
  return response.json();
}

function buildQuery(params) {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== "" && value !== null && value !== undefined) search.set(key, value);
  });
  return search.toString();
}

/* ------------------------------------------------------------------ devices */
async function loadDevices() {
  try {
    const data = await fetchJson("/api/devices");
    state.devices = data.items || [];
    renderDeviceSelect();
    renderDevices();
    updateHeader();
  } catch (error) {
    console.error("devices:", error);
  }
}

function renderDeviceSelect() {
  const select = $("deviceSelect");
  const current = state.deviceId;
  const options = ['<option value="">Все проходные</option>'];
  for (const device of state.devices) {
    const suffix = device.connected ? "" : " · нет связи";
    options.push(`<option value="${device.id}">${esc(device.name)}${suffix}</option>`);
  }
  select.innerHTML = options.join("");
  const exists = state.devices.some((device) => String(device.id) === current);
  state.deviceId = exists ? current : "";
  select.value = state.deviceId;
}

function updateHeader() {
  const total = state.devices.length;
  const online = state.devices.filter((device) => device.connected).length;
  const pill = $("connPill");
  if (total === 0) {
    pill.className = "pill pill-wait";
    $("connText").textContent = "Нет проходных";
    $("deviceLine").textContent = "Добавьте проходную, чтобы начать сбор событий";
  } else {
    pill.className = `pill ${online === total ? "pill-ok" : online > 0 ? "pill-warn" : "pill-bad"}`;
    $("connText").textContent = `${online} из ${total} на связи`;
    if (total === 1) {
      const device = state.devices[0];
      $("deviceLine").textContent = `${device.name} · ${device.host}:${device.port}`;
    } else {
      $("deviceLine").textContent = `${total} проходных · ${online} на связи`;
    }
  }
  const syncTimes = state.devices.map((device) => device.last_sync_at || 0);
  const lastSync = Math.max(0, ...syncTimes);
  $("syncInfo").textContent = lastSync
    ? `Последняя синхронизация ${new Date(lastSync * 1000).toLocaleTimeString("ru-RU")}`
    : "Ожидание синхронизации…";
}

function renderDevices() {
  const grid = $("devicesGrid");
  $("devicesEmpty").hidden = state.devices.length > 0;
  grid.innerHTML = state.devices.map((device) => {
    const model = (device.device || {}).Product || "PERCo";
    const version = (device.device || {})["Version of S30"] || "—";
    const lastEvent = device.last_event_at
      ? fmtTime(device.last_event_at, device.timezone_offset || 0)
      : "—";
    return `
      <article class="device-card ${device.connected ? "is-online" : ""}">
        <header class="device-card-head">
          <div class="device-title">
            <h3>${esc(device.name)}</h3>
            <span class="device-addr">${esc(device.host)}:${device.port}</span>
          </div>
          ${devicePill(device.connected)}
        </header>
        <dl class="device-meta">
          <div><dt>Модель</dt><dd>${esc(model)}</dd></div>
          <div><dt>ПО</dt><dd>${esc(version)}</dd></div>
          <div><dt>Событий</dt><dd>${device.events ?? 0}</dd></div>
          <div><dt>Проходов</dt><dd>${device.passes ?? 0}</dd></div>
          <div class="wide"><dt>Последнее событие</dt><dd>${lastEvent}</dd></div>
        </dl>
        ${device.last_error ? `<p class="device-error" title="${esc(device.last_error)}">${esc(device.last_error)}</p>` : ""}
        <div class="device-actions">
          <button class="btn btn-ghost" type="button" data-edit="${device.id}">Изменить</button>
          <button class="btn btn-danger-ghost" type="button" data-delete="${device.id}">Удалить</button>
        </div>
      </article>`;
  }).join("");
}

/* ------------------------------------------------------------------ events */
async function loadEvents() {
  const { limit, offset } = state.events;
  const query = buildQuery({
    limit, offset,
    device_id: state.deviceId,
    q: state.q,
    direction: state.direction,
    passes_only: state.passesOnly,
  });
  try {
    const data = await fetchJson(`/api/events?${query}`);
    state.events.items = data.items;
    state.events.total = data.total;
    renderEvents();
  } catch (error) {
    console.error("events:", error);
  }
}

function renderEvents() {
  const { items, total, limit, offset } = state.events;
  $("eventsBody").innerHTML = items.map((row) => `
    <tr>
      <td class="cell-time">
        <div>${fmtTime(row.date, row.timezone_offset)}</div>
        <div class="tz">${tzLabel(row.timezone_offset)}</div>
      </td>
      <td><span class="device-badge">${esc(row.device_name || deviceAlias(row.device_id))}</span></td>
      <td><span class="event-label ${eventBadge(row)}">${esc(eventLabel(row))}</span></td>
      <td>${codeCell(row)}</td>
      <td>${dirLabel(row.dir)}</td>
      <td>${userLabel(row)}</td>
    </tr>`).join("");

  $("eventsEmpty").hidden = items.length > 0;
  const from = items.length ? offset + 1 : 0;
  const to = offset + items.length;
  $("eventsInfo").textContent = `Показано ${from}–${to} из ${total}`;
  $("eventsPrev").disabled = offset <= 0;
  $("eventsNext").disabled = offset + limit >= total;
}

/* ------------------------------------------------------------------- codes */
async function loadCodes() {
  const { limit, offset } = state.codes;
  const query = buildQuery({
    limit, offset,
    device_id: state.deviceId,
    q: state.q,
    passes_only: state.passesOnly,
  });
  try {
    const data = await fetchJson(`/api/codes?${query}`);
    state.codes.items = data.items;
    state.codes.total = data.total;
    renderCodes();
  } catch (error) {
    console.error("codes:", error);
  }
}

function renderCodes() {
  const { items, total, limit, offset } = state.codes;
  $("codesBody").innerHTML = items.map((row) => `
    <tr>
      <td><span class="device-badge">${esc(row.device_name || deviceAlias(row.device_id))}</span></td>
      <td>
        <span class="code">${esc(row.code || "—")}</span>
        ${row.uid && row.card && row.uid !== row.card ? `<span class="code-sub">${esc(row.uid)}</span>` : ""}
      </td>
      <td><strong>${row.passes}</strong></td>
      <td class="in-num">${row.entries}</td>
      <td class="out-num">${row.exits}</td>
      <td class="cell-time">${fmtTime(row.first_seen, row.timezone_offset)}</td>
      <td class="cell-time">${fmtTime(row.last_seen, row.timezone_offset)}</td>
    </tr>`).join("");
  $("codesEmpty").hidden = items.length > 0;
  const from = items.length ? offset + 1 : 0;
  const to = offset + items.length;
  $("codesInfo").textContent = `Показано ${from}–${to} из ${total}`;
  $("codesPrev").disabled = offset <= 0;
  $("codesNext").disabled = offset + limit >= total;
}

/* ------------------------------------------------------------------- stats */
async function loadStats() {
  try {
    const query = buildQuery({ period: "today", device_id: state.deviceId });
    const data = await fetchJson(`/api/stats?${query}`);
    $("statPasses").textContent = data.passes;
    $("statIn").textContent = data.entries;
    $("statOut").textContent = data.exits;
    $("statCodes").textContent = data.unique_codes;
    $("statTotal").textContent = data.events_all_time;
    $("statLast").textContent = data.last_event_at
      ? `последнее событие ${fmtTimeShort(data.last_event_at, data.timezone_offset)}`
      : "в базе сервиса";
  } catch (error) {
    console.error("stats:", error);
  }
}

/* ------------------------------------------------------------------- modal */
let editingDeviceId = null;

function openDeviceDialog(device = null) {
  editingDeviceId = device ? device.id : null;
  $("deviceDialogTitle").textContent = device ? "Изменить проходную" : "Новая проходная";
  $("deviceSubmit").textContent = device ? "Сохранить" : "Добавить";
  $("deviceName").value = device ? device.name : "";
  $("deviceHost").value = device ? device.host : "";
  $("devicePort").value = device ? device.port : 80;
  $("devicePassword").value = "";
  $("devicePassword").placeholder = device ? "оставьте пустым, чтобы не менять" : "если задан";
  $("deviceError").hidden = true;
  $("deviceDialog").showModal();
  setTimeout(() => $("deviceName").focus(), 50);
}

function closeDeviceDialog() {
  $("deviceDialog").close();
  editingDeviceId = null;
}

async function submitDevice(event) {
  event.preventDefault();
  const wasEditing = editingDeviceId !== null;
  const name = $("deviceName").value.trim();
  const host = $("deviceHost").value.trim();
  const port = Number($("devicePort").value) || 80;
  const password = $("devicePassword").value;
  const errorBox = $("deviceError");
  errorBox.hidden = true;

  const body = { name, host, port };
  if (!editingDeviceId || password) body.password = password;

  try {
    let device;
    if (editingDeviceId) {
      device = await fetchJson(`/api/devices/${editingDeviceId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
    } else {
      device = await fetchJson("/api/devices", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
    }
    closeDeviceDialog();
    await loadDevices();
    if (!wasEditing && device && device.id) {
      state.deviceId = String(device.id);
      renderDeviceSelect();
    }
    await refreshData();
    await loadDevices();
    showToast(wasEditing ? "Проходная обновлена" : "Проходная добавлена", name, "pass");
  } catch (error) {
    errorBox.textContent = error.message;
    errorBox.hidden = false;
  }
}

async function deleteDevice(device) {
  if (!confirm(`Удалить проходную «${device.name}» вместе с её событиями?`)) return;
  try {
    await fetchJson(`/api/devices/${device.id}`, { method: "DELETE" });
    if (String(device.id) === state.deviceId) state.deviceId = "";
    await loadDevices();
    await refreshData();
    showToast("Проходная удалена", device.name, "deny");
  } catch (error) {
    showToast("Не удалось удалить", error.message, "deny");
  }
}

/* ------------------------------------------------------------------- toast */
function showToast(title, subtitle, kind = "pass") {
  const container = $("toasts");
  const node = document.createElement("div");
  node.className = `toast toast-${kind}`;
  node.innerHTML = `
    <div class="toast-icon">${kind === "pass" ? "✓" : "•"}</div>
    <div class="toast-body"><strong>${esc(title)}</strong><span>${esc(subtitle)}</span></div>`;
  container.prepend(node);
  requestAnimationFrame(() => node.classList.add("visible"));
  setTimeout(() => {
    node.classList.remove("visible");
    setTimeout(() => node.remove(), 250);
  }, 5200);
}

/* -------------------------------------------------------------------- live */
let refreshScheduled = null;
function scheduleRefresh() {
  if (refreshScheduled) return;
  refreshScheduled = setTimeout(async () => {
    refreshScheduled = null;
    await loadStats();
    if (state.view === "events") await loadEvents();
    else if (state.view === "codes") await loadCodes();
    else await loadDevices();
  }, 400);
}

function connectStream() {
  const source = new EventSource("/api/stream");
  source.addEventListener("events", (message) => {
    let payload;
    try {
      payload = JSON.parse(message.data);
    } catch {
      return;
    }
    const events = payload.events || [];
    if (!events.length) return;
    if (events.some((event) => !state.devices.some((device) => String(device.id) === String(event.device_id)))) {
      loadDevices();
    }
    const matches = !state.deviceId || events.some((event) => String(event.device_id) === state.deviceId);
    if (!matches) return;

    const passes = events.filter((event) => PASS_CODES.has(Number(event.code)));
    if (passes.length) {
      const last = passes[passes.length - 1];
      const code = last.card || last.uid || "без кода";
      const dir = last.dir === 0 ? "вход" : last.dir === 1 ? "выход" : "проход";
      const name = (payload.device && payload.device.name) || last.device_name || "";
      showToast(`Проход: ${code}`, [name, dir].filter(Boolean).join(" · "), "pass");
    }
    scheduleRefresh();
  });
}

/* ------------------------------------------------------------------- wiring */
async function refreshData() {
  await Promise.all([
    loadStats(),
    state.view === "events" ? loadEvents() : state.view === "codes" ? loadCodes() : loadDevices(),
  ]);
}

function bindTabs() {
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.addEventListener("click", async () => {
      state.view = tab.dataset.view;
      document.querySelectorAll(".tab").forEach((item) => item.classList.toggle("active", item === tab));
      $("eventsView").hidden = state.view !== "events";
      $("codesView").hidden = state.view !== "codes";
      $("devicesView").hidden = state.view !== "devices";
      if (state.view === "events") await loadEvents();
      else if (state.view === "codes") await loadCodes();
      else await loadDevices();
    });
  });
}

function bindFilters() {
  let searchTimer = null;
  $("deviceSelect").addEventListener("change", async (event) => {
    state.deviceId = event.target.value;
    state.events.offset = 0;
    state.codes.offset = 0;
    await refreshData();
  });
  $("searchInput").addEventListener("input", (event) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(async () => {
      state.q = event.target.value.trim();
      state.events.offset = 0;
      state.codes.offset = 0;
      await refreshData();
    }, 300);
  });
  $("directionSelect").addEventListener("change", async (event) => {
    state.direction = event.target.value;
    state.events.offset = 0;
    await loadEvents();
  });
  $("passesOnly").addEventListener("change", async (event) => {
    state.passesOnly = event.target.checked;
    state.events.offset = 0;
    state.codes.offset = 0;
    await Promise.all([loadStats(), state.view === "codes" ? loadCodes() : loadEvents()]);
  });
  $("refreshBtn").addEventListener("click", async () => {
    await fetchJson("/api/sync", { method: "POST" });
    await Promise.all([loadDevices(), refreshData()]);
  });
  $("addDeviceBtn").addEventListener("click", () => openDeviceDialog());
  $("deviceCancel").addEventListener("click", closeDeviceDialog);
  $("deviceForm").addEventListener("submit", submitDevice);
  $("devicesGrid").addEventListener("click", (event) => {
    const editButton = event.target.closest("[data-edit]");
    const deleteButton = event.target.closest("[data-delete]");
    if (editButton) {
      const device = state.devices.find((item) => String(item.id) === editButton.dataset.edit);
      if (device) openDeviceDialog(device);
    } else if (deleteButton) {
      const device = state.devices.find((item) => String(item.id) === deleteButton.dataset.delete);
      if (device) deleteDevice(device);
    }
  });
}

function bindPager() {
  $("eventsPrev").addEventListener("click", () => {
    state.events.offset = Math.max(0, state.events.offset - state.events.limit);
    loadEvents();
  });
  $("eventsNext").addEventListener("click", () => {
    state.events.offset += state.events.limit;
    loadEvents();
  });
  $("codesPrev").addEventListener("click", () => {
    state.codes.offset = Math.max(0, state.codes.offset - state.codes.limit);
    loadCodes();
  });
  $("codesNext").addEventListener("click", () => {
    state.codes.offset += state.codes.limit;
    loadCodes();
  });
}

async function init() {
  bindTabs();
  bindFilters();
  bindPager();
  await loadDevices();
  await refreshData();
  connectStream();
  setInterval(loadDevices, 10000);
  setInterval(loadStats, 30000);
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      loadDevices();
      refreshData();
    }
  });
}

init();
