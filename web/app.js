/* 入仓工作台前端: 原生 JS, 无构建步骤, 无外部依赖。 */
"use strict";

const $ = (sel) => document.querySelector(sel);
const api = async (path, opts) => {
  const res = await fetch(path, opts);
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
};
const post = (path, body) =>
  api(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? "{}" : JSON.stringify(body),
  });
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const badge = (state) =>
  `<span class="badge b-${esc(state)}">${esc(STATE_LABELS[state] || state)}</span>`;
const table = (columns, rows, rowClick) => {
  if (!rows.length) return `<p class="section-note">暂无数据</p>`;
  const head = columns.map((c) => `<th>${esc(c.title)}</th>`).join("");
  const body = rows.map((r, i) => {
    const click = rowClick ? ` data-ri="${i}"` : "";
    return `<tr class="${rowClick ? "clickable" : ""}"${click}>` +
      columns.map((c) => `<td>${c.cell(r)}</td>`).join("") + "</tr>";
  }).join("");
  return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
};

let STATE_LABELS = {};
let LABELS = {};
let currentData = {};

// --------------------------------------------------------------------- toast
function toast(msg, isError = false) {
  const el = $("#toast");
  el.textContent = msg;
  el.className = "toast" + (isError ? " error" : "");
  setTimeout(() => el.classList.add("hidden"), 4200);
}

// -------------------------------------------------------------------- lineage
async function openLineage(entityType, entityId, title) {
  $("#drawer-title").textContent = `血缘 · ${title}`;
  $("#drawer-body").innerHTML = `<p class="hint">加载中…</p>`;
  $("#drawer").classList.remove("hidden");
  try {
    const rows = await api(
      `/api/lineage?entity_type=${encodeURIComponent(entityType)}` +
      `&entity_id=${encodeURIComponent(entityId)}`);
    if (!rows.length) {
      $("#drawer-body").innerHTML = `<p class="hint">暂无血缘事件</p>`;
      return;
    }
    $("#drawer-body").innerHTML = `<ul class="timeline">` + rows.map((r) => `
      <li>
        <div class="ev">${esc(r.event_label)}
          ${r.batch_id ? `<span class="batch-chip">${esc(r.batch_id)}</span>` : ""}
        </div>
        <div class="meta">${esc(r.at)}</div>
        <div class="detail">${esc(r.detail)}</div>
      </li>`).join("") + `</ul>`;
  } catch (e) {
    $("#drawer-body").innerHTML = `<p class="reason">${esc(e.message)}</p>`;
  }
}
$("#drawer-close").addEventListener("click", () =>
  $("#drawer").classList.add("hidden"));

// --------------------------------------------------------------------- render
async function refresh() {
  const [ov, batches, waiting, orders, lines, payments, reversals, published] =
    await Promise.all([
      api("/api/overview"), api("/api/batches"), api("/api/waiting"),
      api("/api/orders"), api("/api/lines"), api("/api/payments"),
      api("/api/reversals"), api("/api/published"),
    ]);
  currentData = { batches, waiting, orders, lines, payments, reversals, published };
  renderStats(ov);
  renderBatches(batches);
  renderWaiting(waiting);
  renderOrders(orders);
  renderDetails("lines", lines, "line_id");
  renderDetails("payments", payments, "payment_id");
  renderReversals(reversals);
  renderPublished(published);
}

function renderStats(ov) {
  const cards = [
    ["批次 (含重复)", ov.batches, ""],
    ["重复批次", ov.duplicate_batches, ov.duplicate_batches ? "warn" : ""],
    ["有效订单", ov.orders_active, "good"],
    ["已撤销订单", ov.orders_cancelled, ""],
    ["等待/异常条目", ov.waiting_active, ov.exceptions ? "bad" : "warn"],
    ["已关联订单行", ov.lines.LINKED, "good"],
    ["异常订单行", ov.lines.EXCEPTION, "bad"],
    ["已关联付款", ov.payments.LINKED, "good"],
    ["待发布撤销", ov.reversals_pending, "warn"],
    ["已发布撤销", ov.reversals_published, ""],
    ["发布次数", ov.publish_runs, ""],
    ["已发布行/付款", `${ov.published_lines} / ${ov.published_payments}`, "good"],
  ];
  $("#stats").innerHTML = cards.map(([lbl, num, cls]) =>
    `<div class="stat ${cls}"><div class="num">${num}</div><div class="lbl">${lbl}</div></div>`
  ).join("");
}

function renderBatches(rows) {
  $("#tab-batches").innerHTML = table(
    [
      { title: "批次号", cell: (r) => `<span class="mono">${esc(r.batch_id)}</span>` },
      { title: "状态", cell: (r) => badge(r.status) },
      { title: "条目数", cell: (r) => r.item_count },
      { title: "接收时间 (UTC)", cell: (r) => esc(r.received_at) },
      { title: "血缘", cell: (r) =>
          `<button class="small-btn" data-lineage='batch|${esc(r.batch_id)}|批次 ${esc(r.batch_id)}'>查看经过</button>` },
    ], rows);
}

function renderWaiting(rows) {
  const kindLabel = { DETAIL: "明细等待主记录", CANCEL_MARKER: "撤销先到等待订单" };
  const html = table(
    [
      { title: "类型", cell: (r) => kindLabel[r.wait_kind] || r.wait_kind },
      { title: "实体", cell: (r) =>
          `<span class="mono">${esc(r.entity_type || "—")} ${esc(r.entity_id)}</span>` },
      { title: "目标订单", cell: (r) => `<span class="mono">${esc(r.target_order)}</span>` },
      { title: "状态", cell: (r) => badge(r.state) },
      { title: "等待原因", cell: (r) =>
          `<div>${esc(r.reason_label)}</div><div class="reason mono">${esc(r.reason_code)}</div>` },
      { title: "来源批次", cell: (r) =>
          `<span class="mono">${esc(r.batch_id)}</span>` },
      { title: "到达时间", cell: (r) => esc(r.arrived_at) },
      { title: "操作 / 血缘", cell: (r) =>
          (r.wait_kind === "DETAIL" && ["WAITING", "EXCEPTION"].includes(r.state)
            ? `<button class="small-btn" data-master='${esc(r.target_order)}'>补送主记录</button> `
            : "") +
          `<button class="small-btn" data-lineage='${esc(r.entity_type || "order")}|${esc(r.entity_id)}|${esc(r.entity_id)} 血缘'>查看原因</button>` },
    ], rows);
  $("#tab-waiting").innerHTML =
    `<p class="section-note">等待原因可点开血缘查看; 超时进入 EXCEPTION 的数据不会被删除, 主记录补达后自动恢复。</p>` + html;
}

function renderOrders(rows) {
  $("#tab-orders").innerHTML = table(
    [
      { title: "订单号", cell: (r) => `<span class="mono">${esc(r.order_id)}</span>` },
      { title: "状态", cell: (r) => badge(r.state) },
      { title: "当前版本", cell: (r) => "v" + r.current_version },
      { title: "创建批次", cell: (r) => `<span class="mono">${esc(r.create_batch_id)}</span>` },
      { title: "撤销批次", cell: (r) =>
          r.cancel_batch_id ? `<span class="mono">${esc(r.cancel_batch_id)}</span>` : "—" },
      { title: "血缘", cell: (r) =>
          `<button class="small-btn" data-lineage='order|${esc(r.order_id)}|订单 ${esc(r.order_id)}'>查看经过</button>` },
    ], rows);
}

function renderDetails(tab, rows, idCol) {
  const etype = tab === "lines" ? "order_line" : "payment";
  const columns = tab === "lines"
    ? [
        { title: "订单行", cell: (r) => `<span class="mono">${esc(r[idCol])}</span>` },
        { title: "订单", cell: (r) => `<span class="mono">${esc(r.order_id || "—")}</span>` },
        { title: "SKU", cell: (r) => esc(r.sku) },
        { title: "数量", cell: (r) => r.qty },
        { title: "金额", cell: (r) => r.amount },
      ]
    : [
        { title: "付款", cell: (r) => `<span class="mono">${esc(r[idCol])}</span>` },
        { title: "订单", cell: (r) => `<span class="mono">${esc(r.order_id || "—")}</span>` },
        { title: "方式", cell: (r) => esc(r.method) },
        { title: "金额", cell: (r) => r.amount },
      ];
  columns.push(
    { title: "版本", cell: (r) => "v" + r.version },
    { title: "状态", cell: (r) => badge(r.state) },
    { title: "来源批次", cell: (r) => `<span class="mono">${esc(r.batch_id)}</span>` },
    { title: "血缘", cell: (r) =>
        `<button class="small-btn" data-lineage='${etype}|${esc(r[idCol])}|${esc(r[idCol])} 血缘'>查看经过</button>` },
  );
  $(`#tab-${tab}`).innerHTML = table(columns, rows);
}

function renderReversals(rows) {
  $("#tab-reversals").innerHTML = table(
    [
      { title: "撤销结果", cell: (r) => `<span class="mono">${esc(r.reversal_id)}</span>` },
      { title: "订单", cell: (r) => `<span class="mono">${esc(r.order_id)}</span>` },
      { title: "回滚对象", cell: (r) =>
          `<div>${esc(r.source_entity)} <span class="mono">${esc(r.source_id)}</span></div>
           <div class="hint">来源发布: <span class="mono">${esc(r.source_publish_id)}</span></div>` },
      { title: "状态", cell: (r) => badge(r.state) },
      { title: "撤销批次", cell: (r) => `<span class="mono">${esc(r.batch_id)}</span>` },
      { title: "发布号", cell: (r) =>
          r.publish_id ? `<span class="mono">${esc(r.publish_id)}</span>` : "—" },
      { title: "血缘", cell: (r) =>
          `<button class="small-btn" data-lineage='reversal|${esc(r.reversal_id)}|撤销 ${esc(r.reversal_id)}'>查看经过</button>` },
    ], rows);
}

function renderPublished(data) {
  const journal = table(
    [
      { title: "发布号", cell: (r) => `<span class="mono">${esc(r.publish_id)}</span>` },
      { title: "时间", cell: (r) => esc(r.published_at) },
      { title: "订单", cell: (r) => r.order_count },
      { title: "订单行", cell: (r) => r.line_count },
      { title: "付款", cell: (r) => r.payment_count },
      { title: "撤销", cell: (r) => r.reversal_count },
    ], data.journal);
  const snapCols = (idName) => [
    { title: idName, cell: (r) => `<span class="mono">${esc(r[idName])}</span>` },
    { title: "订单", cell: (r) => `<span class="mono">${esc(r.order_id)}</span>` },
    { title: "发布号", cell: (r) => `<span class="mono">${esc(r.publish_id)}</span>` },
    { title: "版本", cell: (r) => "v" + r.version },
  ];
  $("#tab-published").innerHTML = `
    <h3 class="group-title">发布流水</h3>${journal}
    <h3 class="group-title">已发布订单快照</h3>${table(snapCols("order_id"), data.orders)}
    <h3 class="group-title">已发布订单行快照</h3>${table(snapCols("line_id"), data.lines)}
    <h3 class="group-title">已发布付款快照</h3>${table(snapCols("payment_id"), data.payments)}
    <h3 class="group-title">已发布撤销结果 (有来源)</h3>
    ${table([
      { title: "撤销", cell: (r) => `<span class="mono">${esc(r.reversal_id)}</span>` },
      { title: "回滚", cell: (r) => `<span class="mono">${esc(r.source_id)}</span>` },
      { title: "来源发布", cell: (r) => `<span class="mono">${esc(r.source_publish_id)}</span>` },
      { title: "本次发布", cell: (r) => `<span class="mono">${esc(r.publish_id)}</span>` },
    ], data.reversals)}`;
}

// -------------------------------------------------------------------- events
document.addEventListener("click", async (ev) => {
  const lin = ev.target.dataset && ev.target.dataset.lineage;
  if (lin) {
    const [t, id, title] = lin.split("|");
    return openLineage(t, id, title);
  }
  const master = ev.target.dataset && ev.target.dataset.master;
  if (master) {
    try {
      const r = await post("/api/synth/master", { order_id: master });
      toast(`已补送订单 ${master}: 关联 ${r.linked_from_waiting} 条等待数据`);
      await refresh();
    } catch (e) { toast(e.message, true); }
  }
});

document.querySelectorAll(".tab").forEach((btn) =>
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
    btn.classList.add("active");
    $(`#tab-${btn.dataset.tab}`).classList.add("active");
  }));

$("#btn-shuffle").addEventListener("click", async () => {
  try {
    const r = await post("/api/synth/send-shuffled");
    const applied = r.reports.reduce((n, x) => n + (x.applied || 0), 0);
    const dup = r.reports.reduce((n, x) => n + (x.duplicate_versions || []).length, 0);
    toast(`乱序发送 ${r.sent_order.length} 个批次: 新应用 ${applied} 条, 重复版本 ${dup} 条`);
    await refresh();
  } catch (e) { toast(e.message, true); }
});

$("#btn-sweep").addEventListener("click", async () => {
  try {
    const ff = parseInt($("#ff-seconds").value || "0", 10);
    const r = await post("/api/sweep", { fast_forward_seconds: ff });
    toast(ff ? `快进 ${ff}s 后扫描: ${r.timed_out} 条进入异常处理(数据保留)`
             : `扫描完成: ${r.timed_out} 条超时转异常`);
    await refresh();
  } catch (e) { toast(e.message, true); }
});

$("#btn-publish").addEventListener("click", async () => {
  try {
    const r = await post("/api/publish");
    toast(r.published
      ? `发布 ${r.publish_id}: 订单 ${r.orders} / 行 ${r.lines} / 付款 ${r.payments} / 撤销 ${r.reversals}`
      : "没有待发布内容");
    await refresh();
  } catch (e) { toast(e.message, true); }
});

$("#btn-reset").addEventListener("click", async () => {
  if (!confirm("确认清空全部本地数据? (仅演示库)")) return;
  await post("/api/reset");
  toast("已重置");
  await refresh();
});

$("#btn-send-manual").addEventListener("click", async () => {
  const batchId = $("#manual-batch-id").value.trim();
  let items;
  try {
    items = JSON.parse($("#manual-items").value);
  } catch (e) {
    $("#manual-result").textContent = "JSON 解析失败: " + e.message;
    return;
  }
  try {
    const r = await post("/api/ingest", { batch_id: batchId, items });
    $("#manual-result").textContent = JSON.stringify(r, null, 2);
    toast(`批次 ${batchId} 已入仓`);
    await refresh();
  } catch (e) {
    $("#manual-result").textContent = "错误: " + e.message;
  }
});

// ---------------------------------------------------------------------- boot
(async function boot() {
  LABELS = await api("/api/labels");
  STATE_LABELS = LABELS.state;
  await refresh();
})();
