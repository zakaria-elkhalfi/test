require("dotenv").config();
const express = require("express");
const nodemailer = require("nodemailer");
const cors = require("cors");
const multer = require("multer");
const { parse } = require("csv-parse/sync");
const path = require("path");
const { Client, LocalAuth } = require("whatsapp-web.js");
const qrcode = require("qrcode");
const cron = require("node-cron");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const app = express();

const upload = multer({ storage: multer.memoryStorage() });

app.use(cors());
app.use(express.json());

app.use(express.static(path.join(__dirname, "../public")));

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "../public", "index.html"));
});
// ─── Auth Config ──────────────────────────────────────────────────────────────
const JWT_SECRET = process.env.JWT_SECRET || "mailflow-secret-2024";
const ADMIN_USER = process.env.ADMIN_USER || "admin";
const ADMIN_PASS_HASH = bcrypt.hashSync(process.env.ADMIN_PASS || "admin123", 10);

function authRequired(req, res, next) {
  const token = req.headers["authorization"]?.split(" ")[1];
  if (!token) return res.status(401).json({ success: false, error: "Unauthorized" });
  try { req.user = jwt.verify(token, JWT_SECRET); next(); }
  catch { res.status(401).json({ success: false, error: "Invalid token" }); }
}

// ─── Database (in-memory) ─────────────────────────────────────────────────────
let templates = [{ id: 1, name: "Default Template", subject: "", message: "", updatedAt: null }];
let emailLogs = [];
let waLogs = [];
let scheduledJobs = [];
let cronRegistry = {};
let stats = {
  totalEmailsSent: 0, totalEmailsFailed: 0,
  totalWASent: 0, totalWAFailed: 0,
  dailyStats: {}
};

function todayKey() { return new Date().toISOString().split("T")[0]; }
function bumpStat(field) {
  stats[field]++;
  const k = todayKey();
  if (!stats.dailyStats[k]) stats.dailyStats[k] = { emailSent:0, emailFailed:0, waSent:0, waFailed:0 };
  const map = { totalEmailsSent:"emailSent", totalEmailsFailed:"emailFailed", totalWASent:"waSent", totalWAFailed:"waFailed" };
  if (map[field]) stats.dailyStats[k][map[field]]++;
}

// ─── WhatsApp ─────────────────────────────────────────────────────────────────
let waClient = null, waStatus = "disconnected", waQR = null, waSseClients = [];

function broadcastWA(data) {
  waSseClients.forEach(r => r.write(`data: ${JSON.stringify(data)}\n\n`));
}

function initWhatsApp() {
  if (waClient) { waClient.destroy().catch(() => {}); waClient = null; }
  waStatus = "connecting"; waQR = null;
  broadcastWA({ type: "status", status: "connecting" });

  waClient = new Client({
    authStrategy: new LocalAuth({ dataPath: path.join(__dirname, "../.wwebjs_auth") }),
    webVersionCache: {
      type: "remote",
      remotePath: "https://raw.githubusercontent.com/wppconnect-team/wa-version/main/html/2.2412.54.html",
    },
    puppeteer: {
      headless: true,
      args: [
        "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
        "--disable-gpu", "--disable-software-rasterizer", "--disable-extensions",
        "--no-first-run", "--no-zygote", "--single-process",
        "--disable-background-networking", "--disable-default-apps",
        "--disable-sync", "--hide-scrollbars", "--mute-audio",
      ],
    },
  });

  waClient.on("qr", async (qr) => {
    waStatus = "qr";
    try { waQR = await qrcode.toDataURL(qr); broadcastWA({ type: "qr", qr: waQR }); } catch(e) {}
    console.log("📱 QR ready!");
  });
  waClient.on("loading_screen", (p, m) => console.log(`⏳ WA ${p}% — ${m}`));
  waClient.on("authenticated", () => {
    waStatus = "connecting"; waQR = null;
    broadcastWA({ type: "status", status: "connecting" });
  });
  waClient.on("auth_failure", (m) => {
    waStatus = "disconnected"; waQR = null; waClient = null;
    broadcastWA({ type: "status", status: "disconnected", error: m });
  });
  waClient.on("ready", () => {
    waStatus = "ready"; waQR = null;
    broadcastWA({ type: "status", status: "ready" });
    console.log("✅ WhatsApp ready!");
  });
  waClient.on("disconnected", (r) => {
    waStatus = "disconnected"; waQR = null; waClient = null;
    broadcastWA({ type: "status", status: "disconnected" });
    console.log("❌ WA disconnected:", r);
  });
  console.log("🚀 Initializing WhatsApp...");
  waClient.initialize().catch(err => {
    waStatus = "disconnected"; waClient = null;
    broadcastWA({ type: "status", status: "disconnected", error: err.message });
    console.error("WA init error:", err.message);
  });
}

// ─── Email SSE ────────────────────────────────────────────────────────────────
let sseClients = [];
function broadcastSSE(data) {
  sseClients.forEach(r => r.write(`data: ${JSON.stringify(data)}\n\n`));
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function isValidEmail(e) { return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(e).trim()); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function randDelay(a, b) { return Math.floor(Math.random()*(b-a+1))+a; }
function cleanNumber(n) {
  let s = String(n).replace(/[\s\-\(\)\+]/g, "");
  if (!s.startsWith("212") && s.startsWith("0")) s = "212" + s.slice(1);
  return s;
}
function createTransporter() {
  return nodemailer.createTransport({
    host: process.env.SMTP_HOST || "smtp.gmail.com",
    port: parseInt(process.env.SMTP_PORT) || 587,
    secure: process.env.SMTP_SECURE === "true",
    auth: { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS },
  });
}

// ─── Core send functions ───────────────────────────────────────────────────────
async function sendBulkEmails(emails, tpl, broadcast = true) {
  const transporter = createTransporter();
  const minD = parseInt(process.env.EMAIL_DELAY_MIN) || 2000;
  const maxD = parseInt(process.env.EMAIL_DELAY_MAX) || 5000;
  for (let i = 0; i < emails.length; i++) {
    const email = String(emails[i]).trim();
    if (!isValidEmail(email)) {
      const log = { email, status: "failed", error: "Invalid email", sentAt: new Date().toISOString() };
      emailLogs.unshift(log); bumpStat("totalEmailsFailed");
      if (broadcast) broadcastSSE({ type: "log", log, progress: i+1, total: emails.length });
      continue;
    }
    try {
      await transporter.sendMail({
        from: `"${process.env.FROM_NAME || "MailFlow"}" <${process.env.SMTP_USER}>`,
        to: email, subject: tpl.subject,
        text: tpl.message, html: tpl.message.replace(/\n/g, "<br>"),
      });
      const log = { email, status: "sent", error: null, sentAt: new Date().toISOString() };
      emailLogs.unshift(log); bumpStat("totalEmailsSent");
      if (broadcast) broadcastSSE({ type: "log", log, progress: i+1, total: emails.length });
    } catch (err) {
      const log = { email, status: "failed", error: err.message, sentAt: new Date().toISOString() };
      emailLogs.unshift(log); bumpStat("totalEmailsFailed");
      if (broadcast) broadcastSSE({ type: "log", log, progress: i+1, total: emails.length });
    }
    if (i < emails.length - 1) await sleep(randDelay(minD, maxD));
  }
}

async function sendBulkWA(numbers, message, broadcast = true) {
  if (waStatus !== "ready") return;
  const minD = parseInt(process.env.WA_DELAY_MIN) || 5000;
  const maxD = parseInt(process.env.WA_DELAY_MAX) || 15000;
  for (let i = 0; i < numbers.length; i++) {
    const num = cleanNumber(numbers[i]);
    if (num.length < 8) continue;
    try {
      await waClient.sendMessage(num + "@c.us", message);
      const log = { number: num, status: "sent", error: null, sentAt: new Date().toISOString() };
      waLogs.unshift(log); bumpStat("totalWASent");
      if (broadcast) broadcastWA({ type: "wa_log", log, progress: i+1, total: numbers.length });
    } catch (err) {
      const log = { number: num, status: "failed", error: err.message, sentAt: new Date().toISOString() };
      waLogs.unshift(log); bumpStat("totalWAFailed");
      if (broadcast) broadcastWA({ type: "wa_log", log, progress: i+1, total: numbers.length });
    }
    if (i < numbers.length - 1) await sleep(randDelay(minD, maxD));
  }
}

// ─── Scheduler ────────────────────────────────────────────────────────────────
function registerCronJob(job) {
  if (cronRegistry[job.id]) { cronRegistry[job.id].stop(); delete cronRegistry[job.id]; }
  if (!job.enabled || !cron.validate(job.cron)) return;
  cronRegistry[job.id] = cron.schedule(job.cron, async () => {
    job.lastRun = new Date().toISOString();
    console.log(`⏰ Running scheduled job: ${job.name}`);
    const tpl = templates.find(t => t.id === job.templateId) || templates[0];
    if (!tpl) return;
    const recs = job.recipients || [];
    if (job.type === "email") {
      broadcastSSE({ type: "start", total: recs.length });
      await sendBulkEmails(recs, tpl);
      broadcastSSE({ type: "done", total: recs.length });
    } else if (job.type === "whatsapp") {
      broadcastWA({ type: "wa_start", total: recs.length });
      await sendBulkWA(recs, tpl.message);
      broadcastWA({ type: "wa_done", total: recs.length });
    }
  });
}

// ═══════════════════════ API ROUTES ═══════════════════════════════════════════

// ─── Auth ─────────────────────────────────────────────────────────────────────
app.post("/api/login", (req, res) => {
  const { username, password } = req.body;
  if (username !== ADMIN_USER || !bcrypt.compareSync(password, ADMIN_PASS_HASH))
    return res.status(401).json({ success: false, error: "Invalid credentials" });
  const token = jwt.sign({ username }, JWT_SECRET, { expiresIn: "24h" });
  res.json({ success: true, token });
});

app.get("/api/me", authRequired, (req, res) => {
  res.json({ success: true, user: req.user.username });
});

// ─── Stats ────────────────────────────────────────────────────────────────────
app.get("/api/stats", authRequired, (req, res) => {
  const last7 = [];
  for (let i = 6; i >= 0; i--) {
    const d = new Date(); d.setDate(d.getDate() - i);
    const k = d.toISOString().split("T")[0];
    last7.push({ date: k, ...(stats.dailyStats[k] || { emailSent:0, emailFailed:0, waSent:0, waFailed:0 }) });
  }
  res.json({ success: true, overview: stats, last7 });
});

// ─── Templates ────────────────────────────────────────────────────────────────
app.get("/api/templates", authRequired, (req, res) => res.json({ success: true, templates }));

app.post("/api/templates", authRequired, (req, res) => {
  const { name, subject, message } = req.body;
  if (!name || !subject || !message) return res.status(400).json({ success: false, error: "name, subject, message required" });
  const tpl = { id: Date.now(), name, subject, message, updatedAt: new Date().toISOString() };
  templates.push(tpl);
  res.json({ success: true, template: tpl });
});

app.put("/api/templates/:id", authRequired, (req, res) => {
  const id = parseInt(req.params.id);
  const idx = templates.findIndex(t => t.id === id);
  if (idx === -1) return res.status(404).json({ success: false, error: "Not found" });
  templates[idx] = { ...templates[idx], ...req.body, id, updatedAt: new Date().toISOString() };
  res.json({ success: true, template: templates[idx] });
});

app.delete("/api/templates/:id", authRequired, (req, res) => {
  const id = parseInt(req.params.id);
  if (templates.length <= 1) return res.status(400).json({ success: false, error: "Keep at least one template" });
  templates = templates.filter(t => t.id !== id);
  res.json({ success: true });
});

// Legacy
app.get("/api/template", (req, res) => res.json({ success: true, template: templates[0] }));
app.post("/api/template", (req, res) => {
  const { subject, message } = req.body;
  if (!subject || !message) return res.status(400).json({ success: false, error: "Required" });
  templates[0] = { ...templates[0], subject, message, updatedAt: new Date().toISOString() };
  res.json({ success: true, template: templates[0] });
});

// ─── Email SSE ────────────────────────────────────────────────────────────────
app.get("/api/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();
  sseClients.push(res);
  req.on("close", () => { sseClients = sseClients.filter(c => c !== res); });
});

// ─── Email Logs ───────────────────────────────────────────────────────────────
app.get("/api/logs", authRequired, (req, res) => res.json({ success: true, logs: emailLogs }));
app.delete("/api/logs", authRequired, (req, res) => { emailLogs = []; res.json({ success: true }); });

// ─── CSV ──────────────────────────────────────────────────────────────────────
app.post("/api/parse-csv", upload.single("csv"), (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ success: false, error: "No file" });
    const rows = parse(req.file.buffer.toString("utf-8"), { skip_empty_lines: true, trim: true });
    const emails = [];
    rows.forEach(r => r.forEach(c => { if (isValidEmail(c)) emails.push(c.trim()); }));
    if (!emails.length) return res.status(400).json({ success: false, error: "No valid emails in CSV" });
    res.json({ success: true, emails: [...new Set(emails)] });
  } catch (err) { res.status(500).json({ success: false, error: err.message }); }
});

// ─── Send Email ───────────────────────────────────────────────────────────────
app.post("/api/send", async (req, res) => {
  const { emails, templateId } = req.body;
  if (!emails?.length) return res.status(400).json({ success: false, error: "No emails" });
  if (!process.env.SMTP_USER) return res.status(500).json({ success: false, error: "SMTP not configured in .env" });
  const tpl = templateId ? templates.find(t => t.id === templateId) : templates[0];
  if (!tpl?.subject) return res.status(400).json({ success: false, error: "Save a template first" });
  res.json({ success: true, message: `Sending to ${emails.length}...`, total: emails.length });
  broadcastSSE({ type: "start", total: emails.length });
  await sendBulkEmails(emails, tpl);
  broadcastSSE({ type: "done", total: emails.length });
});

// ─── SMTP Test ────────────────────────────────────────────────────────────────
app.get("/api/test-smtp", async (req, res) => {
  if (!process.env.SMTP_USER) return res.json({ success: false, error: "Not configured" });
  try { await createTransporter().verify(); res.json({ success: true }); }
  catch (err) { res.json({ success: false, error: err.message }); }
});

// ─── WhatsApp SSE ─────────────────────────────────────────────────────────────
app.get("/api/wa/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();
  waSseClients.push(res);
  res.write(`data: ${JSON.stringify({ type: "status", status: waStatus, qr: waQR })}\n\n`);
  req.on("close", () => { waSseClients = waSseClients.filter(c => c !== res); });
});

app.get("/api/wa/status", (req, res) => res.json({ status: waStatus, qr: waQR }));
app.post("/api/wa/connect", (req, res) => { initWhatsApp(); res.json({ success: true }); });
app.post("/api/wa/disconnect", async (req, res) => {
  if (waClient) { await waClient.destroy().catch(() => {}); waClient = null; }
  waStatus = "disconnected"; waQR = null;
  broadcastWA({ type: "status", status: "disconnected" });
  res.json({ success: true });
});

app.get("/api/wa/logs", authRequired, (req, res) => res.json({ success: true, logs: waLogs }));
app.delete("/api/wa/logs", authRequired, (req, res) => { waLogs = []; res.json({ success: true }); });

// ─── WhatsApp Groups ──────────────────────────────────────────────────────────
app.get("/api/wa/groups", async (req, res) => {
  if (waStatus !== "ready") return res.json({ success: false, error: "WhatsApp not connected" });
  try {
    const chats = await waClient.getChats();
    const groups = chats.filter(c => c.isGroup).map(g => ({
      id: g.id._serialized, name: g.name, participants: g.participants?.length || 0
    }));
    res.json({ success: true, groups });
  } catch (err) { res.json({ success: false, error: err.message }); }
});

app.post("/api/wa/send-group", async (req, res) => {
  const { groupIds, message } = req.body;
  if (!groupIds?.length) return res.status(400).json({ success: false, error: "No groups" });
  if (!message) return res.status(400).json({ success: false, error: "Message required" });
  if (waStatus !== "ready") return res.status(400).json({ success: false, error: "WhatsApp not connected" });
  res.json({ success: true, message: `Sending to ${groupIds.length} groups...` });
  const minD = parseInt(process.env.WA_DELAY_MIN) || 5000;
  const maxD = parseInt(process.env.WA_DELAY_MAX) || 15000;
  for (let i = 0; i < groupIds.length; i++) {
    try {
      await waClient.sendMessage(groupIds[i], message);
      const log = { number: `Group`, status: "sent", error: null, sentAt: new Date().toISOString() };
      waLogs.unshift(log); bumpStat("totalWASent");
      broadcastWA({ type: "wa_log", log, progress: i+1, total: groupIds.length });
    } catch (err) {
      const log = { number: `Group`, status: "failed", error: err.message, sentAt: new Date().toISOString() };
      waLogs.unshift(log); bumpStat("totalWAFailed");
      broadcastWA({ type: "wa_log", log, progress: i+1, total: groupIds.length });
    }
    if (i < groupIds.length - 1) await sleep(randDelay(minD, maxD));
  }
  broadcastWA({ type: "wa_done", total: groupIds.length });
});

// ─── WA Send contacts ─────────────────────────────────────────────────────────
app.post("/api/wa/send", async (req, res) => {
  const { numbers, message } = req.body;
  if (!numbers?.length) return res.status(400).json({ success: false, error: "No numbers" });
  if (!message) return res.status(400).json({ success: false, error: "Message required" });
  if (waStatus !== "ready") return res.status(400).json({ success: false, error: "WhatsApp not connected" });
  res.json({ success: true, message: `Sending to ${numbers.length} contacts...`, total: numbers.length });
  broadcastWA({ type: "wa_start", total: numbers.length });
  await sendBulkWA(numbers, message);
  broadcastWA({ type: "wa_done", total: numbers.length });
});

// ─── Scheduler routes ─────────────────────────────────────────────────────────
app.get("/api/scheduler", authRequired, (req, res) => res.json({ success: true, jobs: scheduledJobs }));

app.post("/api/scheduler", authRequired, (req, res) => {
  const { name, cron: cronExpr, type, templateId, recipients } = req.body;
  if (!name || !cronExpr || !type) return res.status(400).json({ success: false, error: "name, cron, type required" });
  if (!cron.validate(cronExpr)) return res.status(400).json({ success: false, error: "Invalid cron expression" });
  const job = { id: Date.now(), name, cron: cronExpr, type, templateId: templateId || templates[0].id, recipients: recipients || [], enabled: true, lastRun: null, createdAt: new Date().toISOString() };
  scheduledJobs.push(job);
  registerCronJob(job);
  res.json({ success: true, job });
});

app.put("/api/scheduler/:id", authRequired, (req, res) => {
  const id = parseInt(req.params.id);
  const idx = scheduledJobs.findIndex(j => j.id === id);
  if (idx === -1) return res.status(404).json({ success: false, error: "Not found" });
  scheduledJobs[idx] = { ...scheduledJobs[idx], ...req.body, id };
  registerCronJob(scheduledJobs[idx]);
  res.json({ success: true, job: scheduledJobs[idx] });
});

app.delete("/api/scheduler/:id", authRequired, (req, res) => {
  const id = parseInt(req.params.id);
  if (cronRegistry[id]) { cronRegistry[id].stop(); delete cronRegistry[id]; }
  scheduledJobs = scheduledJobs.filter(j => j.id !== id);
  res.json({ success: true });
});

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n🚀 MailFlow → http://localhost:${PORT}`);
  console.log(`👤 Login: ${ADMIN_USER} / ${process.env.ADMIN_PASS || "admin123"}`);
  console.log(`📧 SMTP: ${process.env.SMTP_USER || "⚠️ not configured"}\n`);
});
