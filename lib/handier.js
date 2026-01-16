const axios = require("axios");
const fs = require("fs");
const path = require("path");
const config = require("../config");
const BASE_URL = "https://x-kira-json-host.vercel.app";

async function getJson(url, options) {
  try {
    options ? options : {};
    const res = await axios({
      method: "GET",
      url: url,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
      },
      ...options,
    });
    return res.data;
  } catch (err) {
    return err;
  }
}

const fetchJson = async (url, options = {}) => {
  const res = await axios.get(url, {
    headers: { "User-Agent": "Mozilla/5.0" },
    ...options,
  });
  return res.data;
};

function makeId(length) {
  const chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  return Array.from(
    { length },
    () => chars[Math.floor(Math.random() * chars.length)]
  ).join("");
}

function getRandom(ext) {
  return `${Math.floor(Math.random() * 10000)}${ext}`;
}

const getBuffer = async (url, options = {}) => {
  const res = await axios.get(url, { ...options, responseType: "arraybuffer" });
  return res.data;
};

function MediaUrls(text) {
  let array = [];
  const regexp =
    /https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()'@:%_\+.~#?!&//=]*)/gi;
  let urls = text.match(regexp);
  if (urls) {
    urls.map((url) => {
      if (
        ["jpg", "jpeg", "png", "gif", "mp4", "webp"].includes(
          url.split(".").pop().toLowerCase()
        )
      ) {
        array.push(url);
      }
    });
    return array;
  } else {
    return false;
  }
}

function extractToken(sessionId) {
  if (!sessionId) return null;
  let m = sessionId.match(/≈\s*([^\^\s]+)/);
  if (m) return m[1].replace(/[^a-zA-Z0-9_-]/g, "");
  let fallback = sessionId.match(/([A-Za-z0-9_-]{6,})/g);
  if (fallback) {
    return fallback.find((t) => /[A-Za-z]/.test(t) && /\d/.test(t));
  }
  return null;
}

async function downloadCreds(sessionDir) {
  try {
    const credsId = config.SESSION_ID;
    const sessionPath = path.join(sessionDir, "creds.json");

    if (fs.existsSync(sessionPath)) {
      console.log("SESSION CONNECTED 🌚")
      return sessionPath;
    }
    // 🔹 STARK-MD~ Mega session support
    if (credsId.startsWith("STARK-MD~")) {
      console.log("[🕸️] DETECTED STARK-MD~SESSION FORMAT");
      const megaId = credsId.replace("STARK-MD~", "").trim();
      if (!megaId)
        throw new Error("❌ MEGA file ID missing after 'STARK-MD~'.");

      let File;
      try {
        File = require("megajs").File;
      } catch { }

      const file = File.fromURL(`https://mega.nz/file/${megaId}`);
      await file.loadAttributes();
      const data = await new Promise((resolve, reject) => {
        file.download((err, data) => (err ? reject(err) : resolve(data)));
      });

      fs.writeFileSync(sessionPath, data);
      console.log("[✅] STARK-MD~SESSION LOADED SUCCESSFULLY");
      return sessionPath;
    }

    if (credsId.startsWith("X-KIRA~")) {
      console.log("[🌱] DETECTED X-KIRA~SESSION FORMAT");
      const megaId = credsId.replace("X-KIRA~", "").trim();
      if (!megaId) throw new Error("❌ MEGA file ID missing after 'X-KIRA~'.");

      let File;
      try {
        File = require("megajs").File;
      } catch { }

      const file = File.fromURL(`https://mega.nz/file/${megaId}`);
      await file.loadAttributes();
      const data = await new Promise((resolve, reject) => {
        file.download((err, data) => (err ? reject(err) : resolve(data)));
      });

      fs.writeFileSync(sessionPath, data);
      console.log("[✅] STARK-MD~SESSION LOADED SUCCESSFULLY");
      return sessionPath;
    }

    // 🔹 Normal SESSION_ID logic
    const token = extractToken(credsId);
    if (!token) throw new Error("❌ Could not extract token from SESSION_ID");

    if (fs.existsSync(sessionPath)) {
      console.log("session already exists");
      return sessionPath;
    }

    const url = `${BASE_URL}/${encodeURIComponent(token)}`;
    console.log("Downloading creds from:", url);

    const res = await axios.get(url, {
      timeout: 10000,
      validateStatus: () => true,
    });
    if (res.status !== 200)
      throw new Error(`❌ Failed to download creds. Status: ${res.status}`);
    if (!res.data) throw new Error("❌ Empty response from server");

    const creds = typeof res.data === "object" ? res.data : { data: res.data };
    fs.writeFileSync(sessionPath, JSON.stringify(creds, null, 2));
    console.log("[✅] SESSION CONNECTED");
    return sessionPath;
  } catch (err) {
    console.error("❌ downloadCreds error:", err.message);
    throw err;
  }
}

module.exports = {
  getJson,
  fetchJson,
  getBuffer,
  makeId,
  getRandom,
  MediaUrls,
  downloadCreds,
};
