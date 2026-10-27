const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");
const { initializeApp, cert, getApps } = require("firebase-admin/app");
const { getAuth } = require("firebase-admin/auth");

let serviceAccount;
try {
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  } else {
    serviceAccount = require("./firebase-service-account.json");
  }
} catch (error) {
  console.error("Firebase service account could not be loaded.", error.message);
  process.exit(1);
}

const app = express();
const server = http.createServer(app);
const io = new Server(server);
if (!getApps().length) initializeApp({ credential: cert(serviceAccount) });

io.use(async (socket, next) => {
  const token = socket.handshake.auth?.token;
  if (!token) return next(new Error("Authentication required"));
  try {
    const decodedToken = await getAuth().verifyIdToken(token);
    socket.userId = decodedToken.uid;
    next();
  } catch (error) {
    console.error("Authentication failed:", error.message);
    next(new Error("Invalid authentication token"));
  }
});

app.get("/", (req, res) => res.sendFile(path.join(__dirname, "public", "index.html")));
app.use(express.static(path.join(__dirname, "public")));
app.get("/health", (req, res) => res.json({ status: "ok" }));

let waitingQueue = [];
const partners = new Map();
const userRegions = new Map();
const userGenders = new Map();
const reportCooldowns = new Map();

function broadcastOnlineCounts() {
  const totalAll = io.engine.clientsCount;
  let totalBD = 0;
  userRegions.forEach(region => { if (region === "BD") totalBD++; });
  userRegions.forEach((region, socketId) => {
    const socket = io.sockets.sockets.get(socketId);
    if (socket) socket.emit("online-count", region === "BD" ? totalBD : totalAll);
  });
}
function removeFromQueue(socketId) {
  waitingQueue = waitingQueue.filter(item => item.socketId !== socketId);
}
function leaveChat(socket) {
  removeFromQueue(socket.id);
  const partnerId = partners.get(socket.id);
  if (partnerId) {
    partners.delete(socket.id);
    partners.delete(partnerId);
    io.to(partnerId).emit("partner-left");
    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (partnerSocket && partnerSocket.connected) {
      const pRegion = userRegions.get(partnerId) || "BD";
      const pGender = userGenders.get(partnerId) || "ANY";
      // Put the remaining participant back into the queue after their partner leaves.
      joinQueue(partnerSocket, pRegion, pGender);
    }
  }
}
function joinQueue(socket, region = "BD", gender = "ANY") {
  leaveChat(socket);
  if (!socket.connected) return;
  region = ["BD", "ALL"].includes(region) ? region : "BD";
  gender = ["ANY", "MALE", "FEMALE"].includes(gender) ? gender : "ANY";
  userRegions.set(socket.id, region);
  userGenders.set(socket.id, gender);
  broadcastOnlineCounts();
  let matchIndex = -1;
  for (let i = 0; i < waitingQueue.length; i++) {
    const item = waitingQueue[i];
    if (item.socketId !== socket.id && io.sockets.sockets.has(item.socketId)) {
      const regionMatch = region === "ALL" || item.region === "ALL" || region === item.region;
      const genderMatch = gender === "ANY" || item.gender === "ANY" || gender === item.gender;
      if (regionMatch && genderMatch) { matchIndex = i; break; }
    }
  }
  if (matchIndex !== -1) {
    const matchedItem = waitingQueue.splice(matchIndex, 1)[0];
    const otherId = matchedItem.socketId;
    partners.set(socket.id, otherId);
    partners.set(otherId, socket.id);
    io.to(socket.id).emit("matched", { peerId: otherId, initiator: true });
    io.to(otherId).emit("matched", { peerId: socket.id, initiator: false });
  } else {
    removeFromQueue(socket.id);
    waitingQueue.push({ socketId: socket.id, region, gender });
    socket.emit("waiting");
  }
}

io.on("connection", async socket => {
  console.log("Connected:", socket.id);
  let clientIp = socket.handshake.headers["x-forwarded-for"] || socket.handshake.address || "";
  if (clientIp.includes(",")) clientIp = clientIp.split(",")[0].trim();
  clientIp = clientIp.replace(/^::ffff:/, "");
  let isBDUser = true;
  if (clientIp && !["127.0.0.1", "::1"].includes(clientIp) && !clientIp.startsWith("192.168.") && !clientIp.startsWith("10.") && !/^172\.(1[6-9]|2\d|3[01])\./.test(clientIp)) {
    try {
      const response = await fetch(`http://ip-api.com/json/${encodeURIComponent(clientIp)}?fields=status,countryCode`);
      const data = await response.json();
      if (data.status === "success") isBDUser = data.countryCode === "BD";
    } catch (error) {
      console.warn("Could not detect country from IP; defaulting to Bangladesh.");
    }
  }
  socket.isBD = isBDUser;
  userRegions.set(socket.id, "BD");
  userGenders.set(socket.id, "ANY");
  broadcastOnlineCounts();

  socket.on("next", data => {
    let region = data?.region || "BD";
    const gender = data?.gender || "ANY";
    if (region === "BD" && !socket.isBD) region = "ALL";
    joinQueue(socket, region, gender);
  });
  socket.on("stop-chat", () => leaveChat(socket));
  socket.on("typing", isTyping => {
    const partnerId = partners.get(socket.id);
    if (partnerId) io.to(partnerId).emit("partner-typing", Boolean(isTyping));
  });
  socket.on("signal", message => {
    if (!message || typeof message.target !== "string" || partners.get(socket.id) !== message.target || !message.data || typeof message.data.type !== "string") return;
    // Limit chat payload size; WebRTC SDP/candidate messages are passed through unchanged.
    if (message.data.type === "chat" && (typeof message.data.message !== "string" || message.data.message.length > 500)) return;
    io.to(message.target).emit("signal", { from: socket.id, data: message.data });
  });
  socket.on("report", payload => {
    const partnerId = partners.get(socket.id);
    if (!partnerId) {
      socket.emit("report-result", { ok: false, message: "You are not currently connected to a user." });
      return;
    }
    const now = Date.now();
    const lastReport = reportCooldowns.get(socket.id) || 0;
    if (now - lastReport < 30000) {
      socket.emit("report-result", { ok: false, message: "Please wait before sending another report." });
      return;
    }
    const reason = typeof payload?.reason === "string" ? payload.reason.trim().slice(0, 250) : "No reason supplied";
    if (!reason) {
      socket.emit("report-result", { ok: false, message: "Please provide a report reason." });
      return;
    }
    reportCooldowns.set(socket.id, now);
    // This records a report for the site owner; it does not automatically ban anyone.
    console.warn("USER REPORT", JSON.stringify({ at: new Date().toISOString(), reporterUid: socket.userId, reportedSocketId: partnerId, reason }));
    socket.emit("report-result", { ok: true, message: "Report submitted. Thank you." });
  });
  socket.on("disconnect", () => {
    leaveChat(socket);
    userRegions.delete(socket.id);
    userGenders.delete(socket.id);
    reportCooldowns.delete(socket.id);
    broadcastOnlineCounts();
    console.log("Disconnected:", socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, "0.0.0.0", () => console.log(`BanglaConnect server running on port ${PORT}`));
