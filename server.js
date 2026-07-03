// server.js
// Polls YouTube for subscriber count + live chat (Super Chats, Super Stickers,
// membership gifts) + live stream status, and queues "ball drop" events for
// your Roblox game to consume.
//
// Deploy on Render.com or Railway (free tiers work, unlike Replit which sleeps).
// Set these environment variables in your hosting dashboard:
//   YOUTUBE_API_KEY   - from Google Cloud Console (YouTube Data API v3 enabled)
//   CHANNEL_ID        - your channel ID, starts with "UC..."
//   SHARED_SECRET     - any random string, acts as a password for your endpoints

const express = require("express");
const app = express();

// ---- CONFIG ----
const YOUTUBE_API_KEY = process.env.YOUTUBE_API_KEY;
const CHANNEL_ID = process.env.CHANNEL_ID;
const SHARED_SECRET = process.env.SHARED_SECRET || "change-me";

const SUB_POLL_MS = 60 * 1000;        // subscriber count: check every 60s (quota-friendly)
const LIVE_CHECK_MS = 60 * 1000;      // check if a stream is live every 60s
const CHAT_POLL_MS = 8 * 1000;        // poll live chat every 8s while live
const LIKE_POLL_MS = 30 * 1000;       // check the live video's like count every 30s

const LIKE_BATCH_SIZE = 5;            // every 5 new likes = 1 ball (avoids ball-flooding on popular streams)

// ---- BALL RULES ----
function ballsForSuperChat(usdAmount) {
  // $1 = 5 balls, then +1 ball per additional whole dollar
  if (usdAmount < 1) return Math.max(1, Math.round(usdAmount * 5));
  return 5 + Math.floor(usdAmount - 1);
}
function ballsForGift(giftCount) {
  return Math.max(10, giftCount * 10); // 10 golden balls minimum, scales with gift count
}
const BALLS_FOR_SUB = 3;
const BALLS_FOR_STREAM_START = 1;
const BALLS_PER_LIKE_BATCH = 1; // 1 ball dropped per LIKE_BATCH_SIZE new likes

// ---- STATE ----
let lastKnownSubCount = null;
let currentLiveChatId = null;
let currentVideoId = null;
let liveChatNextPageToken = null;
let wasLive = false;
let lastKnownLikeCount = null;
let likeRemainder = 0; // carries over leftover likes that haven't hit a full batch yet
let eventQueue = []; // array of { type, balls, message, color }

function pushEvent(evt) {
  eventQueue.push(evt);
  console.log("Queued event:", evt);
}

// ---- SUBSCRIBER POLLING ----
async function pollSubscribers() {
  try {
    const url = `https://www.googleapis.com/youtube/v3/channels?part=statistics&id=${CHANNEL_ID}&key=${YOUTUBE_API_KEY}`;
    const res = await fetch(url);
    const data = await res.json();
    if (!data.items?.length) return console.error("No channel data:", data);

    const count = parseInt(data.items[0].statistics.subscriberCount, 10);

    if (lastKnownSubCount === null) {
      lastKnownSubCount = count;
      console.log(`Baseline subscribers: ${count}`);
      return;
    }
    if (count > lastKnownSubCount) {
      const gained = count - lastKnownSubCount;
      for (let i = 0; i < gained; i++) {
        pushEvent({
          type: "subscribe",
          balls: BALLS_FOR_SUB,
          message: "Someone just subscribed!",
          color: "blue",
        });
      }
      lastKnownSubCount = count;
    } else if (count < lastKnownSubCount) {
      lastKnownSubCount = count; // unsub, no event
    }
  } catch (err) {
    console.error("pollSubscribers error:", err.message);
  }
}

// ---- LIVE STATUS POLLING ----
async function pollLiveStatus() {
  try {
    const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${CHANNEL_ID}&eventType=live&type=video&key=${YOUTUBE_API_KEY}`;
    const res = await fetch(url);
    const data = await res.json();
    const liveVideo = data.items?.[0];

    if (liveVideo && !wasLive) {
      wasLive = true;
      pushEvent({
        type: "stream_start",
        balls: BALLS_FOR_STREAM_START,
        message: "Stream just started!",
        color: "silver",
      });
      // fetch the liveChatId for this broadcast so we can poll chat
      const videoId = liveVideo.id.videoId;
      currentVideoId = videoId;
      const vRes = await fetch(
        `https://www.googleapis.com/youtube/v3/videos?part=liveStreamingDetails&id=${videoId}&key=${YOUTUBE_API_KEY}`
      );
      const vData = await vRes.json();
      currentLiveChatId = vData.items?.[0]?.liveStreamingDetails?.activeLiveChatId || null;
      liveChatNextPageToken = null;
      lastKnownLikeCount = null; // re-baseline likes for this new stream
      likeRemainder = 0;
    } else if (!liveVideo && wasLive) {
      wasLive = false;
      currentLiveChatId = null;
      currentVideoId = null;
      liveChatNextPageToken = null;
      lastKnownLikeCount = null;
      likeRemainder = 0;
      console.log("Stream ended.");
    }
  } catch (err) {
    console.error("pollLiveStatus error:", err.message);
  }
}

// ---- LIVE CHAT POLLING (Super Chats, Super Stickers, Gifted Memberships) ----
async function pollLiveChat() {
  if (!currentLiveChatId) return;
  try {
    let url = `https://www.googleapis.com/youtube/v3/liveChat/messages?liveChatId=${currentLiveChatId}&part=snippet,authorDetails&key=${YOUTUBE_API_KEY}`;
    if (liveChatNextPageToken) url += `&pageToken=${liveChatNextPageToken}`;

    const res = await fetch(url);
    const data = await res.json();
    if (data.error) {
      console.error("Live chat error:", data.error.message);
      return;
    }
    liveChatNextPageToken = data.nextPageToken;

    for (const item of data.items || []) {
      const snippet = item.snippet;
      const author = item.authorDetails?.displayName || "Someone";

      if (snippet.type === "superChatEvent" || snippet.type === "superStickerEvent") {
        const details = snippet.superChatDetails || snippet.superStickerDetails;
        const usdAmount = (details.amountMicros || 0) / 1e6;
        pushEvent({
          type: "superchat",
          balls: ballsForSuperChat(usdAmount),
          message: `${author} sent a Super Chat! (${details.amountDisplayString || "$" + usdAmount.toFixed(2)})`,
          color: "green",
        });
      } else if (snippet.type === "membershipGiftingEvent") {
        const giftCount = snippet.membershipGiftingDetails?.giftMembershipsCount || 1;
        pushEvent({
          type: "gift",
          balls: ballsForGift(giftCount),
          message: `${author} gifted ${giftCount} membership${giftCount > 1 ? "s" : ""}!`,
          color: "gold",
        });
      }
      // Note: individual "giftMembershipReceivedEvent" messages are skipped to
      // avoid double-counting the same gift batch above.
    }
  } catch (err) {
    console.error("pollLiveChat error:", err.message);
  }
}

// ---- LIKE POLLING (likes on the current live stream) ----
async function pollLikes() {
  if (!currentVideoId) return;
  try {
    const url = `https://www.googleapis.com/youtube/v3/videos?part=statistics&id=${currentVideoId}&key=${YOUTUBE_API_KEY}`;
    const res = await fetch(url);
    const data = await res.json();
    if (!data.items?.length) return;

    const likeCount = parseInt(data.items[0].statistics.likeCount || "0", 10);

    if (lastKnownLikeCount === null) {
      lastKnownLikeCount = likeCount;
      console.log(`Baseline likes for this stream: ${likeCount}`);
      return;
    }

    if (likeCount > lastKnownLikeCount) {
      const gained = likeCount - lastKnownLikeCount;
      lastKnownLikeCount = likeCount;
      likeRemainder += gained;

      const batches = Math.floor(likeRemainder / LIKE_BATCH_SIZE);
      if (batches > 0) {
        likeRemainder -= batches * LIKE_BATCH_SIZE;
        for (let i = 0; i < batches; i++) {
          pushEvent({
            type: "like",
            balls: BALLS_PER_LIKE_BATCH,
            message: `${LIKE_BATCH_SIZE} people just liked the stream!`,
            color: "pink",
          });
        }
      }
    }
    // note: likeCount can also drop if people un-like, we just track silently
    else if (likeCount < lastKnownLikeCount) {
      lastKnownLikeCount = likeCount;
    }
  } catch (err) {
    console.error("pollLikes error:", err.message);
  }
}

setInterval(pollSubscribers, SUB_POLL_MS);
setInterval(pollLiveStatus, LIVE_CHECK_MS);
setInterval(pollLiveChat, CHAT_POLL_MS);
setInterval(pollLikes, LIKE_POLL_MS);
pollSubscribers();
pollLiveStatus();

// ---- ENDPOINTS ----

// Roblox polls this every few seconds. Returns queued events and clears the queue.
app.get("/api/consume-events", (req, res) => {
  if (req.query.secret !== SHARED_SECRET) return res.status(401).json({ error: "unauthorized" });
  const events = eventQueue;
  eventQueue = [];
  res.json({ events });
});

app.get("/api/status", (req, res) => {
  if (req.query.secret !== SHARED_SECRET) return res.status(401).json({ error: "unauthorized" });
  res.json({
    totalSubscribers: lastKnownSubCount,
    isLive: wasLive,
    currentLikeCount: lastKnownLikeCount,
    queuedEvents: eventQueue.length,
  });
});


app.get("/api/debug-live", async (req, res) => {
  if (req.query.secret !== SHARED_SECRET) return res.status(401).end();
  const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${CHANNEL_ID}&eventType=live&type=video&key=${YOUTUBE_API_KEY}`;
  const r = await fetch(url);
  const data = await r.json();
  res.json(data);
});const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
