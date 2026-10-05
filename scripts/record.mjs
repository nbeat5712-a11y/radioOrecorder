// scripts/record.mjs
// Run by GitHub Actions (see .github/workflows/record-broadcasts.yml).
//
// What it does, every time the workflow fires:
//   1. Reads the Google Calendar and finds regular, timed events that are running now or
//      start within the next LOOKAHEAD_MS. (All-day events and "~" special events are skipped.)
//   2. Waits for the event to start, then saves the live stream to a temp file until the
//      event ends (reconnecting if the connection drops).
//   3. Uploads the result to a Google Drive folder, which is created (and shared as
//      "anyone with the link can view") the first time the script runs.
//   4. If another event starts right when this one ends, it carries on with that one in the
//      same run, so nothing is lost between back-to-back shows.
//
// Usage:  node scripts/record.mjs           (normal run)
//         node scripts/record.mjs --setup   (only create the Drive folder and print its ID)
//
// Required environment variables (GitHub repo -> Settings -> Secrets and variables -> Actions):
//   GOOGLE_CALENDAR_API_KEY, GOOGLE_CALENDAR_ID
//   GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, GOOGLE_OAUTH_REFRESH_TOKEN
// Optional:
//   STREAM_URL  - overrides the default stream address

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { once } from 'node:events';

const STREAM_URL = process.env.STREAM_URL || 'https://c44.radioboss.fm:8178/stream';
const TIMEZONE = 'Asia/Jerusalem';            // only used for the date in the file name
const LOOKAHEAD_MS = 25 * 60 * 1000;          // how far ahead the first look at the calendar goes
const CHAIN_LOOKAHEAD_MS = 3 * 60 * 1000;     // after a recording, only chain events starting this soon
const MIN_REMAINING_MS = 60 * 1000;           // don't bother with an event that has < 1 minute left
const JOB_BUDGET_MS = 345 * 60 * 1000;        // GitHub kills jobs at 6h; stay safely under it
const MIN_USEFUL_BYTES = 100 * 1024;          // anything smaller than this is a failed recording
const FOLDER_NAME = 'imsostablee Recordings';
const FOLDER_ROLE = 'recordings-root';        // stored as a Drive property so renaming the folder is safe
const SPECIAL_PREFIX = /^\s*~\s*/;            // same rule as api/calendar.js

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const log = (...args) => console.log(new Date().toISOString(), ...args);

function need(name) {
    const value = process.env[name];
    if (!value) throw new Error(`Missing environment variable ${name}`);
    return value;
}

// ---------------------------------------------------------------- Calendar

async function fetchUpcomingEvents(fromMs, lookaheadMs) {
    const apiKey = need('GOOGLE_CALENDAR_API_KEY');
    const calendarId = need('GOOGLE_CALENDAR_ID');
    const timeMin = new Date(fromMs).toISOString();
    const timeMax = new Date(fromMs + lookaheadMs).toISOString();

    // timeMin is compared against the event's END, so events that are already running are included.
    const url = `https://www.googleapis.com/calendar/v3/calendars/${encodeURIComponent(calendarId)}/events` +
        `?key=${apiKey}` +
        `&timeMin=${encodeURIComponent(timeMin)}` +
        `&timeMax=${encodeURIComponent(timeMax)}` +
        `&singleEvents=true&orderBy=startTime&maxResults=50`;

    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) {
        // The URL contains the API key, so never print it.
        throw new Error(`Google Calendar request failed: ${res.status} ${await res.text().catch(() => '')}`);
    }
    const data = await res.json();

    return (data.items || [])
        .filter((item) => item.status !== 'cancelled' && item.start && item.start.dateTime && item.end && item.end.dateTime)
        .filter((item) => !SPECIAL_PREFIX.test(item.summary || ''))
        .map((item) => ({
            // One stable key per event instance, used to avoid recording the same event twice.
            key: crypto.createHash('sha1').update(`${item.id}|${item.start.dateTime}`).digest('hex'),
            title: (item.summary || 'Untitled event').trim(),
            startMs: new Date(item.start.dateTime).getTime(),
            endMs: new Date(item.end.dateTime).getTime()
        }))
        .filter((ev) => Number.isFinite(ev.startMs) && Number.isFinite(ev.endMs) && ev.endMs > ev.startMs);
}

// ---------------------------------------------------------------- Google Drive

async function getAccessToken() {
    const res = await fetch('https://oauth2.googleapis.com/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
            client_id: need('GOOGLE_OAUTH_CLIENT_ID'),
            client_secret: need('GOOGLE_OAUTH_CLIENT_SECRET'),
            refresh_token: need('GOOGLE_OAUTH_REFRESH_TOKEN'),
            grant_type: 'refresh_token'
        })
    });
    if (!res.ok) throw new Error(`Could not get a Google access token: ${res.status} ${await res.text().catch(() => '')}`);
    const data = await res.json();
    return data.access_token;
}

async function driveJson(token, url, options = {}) {
    const res = await fetch(url, {
        ...options,
        headers: { Authorization: `Bearer ${token}`, ...(options.headers || {}) }
    });
    if (!res.ok) throw new Error(`Drive request failed: ${res.status} ${await res.text().catch(() => '')}`);
    return res.json();
}

const escapeQuery = (value) => String(value).replace(/[\\']/g, '\\$&');

async function findOrCreateFolder(token) {
    const q = `properties has { key='imsRole' and value='${FOLDER_ROLE}' } and trashed=false`;
    const list = await driveJson(token,
        `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(q)}&fields=files(id,name)&spaces=drive`);
    if (list.files && list.files.length > 0) return list.files[0].id;

    log('Creating the Drive folder...');
    const folder = await driveJson(token, 'https://www.googleapis.com/drive/v3/files?fields=id', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            name: FOLDER_NAME,
            mimeType: 'application/vnd.google-apps.folder',
            properties: { imsRole: FOLDER_ROLE }
        })
    });
    // Anyone with the link can view -> the website can list and play the files.
    await driveJson(token, `https://www.googleapis.com/drive/v3/files/${folder.id}/permissions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ role: 'reader', type: 'anyone' })
    });
    return folder.id;
}

async function alreadyRecorded(token, eventKey) {
    const q = `properties has { key='eventKey' and value='${escapeQuery(eventKey)}' } and trashed=false`;
    const list = await driveJson(token,
        `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(q)}&fields=files(id)&spaces=drive`);
    return !!(list.files && list.files.length > 0);
}

async function uploadRecording(folderId, filePath, meta) {
    const token = await getAccessToken(); // fresh token: the recording itself may have taken hours
    const size = fs.statSync(filePath).size;

    const init = await fetch('https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&fields=id,name', {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json; charset=UTF-8',
            'X-Upload-Content-Type': meta.mimeType,
            'X-Upload-Content-Length': String(size)
        },
        body: JSON.stringify({
            name: meta.name,
            parents: [folderId],
            description: meta.description,
            properties: { eventKey: meta.eventKey }
        })
    });
    if (!init.ok) throw new Error(`Drive upload could not start: ${init.status} ${await init.text().catch(() => '')}`);
    const location = init.headers.get('location');
    if (!location) throw new Error('Drive upload could not start: no upload location returned');

    const put = await fetch(location, {
        method: 'PUT',
        headers: { 'Content-Type': meta.mimeType },
        body: fs.readFileSync(filePath)
    });
    if (!put.ok) throw new Error(`Drive upload failed: ${put.status} ${await put.text().catch(() => '')}`);
    return put.json();
}

async function withRetry(label, fn, attempts = 3) {
    for (let i = 1; ; i++) {
        try {
            return await fn();
        } catch (err) {
            if (i >= attempts) throw err;
            log(`${label} failed (attempt ${i}/${attempts}): ${err.message}`);
            await sleep(5000 * i);
        }
    }
}

// ---------------------------------------------------------------- Recording

function audioFormat(contentType = '') {
    const ct = contentType.toLowerCase();
    if (ct.includes('aac')) return { ext: 'aac', mime: 'audio/aac' };
    if (ct.includes('opus')) return { ext: 'opus', mime: 'audio/ogg' };
    if (ct.includes('ogg')) return { ext: 'ogg', mime: 'audio/ogg' };
    return { ext: 'mp3', mime: 'audio/mpeg' };
}

// Saves the raw stream bytes (no re-encoding, so original quality) until untilMs.
async function recordStream(untilMs, filePath) {
    const out = fs.createWriteStream(filePath);
    let bytes = 0;
    let contentType = null;
    let failures = 0;

    while (Date.now() < untilMs - 1000) {
        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), Math.max(0, untilMs - Date.now()));
        try {
            const res = await fetch(STREAM_URL, { signal: ctrl.signal, headers: { 'Icy-MetaData': '0' } });
            if (!res.ok || !res.body) throw new Error(`stream responded with ${res.status}`);
            if (!contentType) contentType = res.headers.get('content-type');
            failures = 0;
            for await (const chunk of res.body) {
                bytes += chunk.length;
                if (!out.write(chunk)) await once(out, 'drain');
            }
            log('Stream ended early, reconnecting...');
        } catch (err) {
            if (ctrl.signal.aborted) break; // reached the end of the event
            failures++;
            log(`Stream error (${failures}): ${err.message}`);
        } finally {
            clearTimeout(timer);
        }
        // Keep trying until the event is over (the station may come back), backing off a little.
        if (Date.now() < untilMs - 1000) await sleep(Math.min(2000 * Math.max(failures, 1), 15000));
    }

    out.end();
    await once(out, 'finish');
    return { bytes, contentType };
}

function localStamp(ms) {
    const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: TIMEZONE, year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', hourCycle: 'h23'
    }).formatToParts(new Date(ms));
    const get = (type) => parts.find((p) => p.type === type).value;
    return `${get('year')}-${get('month')}-${get('day')}_${get('hour')}${get('minute')}`;
}

function safeTitle(title) {
    return title.replace(/[\\/:*?"<>|\u0000-\u001f]+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 80) || 'broadcast';
}

// Waits for the event, records it, then starts the upload WITHOUT waiting for it. Returns
// { upload } where upload is a promise (resolving to true/false) - wrapped in an object so
// the async function doesn't flatten it - letting the caller move straight on to a following
// event while this one uploads.
async function recordEvent(ev, folderId, runDeadlineMs) {
    const waitMs = ev.startMs - Date.now();
    if (waitMs > 0) {
        log(`Waiting ${Math.round(waitMs / 1000)}s for "${ev.title}" to start...`);
        await sleep(waitMs);
    }

    const recordedFromMs = Date.now();
    const untilMs = Math.min(ev.endMs, runDeadlineMs);
    log(`Recording "${ev.title}" until ${new Date(untilMs).toISOString()}`);

    const tmpPath = path.join(os.tmpdir(), `rec-${ev.key.slice(0, 8)}-${Date.now()}.part`);
    const { bytes, contentType } = await recordStream(untilMs, tmpPath);

    if (bytes < MIN_USEFUL_BYTES) {
        fs.rmSync(tmpPath, { force: true });
        throw new Error(`Recording of "${ev.title}" is empty or too small (${bytes} bytes)`);
    }

    const { ext, mime } = audioFormat(contentType || '');
    const name = `${localStamp(ev.startMs)}_${safeTitle(ev.title)}.${ext}`;
    const description = JSON.stringify({
        title: ev.title,
        start: new Date(ev.startMs).toISOString(),
        end: new Date(ev.endMs).toISOString(),
        // Differs from "start" when the recording began late (e.g. a delayed GitHub schedule).
        recordedFrom: new Date(recordedFromMs).toISOString()
    });
    log(`Recorded ${(bytes / 1048576).toFixed(1)} MB, uploading as "${name}"`);

    const upload = withRetry('Upload', () => uploadRecording(folderId, tmpPath, { name, mimeType: mime, description, eventKey: ev.key }))
        .then(() => { log(`Uploaded "${name}"`); return true; })
        .catch((err) => { log(`Upload of "${name}" failed: ${err.message}`); return false; })
        .finally(() => fs.rmSync(tmpPath, { force: true }));
    return { upload };
}

// ---------------------------------------------------------------- Main

function writeSummary(text) {
    if (process.env.GITHUB_STEP_SUMMARY) fs.appendFileSync(process.env.GITHUB_STEP_SUMMARY, text + '\n');
}

async function main() {
    const runStartMs = Date.now();
    const runDeadlineMs = runStartMs + JOB_BUDGET_MS;

    if (process.argv.includes('--setup')) {
        const token = await getAccessToken();
        const folderId = await findOrCreateFolder(token);
        log(`Drive folder ready. GOOGLE_DRIVE_FOLDER_ID = ${folderId}`);
        writeSummary(`### Drive folder ready\n\nSet this in Vercel as \`GOOGLE_DRIVE_FOLDER_ID\`:\n\n\`${folderId}\``);
        return;
    }

    const handled = new Set();
    const uploads = [];
    let folderId = null;
    let failed = false;
    let first = true;

    while (runDeadlineMs - Date.now() > 5 * 60 * 1000) {
        let events;
        try {
            events = await fetchUpcomingEvents(Date.now(), first ? LOOKAHEAD_MS : CHAIN_LOOKAHEAD_MS);
        } catch (err) {
            if (first) throw err;
            log(err.message);
            break;
        }
        first = false;

        const next = events.find((ev) => !handled.has(ev.key) && ev.endMs - Date.now() > MIN_REMAINING_MS);
        if (!next) break;
        handled.add(next.key);

        const token = await getAccessToken();
        if (!folderId) folderId = await findOrCreateFolder(token);
        if (await alreadyRecorded(token, next.key)) {
            log(`"${next.title}" is already recorded, skipping.`);
            continue;
        }

        try {
            // Returns as soon as the recording is finished; the upload continues in the background.
            const { upload } = await recordEvent(next, folderId, runDeadlineMs);
            uploads.push(upload);
        } catch (err) {
            failed = true;
            log(err.message);
        }
    }

    const results = await Promise.all(uploads);
    if (results.some((ok) => !ok)) failed = true;
    if (handled.size === 0) log('Nothing to record right now.');
    if (failed) process.exitCode = 1;
}

main().catch((err) => {
    console.error(err.message || err);
    process.exit(1);
});
