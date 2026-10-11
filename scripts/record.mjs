
Record mjs · TXT
// scripts/record.mjs
// Run by GitHub Actions (see .github/workflows/record-broadcasts.yml).
//
// What it does, every time the workflow fires:
//   1. Reads the Google Calendar and finds regular, timed events that are running now or
//      start within the next LOOKAHEAD_MS. (All-day events and "~" special events are skipped.)
//   2. Waits for the event to start, then saves the live stream to a temp file until the
//      event ends (reconnecting if the connection drops).
//   3. Wraps the raw AAC stream in an M4A container (no re-encoding, same quality) so the
//      website can show the duration and seek to any time, then uploads it to Cloudflare R2.
//   4. Rebuilds index.json in the bucket - the list the website's ARCHIVE tab reads.
//   5. If another event starts right when this one ends, it carries on with that one in the
//      same run, so nothing is lost between back-to-back shows.
//
// Usage:  node scripts/record.mjs             (normal run)
//         node scripts/record.mjs --reindex   (only rebuild index.json from the bucket contents;
//                                              use after adding or deleting files by hand)
//
// Required environment variables (GitHub repo -> Settings -> Secrets and variables -> Actions):
//   GOOGLE_CALENDAR_API_KEY, GOOGLE_CALENDAR_ID
//   R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
//   R2_PUBLIC_URL   - the bucket's public address, e.g. https://pub-xxxx.r2.dev
// Optional:
//   STREAM_URL      - overrides the default stream address
//
// Bucket layout: recordings sit in the bucket root ("2026-10-05_2100_@name (Live).m4a"), or
// inside a folder named after the DJ ("DJ name/2026-10-05_show.m4a"). Keys starting with "."
// (".events/...") are internal markers that stop the same event being recorded twice.
 
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { once } from 'node:events';
import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { S3Client, ListObjectsV2Command, HeadObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
 
const STREAM_URL = process.env.STREAM_URL || 'https://c44.radioboss.fm:8178/stream';
const TIMEZONE = 'Asia/Jerusalem';            // only used for the date in the file name
const LOOKAHEAD_MS = 25 * 60 * 1000;          // how far ahead the first look at the calendar goes
const CHAIN_LOOKAHEAD_MS = 3 * 60 * 1000;     // after a recording, only chain events starting this soon
const MIN_REMAINING_MS = 60 * 1000;           // don't bother with an event that has < 1 minute left
const JOB_BUDGET_MS = 345 * 60 * 1000;        // GitHub kills jobs at 6h; stay safely under it
const MIN_USEFUL_BYTES = 100 * 1024;          // anything smaller than this is a failed recording
const SPECIAL_PREFIX = /^\s*~\s*/;            // same rule as api/calendar.js
const INDEX_KEY = 'index.json';
const MARKER_PREFIX = '.events/';
 
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
 
// ---------------------------------------------------------------- Cloudflare R2
 
let s3 = null;
function r2() {
    if (!s3) {
        s3 = new S3Client({
            region: 'auto',
            endpoint: `https://${need('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com`,
            credentials: {
                accessKeyId: need('R2_ACCESS_KEY_ID'),
                secretAccessKey: need('R2_SECRET_ACCESS_KEY')
            },
            // R2 does not support the SDK's newer default checksum headers.
            requestChecksumCalculation: 'WHEN_REQUIRED',
            responseChecksumValidation: 'WHEN_REQUIRED'
        });
    }
    return s3;
}
 
const bucket = () => need('R2_BUCKET');
 
function isNotFound(err) {
    return err && (err.name === 'NotFound' || err.name === 'NoSuchKey' || (err.$metadata && err.$metadata.httpStatusCode === 404));
}
 
async function alreadyRecorded(eventKey) {
    try {
        await r2().send(new HeadObjectCommand({ Bucket: bucket(), Key: MARKER_PREFIX + eventKey }));
        return true;
    } catch (err) {
        if (isNotFound(err)) return false;
        throw err;
    }
}
 
async function markRecorded(eventKey) {
    await r2().send(new PutObjectCommand({ Bucket: bucket(), Key: MARKER_PREFIX + eventKey, Body: '', ContentType: 'text/plain' }));
}
 
async function uploadRecording(filePath, key, contentType) {
    const upload = new Upload({
        client: r2(),
        params: {
            Bucket: bucket(),
            Key: key,
            Body: fs.createReadStream(filePath),
            ContentType: contentType,
            CacheControl: 'public, max-age=31536000'
        },
        queueSize: 2,
        partSize: 16 * 1024 * 1024
    });
    await upload.done();
}
 
async function listAllObjects() {
    const objects = [];
    let token;
    do {
        const page = await r2().send(new ListObjectsV2Command({ Bucket: bucket(), ContinuationToken: token }));
        objects.push(...(page.Contents || []));
        token = page.IsTruncated ? page.NextContinuationToken : undefined;
    } while (token);
    return objects;
}
 
// ---------------------------------------------------------------- Archive index
// Same naming rules the website used when it read the Google Drive folder:
//   1. "@name" in the file name is the DJ (case-insensitive grouping)
//   2. else the folder the file sits in
//   3. else "DJ - Title - date" / "DJ_Title_date" in the file name
//   4. else "Unknown"
// The date comes from the file name (YYYY-MM-DD) and falls back to the upload time.
 
const AUDIO_EXT = /\.(mp3|m4a|aac|ogg|opus|wav|flac)$/i;
const DATE_RE = /(\d{4})[-_.](\d{2})[-_.](\d{2})/;
const AT_RE = /(^|[\s_\-–—|,])(@[^\s_\-–—|,()]+)/;
 
function cleanSeparators(s) {
    return s.replace(/^[-_ .|,]+|[-_ .|,]+$/g, '').replace(/[-_ .]{2,}/g, ' - ');
}
 
export function parseName(fileName) {
    const base = fileName.replace(AUDIO_EXT, '');
    const m = base.match(DATE_RE);
    const date = m ? `${m[1]}-${m[2]}-${m[3]}` : null;
 
    // Drop the date, plus a recording time like "_2100" that directly follows it.
    const withoutDate = base.replace(/(\d{4})[-_.](\d{2})[-_.](\d{2})([-_ ]\d{4})?/, '');
 
    const at = withoutDate.match(AT_RE);
    const atDj = at ? at[2] : null;
    const rest = cleanSeparators(atDj ? withoutDate.replace(AT_RE, '$1') : withoutDate);
 
    let dj = null;
    let title = rest;
    if (!atDj) {
        const dash = rest.split(' - ');
        if (dash.length > 1) {
            dj = dash[0].trim();
            title = dash.slice(1).join(' - ').trim();
        } else {
            const us = rest.split('_');
            if (us.length > 1) {
                dj = us[0].trim();
                title = us.slice(1).join(' ').trim();
            }
        }
    }
    return { atDj, dj, title: title || base, date };
}
 
export function buildItems(objects, publicUrl) {
    const base = publicUrl.replace(/\/+$/, '');
    const spelling = new Map(); // "@Name" and "@name" must land in the same group
    const canonical = (name) => {
        const key = name.toLowerCase();
        if (!spelling.has(key)) spelling.set(key, name);
        return spelling.get(key);
    };
 
    return objects
        .filter((o) => o.Key && !o.Key.startsWith('.') && o.Key !== INDEX_KEY && AUDIO_EXT.test(o.Key))
        .map((o) => {
            const parts = o.Key.split('/');
            const fileName = parts[parts.length - 1];
            const folderDj = parts.length > 1 ? parts[0] : null;
            const parsed = parseName(fileName);
            const modified = o.LastModified ? new Date(o.LastModified).toISOString().slice(0, 10) : null;
            const owner = parsed.atDj || folderDj || parsed.dj || 'Unknown';
            return {
                id: o.Key,
                dj: canonical(owner),
                title: parsed.title,
                date: parsed.date || modified,
                size: typeof o.Size === 'number' ? o.Size : null,
                url: `${base}/${parts.map(encodeURIComponent).join('/')}`
            };
        });
}
 
async function writeIndex() {
    const objects = await listAllObjects();
    const items = buildItems(objects, need('R2_PUBLIC_URL'));
    await r2().send(new PutObjectCommand({
        Bucket: bucket(),
        Key: INDEX_KEY,
        Body: JSON.stringify(items),
        ContentType: 'application/json; charset=utf-8',
        CacheControl: 'no-cache, max-age=0'
    }));
    log(`index.json updated with ${items.length} recording(s).`);
}
 
// ---------------------------------------------------------------- Recording
 
function audioFormat(contentType = '') {
    const ct = contentType.toLowerCase();
    if (ct.includes('aac')) return { ext: 'aac', mime: 'audio/aac', wrap: true };
    if (ct.includes('opus')) return { ext: 'opus', mime: 'audio/ogg', wrap: false };
    if (ct.includes('ogg')) return { ext: 'ogg', mime: 'audio/ogg', wrap: false };
    return { ext: 'mp3', mime: 'audio/mpeg', wrap: false };
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
 
// Copies the raw AAC (ADTS) stream into an M4A container. No re-encoding: same audio, but the
// file now knows its length and can be seeked, which browsers (especially iPhones) need.
function remuxToM4a(inputPath, outputPath) {
    return new Promise((resolve, reject) => {
        const ff = spawn('ffmpeg', [
            '-y', '-v', 'error',
            '-f', 'aac', '-i', inputPath,
            '-c', 'copy',
            '-bsf:a', 'aac_adtstoasc',
            '-movflags', '+faststart',
            outputPath
        ], { stdio: ['ignore', 'inherit', 'inherit'] });
        ff.on('error', reject);
        ff.on('exit', (code) => (code === 0 ? resolve() : reject(new Error(`ffmpeg exited with code ${code}`))));
    });
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
 
// Waits for the event, records it, converts it, then starts the upload WITHOUT waiting for it.
// Returns { upload } where upload is a promise (resolving to true/false) - wrapped in an object so
// the async function doesn't flatten it - letting the caller move straight on to a following
// event while this one uploads.
async function recordEvent(ev, runDeadlineMs) {
    const waitMs = ev.startMs - Date.now();
    if (waitMs > 0) {
        log(`Waiting ${Math.round(waitMs / 1000)}s for "${ev.title}" to start...`);
        await sleep(waitMs);
    }
 
    const untilMs = Math.min(ev.endMs, runDeadlineMs);
    log(`Recording "${ev.title}" until ${new Date(untilMs).toISOString()}`);
 
    const tmpPath = path.join(os.tmpdir(), `rec-${ev.key.slice(0, 8)}-${Date.now()}.part`);
    const m4aPath = `${tmpPath}.m4a`;
    const cleanup = () => { fs.rmSync(tmpPath, { force: true }); fs.rmSync(m4aPath, { force: true }); };
 
    const { bytes, contentType } = await recordStream(untilMs, tmpPath);
 
    if (bytes < MIN_USEFUL_BYTES) {
        cleanup();
        throw new Error(`Recording of "${ev.title}" is empty or too small (${bytes} bytes)`);
    }
 
    const format = audioFormat(contentType || '');
    let uploadPath = tmpPath;
    let ext = format.ext;
    let mime = format.mime;
 
    if (format.wrap) {
        try {
            await remuxToM4a(tmpPath, m4aPath);
            uploadPath = m4aPath;
            ext = 'm4a';
            mime = 'audio/mp4';
        } catch (err) {
            // Never lose a recording because of the conversion: fall back to the raw file.
            log(`M4A conversion failed (${err.message}); uploading the raw .${ext} file instead.`);
        }
    }
 
    const key = `${localStamp(ev.startMs)}_${safeTitle(ev.title)}.${ext}`;
    const finalBytes = fs.statSync(uploadPath).size;
    log(`Recorded ${(bytes / 1048576).toFixed(1)} MB, uploading ${(finalBytes / 1048576).toFixed(1)} MB as "${key}"`);
 
    const upload = withRetry('Upload', async () => {
        await uploadRecording(uploadPath, key, mime);
        await markRecorded(ev.key);
    })
        .then(() => { log(`Uploaded "${key}"`); return true; })
        .catch((err) => { log(`Upload of "${key}" failed: ${err.message}`); return false; })
        .finally(cleanup);
    return { upload };
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
 
// ---------------------------------------------------------------- Main
 
async function main() {
    if (process.argv.includes('--reindex')) {
        await writeIndex();
        return;
    }
 
    const runStartMs = Date.now();
    const runDeadlineMs = runStartMs + JOB_BUDGET_MS;
 
    const handled = new Set();
    const uploads = [];
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
 
        if (await alreadyRecorded(next.key)) {
            log(`"${next.title}" is already recorded, skipping.`);
            continue;
        }
 
        try {
            // Returns as soon as the recording is finished; the upload continues in the background.
            const { upload } = await recordEvent(next, runDeadlineMs);
            uploads.push(upload);
        } catch (err) {
            failed = true;
            log(err.message);
        }
    }
 
    const results = await Promise.all(uploads);
    if (results.some((ok) => !ok)) failed = true;
 
    if (results.some((ok) => ok)) {
        try {
            await writeIndex();
        } catch (err) {
            failed = true;
            log(`Could not update index.json: ${err.message}`);
        }
    }
 
    if (handled.size === 0) log('Nothing to record right now.');
    if (failed) process.exitCode = 1;
}
 
// Only run when executed directly (so the helper functions can be tested by importing this file).
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
    main().catch((err) => {
        console.error(err.message || err);
        process.exit(1);
    });
}
 
Claude finished the response
