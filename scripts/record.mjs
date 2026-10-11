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
const
