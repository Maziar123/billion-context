// Storage conservation on FRESH disposable stores (audit A2/A5/A6): every case writes through the
// real ingest planner into a real sqlite `index.db` under a fresh mkdtemp dir and reads it back
// through the real `StorageBackend` contract — never live stores, mocks or table-count echoes.
// Historical full-node drift totals are field observations, NOT deletion proof.
//
// Lanes: sync = `UnifiedShadow.maybeOpen`; async = `UnifiedShadow.fromBackend` over a REAL
// `SqliteBackend` (the #5412 queue twin). Neither sleeps. The (namespace, conversation) binding
// is an IDENTITY map, NOT a writer lease.

import { test } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { UnifiedShadow } from "../src/storage/shadow.ts";
import { openBackend, type StorageBackend } from "../src/storage/backend.ts";
import { canonicalDigest } from "../src/storage/canonical.ts";
import { planIngest, type LegacySessionLike } from "../src/storage/ingest.ts";
import type { CoreMessage } from "acp-kernel";

process.env.BILI_PERSIST_ZSTD = "0";
// Hermeticity pin (#4323/#5408/#5412 family): a developer machine's real config tier must not leak in.
const HERMETIC = fs.mkdtempSync(path.join(os.tmpdir(), "bili-conservation-hermetic-"));
process.env.BILI_CONFIG_FILE = path.join(HERMETIC, "no-config.json");
delete process.env.BILI_STORAGE_UNIFIED;

function rmBestEffort(p: string): void {
    try {
        fs.rmSync(p, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    } catch {
        /* a leaked temp dir on a Windows runner is harmless */
    }
}
const tmpDir = (tag: string): string => fs.mkdtempSync(path.join(HERMETIC, `${tag}-`));
process.on("exit", () => rmBestEffort(HERMETIC));
// ---- fixtures ---------------------------------------------------------------
interface BlockSpec { blockId: string; effectiveMessageIds: string[]; }
const msg = (i: number, text = `body ${i}`): CoreMessage => ({ id: `c_msg${i}`, role: i % 2 === 0 ? "user" : "assistant", contentType: "text", text }) as unknown as CoreMessage;

/** Legacy-shaped session with fold blocks covering a prefix. Refs are minted the way the
 *  kernel mints them: `mNNNNN` in tail order. */
function session(id: string, tail: CoreMessage[], blocks: BlockSpec[] = [], folded = false): LegacySessionLike {
    const refs = tail.map((m, i) => [`m${String(i + 1).padStart(5, "0")}`, m.id] as const);
    return {
        id,
        createdAt: 1_000,
        lastSeen: 2_000,
        meta: { protocol: "anthropic" },
        stats: { requests: 1 },
        metadata: {},
        state: {
            blocks: blocks.map((b, idx) => ({
                blockId: b.blockId, runId: idx + 1, createdAt: 1_000 + idx, tier: 1, active: true,
                summary: `summary ${b.blockId}`, kind: "fold", reason: "conservation",
                effectiveMessageIds: b.effectiveMessageIds, directMessageIds: [],
            })),
            messageRefs: { byRaw: Object.fromEntries(refs.map(([ref, mid]) => [mid, ref])), byRef: Object.fromEntries(refs) },
            nextBlockId: blocks.length + 1,
            nextRunId: blocks.length + 1,
        },
        blockContents: new Map(),
        lastMessages: tail,
        lastMessagesFolded: folded,
    } as unknown as LegacySessionLike;
}

const asLegacy = (record: unknown): LegacySessionLike => record as LegacySessionLike;
const baseTail = (n = 6): CoreMessage[] => Array.from({ length: n }, (_, i) => msg(i));
const FOLD: BlockSpec[] = [{ blockId: "b1", effectiveMessageIds: ["c_msg0", "c_msg1"] }];
/** Logical view + row identities, read through the backend contract only. */
async function snapshot(be: StorageBackend, id: string) {
    return be.readSnapshot(async (r) => {
        const row = await r.getSessionRow(id);
        assert.ok(row, `session row must exist for ${id}`);
        const [messages, blocks, refs] = [await r.getMessages(id), await r.getBlocks(id, { limit: 10_000 }), await r.getRefs(id)];
        // STRUCTURED rows, not encoded strings: the canonical digest can be equal for views whose
        // field-level shape differs, so conservation is asserted field by field below.
        return {
            digest: canonicalDigest({ row, messages, blocks, refs }),
            messages: messages
                .map((m) => ({ seq: m.seq, msgId: m.msgId, ref: m.ref, contentHash: m.contentHash, retiredAt: m.retiredAt }))
                .sort((a, b) => a.seq - b.seq),
            refs: refs.map((x) => ({ ord: x.ord, rawId: x.rawId, ref: x.ref })),
            blocks: blocks.map((b) => ({ blockId: b.blockId, ord: b.ord, summaryHash: b.summaryHash, active: b.active })),
            tail: (JSON.parse(row.tailJson) as { tailSeqs?: number[] }).tailSeqs ?? [],
        };
    });
}

const asyncBackend = (dir: string): Promise<StorageBackend> => openBackend("sqlite", path.join(dir, "index.db"));

/** Sync settles inside `record()`; async discards its enqueue promise, so the queue tail is the
 *  real "this save ran" edge — awaited instead of a sleep. `closeAsync()` drains AND closes. */
type LaneName = "sync" | "async";
interface Lane {
    shadow: UnifiedShadow;
    save(record: LegacySessionLike, rawTail?: { lastMessages: CoreMessage[]; lastMessagesFolded?: boolean }): Promise<void>;
    finish(): Promise<void>;
}
const LANES: readonly LaneName[] = ["sync", "async"];

function queueSettled(s: UnifiedShadow): Promise<void> {
    const tail: unknown = (s as unknown as { queueTail?: unknown }).queueTail;
    if (!(tail instanceof Promise)) throw new Error("UnifiedShadow no longer exposes queueTail");
    return tail as Promise<void>;
}

async function openLane(name: LaneName, dir: string): Promise<Lane> {
    if (name === "sync") {
        const shadow = UnifiedShadow.maybeOpen(dir, { BILI_STORAGE_UNIFIED: "shadow" }, asLegacy);
        assert.ok(shadow, "sync shadow must open on a fresh dir");
        return { shadow, save: async (record, rawTail) => void shadow.record(record, rawTail), finish: async () => shadow.close() };
    }
    const shadow = UnifiedShadow.fromBackend("shadow", await asyncBackend(dir), asLegacy);
    return { shadow, save: async (record, rawTail) => (shadow.record(record, rawTail), await queueSettled(shadow)), finish: async () => shadow.closeAsync() };
}

/** Open index.db, run one read through the backend contract, close. */
async function withBackend<T>(dir: string, read: (be: StorageBackend) => Promise<T>): Promise<T> {
    const be = await asyncBackend(dir);
    try {
        return await read(be);
    } finally {
        await be.close();
    }
}
const counters = (s: UnifiedShadow): { verifySamples: number; verifyOk: number; verifyDrift: number } => s.stats;
/** The drift latch: `readonly driftedSessions`, which the counters only summarise (#5409/#5412). */
const latched = (s: UnifiedShadow): Set<string> => s.driftedSessions;
// ---- 4. two real processes, distinct sessions, one local index.db -----------
/** Re-executed child role: a REAL shadow write in its own process — two sqlite clients.
 *  It OPENS immediately (concurrent open is the shape under test — never serialized) and
 *  parks on stdin until the parent releases it, so both writers are live at once. */
const ROLE_FLAG = "--role=lane-writer";
const ROLE_AT = process.argv.findIndex((a) => a === ROLE_FLAG);
if (ROLE_AT >= 0) {
    const [dir, lane] = [process.argv[ROLE_AT + 1], process.argv[ROLE_AT + 2]];
    let shadow: UnifiedShadow | null = null;
    const report = (value: { ok: boolean; lane: string }): Promise<void> =>
        new Promise((resolve) => process.stdout.write(`${JSON.stringify(value)}\n`, () => resolve()));
    try {
        shadow = UnifiedShadow.maybeOpen(dir, { BILI_STORAGE_UNIFIED: "shadow" }, asLegacy);
        if (!shadow) throw new Error("shadow did not open");
        // Barrier: announce that the store is OPEN, then wait for the parent's GO so both
        // processes write while both handles are live (no retry, no sleep, no serialization).
        process.stdout.write(`READY ${lane}\n`);
        await new Promise<void>((resolve) => process.stdin.once("data", () => resolve()));
        shadow.record(session(`sess-${lane}`, baseTail(4), [{ blockId: `blk-${lane}`, effectiveMessageIds: ["c_msg0"] }]));
        // `record()` swallows ingest failures into stats; the exit code must not.
        if (shadow.stats.lastError !== null) throw new Error(String(shadow.stats.lastError));
    } catch (e) {
        process.stderr.write(`lane-writer ${lane} failed: ${(e as Error).stack ?? String(e)}\n`);
        process.exit(7);
    }
    // Close BEFORE reporting: the async path is fire-and-forget, so the report is only truthful once drained.
    shadow?.close();
    await report({ ok: true, lane });
    process.exit(0);
}
// ---- 1. conservation across close/reopen, both lanes ------------------------
for (const lane of LANES) {
    test(`conservation(${lane}): save → close → reopen preserves ids, hashes, refs and blocks`, async () => {
        const dir = tmpDir(`cons-${lane}`);
        const id = `cons-${lane}`;
        try {
            const configured = await openLane(lane, dir);
            await configured.save(session(id, baseTail(), FOLD));
            const c = counters(configured.shadow);
            assert.equal(c.verifyDrift, 0, "a clean write must not drift");
            assert.equal(c.verifyOk, 1, "the first ingest is sampled and clean");
            await configured.finish();

            // Reopen from disk: no in-memory state may carry the answer.
            const { payload, ids, after } = await withBackend(dir, async (be) => {
                const [blocks, ids, after] = [await be.getBlocks(id, { limit: 10 }), await be.listSessionIds(), await snapshot(be, id)];
                return { payload: JSON.parse(blocks[0].payloadJson) as { effectiveMessageIds: string[] }, ids, after };
            });
            // A genuinely independent second open, after the first one closed.
            const second = await withBackend(dir, (be) => snapshot(be, id));
            assert.deepEqual(ids, [id], "the reopen lists exactly the session that was saved");
            assert.deepEqual(payload.effectiveMessageIds, ["c_msg0", "c_msg1"], "the fold block keeps the ids it covers");
            // Field-for-field equality across two independent opens, plus exact identities/hashes/refs.
            assert.deepEqual(second, after, "an independent reopen reads the identical canonical view, row for row");
            assert.equal(after.messages.length, baseTail().length, "every message row is conserved");
            assert.deepEqual(after.messages.map((m) => m.msgId), baseTail().map((m) => m.id), "identities are conserved in tail order");
            assert.ok(after.messages.every((m) => m.ref !== null && m.contentHash.length === 64 && m.retiredAt === null), "each row keeps a ref, a full hash, and stays live");
            assert.deepEqual(after.refs.map((r) => r.rawId), baseTail().map((m) => m.id), "the ref ledger points at exactly the conserved identities");
            assert.deepEqual(after.blocks.map((b) => b.blockId), ["b1"], "block rows are conserved");
            assert.equal(after.tail.length, after.messages.length, "the tail pointer covers every conserved message");
        } finally {
            rmBestEffort(dir);
        }
    });
}
// ---- 2. a governed rewind leaves the counters clean -------------------------
for (const lane of LANES) {
    test(`rewind(${lane}): a governed rewind verifies clean and only retires the dropped tail row`, async () => {
        const dir = tmpDir(`rewind-${lane}`);
        const id = `rewind-${lane}`;
        const tail6 = baseTail(6);
        const rewound = tail6.slice(0, 5);
        try {
            const configured = await openLane(lane, dir);
            // Only a complete log may retire rows, and `record()` sets `completeLog` from a nonempty raw tail.
            await configured.save(session(id, tail6, FOLD), { lastMessages: tail6 });
            await configured.save(session(id, rewound, FOLD), { lastMessages: rewound });
            // Sampling is per-session ingest n = 1, 21, 41, … — two saves sample ONCE, and it must be clean.
            const { verifySamples, verifyOk, verifyDrift } = counters(configured.shadow);
            assert.deepEqual({ verifySamples, verifyOk, verifyDrift }, { verifySamples: 1, verifyOk: 1, verifyDrift: 0 }, "a governed rewind reaches one sampled ingest and verifies clean");
            assert.equal(latched(configured.shadow).has(id), false, "and it must not latch the session");
            await configured.finish();

            const { live, retired } = await withBackend(dir, async (be) => ({
                live: (await be.getMessages(id)).map((m) => m.msgId),
                retired: (await be.getMessages(id, { includeRetired: true })).filter((m) => m.retiredAt !== null).map((m) => m.msgId),
            }));
            assert.deepEqual(live, ["c_msg0", "c_msg1", "c_msg2", "c_msg3", "c_msg4"], "the live view follows the governed rewind");
            assert.deepEqual(retired, ["c_msg5"], "the rewound-out row is RETIRED, never deleted");
        } finally {
            rmBestEffort(dir);
        }
    });
}
interface Writer { lane: string; child: ChildProcess; ready: Promise<void>; done: Promise<{ ok: boolean; lane: string }> }

/** Spawn one child, capturing stdout/stderr so ANY failure is reported verbatim. Only the real
 *  loader/preload flags are forwarded — the inherited `--test-*`/`--inspect-*` noise is dropped. */
function spawnWriter(dir: string, lane: string): Writer {
    // Only loader/preload flags survive: inherited --test-*/--inspect-* noise must not reach the child.
    const loaderArgs: string[] = [];
    for (let i = 0; i < process.execArgv.length; i += 1) {
        const flag = process.execArgv[i];
        if (flag.startsWith("--import=") || flag.startsWith("--loader=") || flag.startsWith("--require=")) loaderArgs.push(flag);
        else if (flag === "--import" || flag === "--loader" || flag === "--require") loaderArgs.push(flag, process.execArgv[(i += 1)]);
    }
    const child = spawn(process.execPath, [...loaderArgs, fileURLToPath(import.meta.url), ROLE_FLAG, dir, lane], { stdio: ["pipe", "pipe", "pipe"] });
    let out = "";
    let err = "";
    const ready = Promise.withResolvers<void>();
    child.stdout.on("data", (b: Buffer) => {
        out += b.toString();
        if (out.includes(`READY ${lane}`)) ready.resolve();
    });
    child.stderr.on("data", (b: Buffer) => (err += b.toString()));
    const done = Promise.withResolvers<{ ok: boolean; lane: string }>();
    const fail = (why: string): void => {
        const error = new Error(`lane-writer ${lane} ${why}\nstdout: ${out}\nstderr: ${err}`);
        ready.reject(error);
        done.reject(error);
    };
    child.on("error", (e) => {
        ready.reject(e);
        fail(`spawn error (${e.message})`);
    });
    child.on("close", (code) => {
        if (code !== 0) return fail(`exited ${code}`);
        const line = out.split("\n").findLast((l) => l.trim().startsWith("{"));
        if (!line) return fail("reported nothing");
        const report = JSON.parse(line) as { ok: boolean; lane: string };
        if (!report.ok || report.lane !== lane) return fail(`reported ${line}`);
        done.resolve(report);
    });
    return { lane, child, ready: ready.promise, done: done.promise };
}

test("lanes: two real processes sharing one local index.db keep independent committed sessions", async () => {
    const dir = tmpDir("lanes");
    try {
        const lanes = ["lane-alpha", "lane-beta"];
        const writers = lanes.map((lane) => spawnWriter(dir, lane));
        const deadline = setTimeout(() => { for (const w of writers) w.child.kill("SIGKILL"); }, 20_000);
        try {
            const [, results] = await Promise.all([
                Promise.all(writers.map((w) => w.ready)).then(() => {
                    for (const w of writers) w.child.stdin?.write("GO\n");
                }),
                Promise.all(writers.map((w) => w.done)),
            ]);
            assert.deepEqual(results.map((r) => r.lane).sort(), lanes);
        } finally {
            clearTimeout(deadline);
            for (const w of writers) if (w.child.exitCode === null) w.child.kill("SIGKILL");
            await Promise.allSettled(writers.map((w) => w.done));
        }

        const { ids, snaps } = await withBackend(dir, async (be) => ({
            ids: await be.listSessionIds(),
            snaps: await Promise.all(lanes.map((lane) => snapshot(be, `sess-${lane}`))),
        }));
        assert.deepEqual(ids, ["sess-lane-alpha", "sess-lane-beta"], "each lane's session keeps its own row — no cross-lane overwrite");
        const shape = snaps.map((s) => ({ msgs: s.messages.length, refs: s.refs.length, blocks: s.blocks.map((b) => b.blockId) }));
        assert.deepEqual(
            shape,
            lanes.map((lane) => ({ msgs: 4, refs: 4, blocks: [`blk-${lane}`] })),
            "each lane keeps every identity and ref, and only its own block",
        );
        assert.notEqual(snaps[0].digest, snaps[1].digest, "distinct lanes must not collapse into one view");
    } finally {
        rmBestEffort(dir);
    }
});
