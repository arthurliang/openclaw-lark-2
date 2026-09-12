"use strict";
/**
 * Copyright (c) 2026 ByteDance Ltd. and/or its affiliates
 * SPDX-License-Identifier: MIT
 *
 * Streaming card controller for the Lark/Feishu channel plugin.
 *
 * Manages the full lifecycle of a streaming CardKit card:
 * idle → creating → streaming → completed / aborted / terminated.
 *
 * Delegates throttling to FlushController and message-unavailable
 * detection to UnavailableGuard.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.StreamingCardController = void 0;
exports.prepareTerminalCardContent = prepareTerminalCardContent;
const promises_1 = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const agent_runtime_1 = require("openclaw/plugin-sdk/agent-runtime");
const reply_runtime_1 = require("openclaw/plugin-sdk/reply-runtime");
const api_error_1 = require("../core/api-error.js");
const lark_logger_1 = require("../core/lark-logger.js");
const lark_client_1 = require("../core/lark-client.js");
const shutdown_hooks_1 = require("../core/shutdown-hooks.js");
const send_1 = require("../messaging/outbound/send.js");
const builder_1 = require("./builder.js");
const card_error_1 = require("./card-error.js");
const cardkit_1 = require("./cardkit.js");
const flush_controller_1 = require("./flush-controller.js");
const image_resolver_1 = require("./image-resolver.js");
const markdown_style_1 = require("./markdown-style.js");
const tool_use_display_1 = require("./tool-use-display.js");
const tool_use_trace_store_1 = require("./tool-use-trace-store.js");
const reply_dispatcher_types_1 = require("./reply-dispatcher-types.js");
const unavailable_guard_1 = require("./unavailable-guard.js");
const log = (0, lark_logger_1.larkLogger)('card/streaming');
// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
/**
 * 300309 续流次数上限。
 *
 * 飞书 CardKit 流式会话有服务端时限（实测约 10 分钟）。
 * 每次续流新建一张卡片，若账户级限制导致持续 300309，
 * 无上限会无限新建卡片。达到上限后禁用 CardKit 流式，
 * 让 onIdle 走既有终态路径（含 300305 拆分兜底）收尾。
 */
const MAX_CARD_CONTINUATIONS = 3;
/**
 * 流式卡片元素预算 —— 从源头避免撞 300305「element exceeds the limit」。
 *
 * 取值依据（⚠️ 不确定性如实标注）：
 * - 飞书**未公布元素级上限**，无法给出权威口径；
 * - 唯一可用的实测数据来自静态终态卡：30000 字符
 *   （见 FEISHU_TERMINAL_TEXT_CHUNK_TARGET，2026-09 实测安全）；
 * - 流式正文与终态正文同为单个 markdown 元素，故按同一口径保守外推，
 *   再留约 20% 余量 → 24000 字符；
 * - `maxElements`（Markdown 块数）与 `maxBytes`（UTF-8 字节）为二次预判，
 *   飞书同样未公布，属经验值。
 *
 * 该预算可在运行时通过 deps.elementBudget 覆盖（可配置），便于按实测调整。
 * 真实上限未知：若实测仍出现 300305，需下调；若过于保守可上调。
 */
const STREAMING_ELEMENT_BUDGET = {
    maxChars: 24000,
    // ≈ 24000 字符 × 2 字节（中英混排经验均值）
    maxBytes: 48000,
    // 单卡 Markdown 块数经验上限
    maxElements: 100,
};
// ---------------------------------------------------------------------------
// StreamingCardController
// ---------------------------------------------------------------------------
class StreamingCardController {
    // ---- Explicit state machine ----
    phase = 'idle';
    // ---- Structured state ----
    cardKit = {
        cardKitCardId: null,
        originalCardKitCardId: null,
        cardKitSequence: 0,
        cardMessageId: null,
        continuationCount: 0,
        /** 已熔断的卡片（收到过 300305）— 此后禁止任何 CardKit 写入。 */
        frozenCardIds: new Set(),
        /** 已熔断卡片对应的 messageId — 流式路径禁止对它们再做 IM patch。 */
        frozenMessageIds: new Set(),
    };
    text = {
        accumulatedText: '',
        completedText: '',
        streamingPrefix: '',
        lastPartialText: '',
        lastFlushedText: '',
        /** 已展示过的正文前缀长度（当前窗口在 accumulatedText 中的起始下标）。 */
        streamingOffsetChars: 0,
        /** 上一次成功推送的窗口长度（收到 300305 时据此推进窗口起点）。 */
        lastPushedWindowLen: 0,
    };
    reasoning = {
        accumulatedReasoningText: '',
        reasoningStartTime: null,
        reasoningElapsedMs: 0,
        isReasoningPhase: false,
    };
    toolUse = {
        startedAt: null,
        elapsedMs: 0,
        isActive: false,
    };
    // ---- Sub-controllers ----
    flush;
    guard;
    imageResolver;
    // ---- Lifecycle ----
    createEpoch = 0;
    _terminalReason = null;
    dispatchFullyComplete = false;
    cardCreationPromise = null;
    disposeShutdownHook = null;
    dispatchStartTime = Date.now();
    // ---- Injected dependencies ----
    deps;
    elapsed() {
        return Date.now() - this.dispatchStartTime;
    }
    needsFooterMetrics() {
        const footer = this.deps.resolvedFooter;
        return footer.tokens || footer.cache || footer.context || footer.model;
    }
    async getFooterSessionMetrics() {
        try {
            const runtime = lark_client_1.LarkClient.runtime;
            if (!runtime)
                return undefined;
            // OpenClaw 2.0: per-session usage metrics live in the agent
            // transcript SQLite (transcript_events.message.usage), not the
            // legacy sessions.json file that 2.0 migrated away.
            const agentId = this.deps.agentId;
            const sessionKey = this.deps.sessionKey.trim().toLowerCase();
            const dbPath = path.join(os.homedir(), '.openclaw', 'agents', agentId, 'agent', 'openclaw-agent.sqlite');
            const { DatabaseSync } = require('node:sqlite');
            const db = new DatabaseSync(dbPath, { readOnly: true });
            try {
                const window = db.prepare('SELECT session_id FROM session_windows WHERE lower(session_key) = ? ORDER BY updated_at DESC LIMIT 1').get(sessionKey);
                if (!window)
                    return undefined;
                const row = db.prepare("SELECT event_json FROM transcript_events WHERE session_id = ? AND event_json LIKE '%usage%' ORDER BY rowid DESC LIMIT 1").get(window.session_id);
                if (!row)
                    return undefined;
                const ev = JSON.parse(row.event_json);
                const msg = ev?.message ?? {};
                const u = msg.usage;
                if (!u)
                    return undefined;
                const metrics = {
                    inputTokens: typeof u.input === 'number' ? u.input : undefined,
                    outputTokens: typeof u.output === 'number' ? u.output : undefined,
                    cacheRead: typeof u.cacheRead === 'number' ? u.cacheRead : undefined,
                    cacheWrite: typeof u.cacheWrite === 'number' ? u.cacheWrite : undefined,
                    totalTokens: typeof u.totalTokens === 'number' ? u.totalTokens : undefined,
                    model: typeof msg.model === 'string' ? msg.model : undefined,
                    provider: typeof msg.provider === 'string' ? msg.provider : undefined,
                    agentId,
                };
                // Best-effort context window from the model catalog in cfg.
                const ctxWindow = this.resolveContextWindow(msg.provider, msg.model);
                if (ctxWindow != null)
                    metrics.contextTokens = ctxWindow;
                log.debug('footer metrics lookup: found usage from agent transcript sqlite', {
                    sessionKey: this.deps.sessionKey,
                    agentId,
                });
                return metrics;
            }
            finally {
                db.close();
            }
        }
        catch (err) {
            log.warn('footer metrics lookup failed', { error: String(err), sessionKey: this.deps.sessionKey });
            return undefined;
        }
    }
    /** Resolve a model's context window from cfg.models.providers. */
    resolveContextWindow(provider, model) {
        try {
            const providers = this.deps.cfg?.models?.providers ?? {};
            const pcfg = providers[provider];
            if (!pcfg || !Array.isArray(pcfg.models))
                return undefined;
            const found = pcfg.models.find((m) => m && m.id === model);
            return typeof found?.contextWindow === 'number' ? found.contextWindow : undefined;
        }
        catch {
            return undefined;
        }
    }
    constructor(deps) {
        this.deps = deps;
        this.guard = new unavailable_guard_1.UnavailableGuard({
            replyToMessageId: deps.replyToMessageId,
            getCardMessageId: () => this.cardKit.cardMessageId,
            onTerminate: () => {
                this.transition('terminated', 'UnavailableGuard', 'unavailable');
            },
        });
        this.flush = new flush_controller_1.FlushController(() => this.performFlush());
        this.imageResolver = new image_resolver_1.ImageResolver({
            cfg: deps.cfg,
            accountId: deps.accountId,
            onImageResolved: () => {
                if (!this.isTerminalPhase && this.cardKit.cardMessageId) {
                    void this.throttledCardUpdate();
                }
            },
        });
    }
    // ------------------------------------------------------------------
    // Public accessors
    // ------------------------------------------------------------------
    get cardMessageId() {
        return this.cardKit.cardMessageId;
    }
    get isTerminalPhase() {
        return reply_dispatcher_types_1.TERMINAL_PHASES.has(this.phase);
    }
    /**
     * Whether the card has been explicitly aborted (via abortCard()).
     *
     * Distinct from isTerminalPhase — creation_failed is NOT an abort;
     * it should allow fallthrough to static delivery in the factory.
     */
    get isAborted() {
        return this.phase === 'aborted';
    }
    /** Whether the reply pipeline was terminated due to an unavailable message. */
    get isTerminated() {
        return this.guard.isTerminated;
    }
    /** Check if the pipeline should skip further operations for this source. */
    shouldSkipForUnavailable(source) {
        return this.guard.shouldSkip(source);
    }
    /** Attempt to terminate the pipeline due to an unavailable message error. */
    terminateIfUnavailable(source, err) {
        return this.guard.terminate(source, err);
    }
    /** Why the controller entered a terminal phase, or null if still active. */
    get terminalReason() {
        return this._terminalReason;
    }
    /** @internal — exposed for test assertions only. */
    get currentPhase() {
        return this.phase;
    }
    get shouldDisplayToolUse() {
        return this.deps.toolUseDisplay.showToolUse;
    }
    /**
     * Activity-only mode (static/group replies): the controller drives a
     * lightweight tool-activity card only — text/reasoning streaming is
     * handled by the static deliver() path, so those callbacks are no-ops
     * and the card is removed once the final reply is delivered.
     */
    get activityOnly() {
        return this.deps.activityOnly === true;
    }
    computeToolUseDisplay() {
        if (!this.shouldDisplayToolUse)
            return null;
        const traceSteps = (0, tool_use_trace_store_1.getToolUseTraceSteps)(this.deps.sessionKey);
        return (0, tool_use_display_1.normalizeToolUseDisplay)({
            traceSteps,
            showFullPaths: this.deps.toolUseDisplay.showFullPaths,
            showResultDetails: this.deps.toolUseDisplay.showToolResultDetails,
        });
    }
    get visibleToolUseElapsedMs() {
        if (!this.shouldDisplayToolUse || !this.toolUse.startedAt) {
            return undefined;
        }
        return this.toolUse.elapsedMs || Date.now() - this.toolUse.startedAt;
    }
    computeToolUseTitleSuffix(display) {
        if (!this.shouldDisplayToolUse)
            return undefined;
        const stepCount = display?.stepCount ?? 0;
        return stepCount > 0 ? (0, tool_use_display_1.buildToolUseTitleSuffix)({ stepCount }) : undefined;
    }
    // ------------------------------------------------------------------
    // Unified callback guard
    // ------------------------------------------------------------------
    /**
     * Unified callback guard — returns true if the pipeline is active
     * and the callback should proceed.
     *
     * Combines three checks:
     * 1. guard.isTerminated — message recalled/deleted
     * 2. guard.shouldSkip(source) — eagerly detect unavailable messages
     * 3. isTerminalPhase — completed/aborted/terminated/creation_failed
     */
    shouldProceed(source) {
        if (this.guard.isTerminated || this.guard.shouldSkip(source))
            return false;
        return !this.isTerminalPhase;
    }
    // ------------------------------------------------------------------
    // State machine
    // ------------------------------------------------------------------
    isStaleCreate(epoch) {
        return epoch !== this.createEpoch;
    }
    transition(to, source, reason) {
        const from = this.phase;
        if (from === to)
            return false;
        if (!reply_dispatcher_types_1.PHASE_TRANSITIONS[from].has(to)) {
            log.warn('phase transition rejected', { from, to, source });
            return false;
        }
        this.phase = to;
        log.info('phase transition', { from, to, source, reason });
        if (reply_dispatcher_types_1.TERMINAL_PHASES.has(to)) {
            this._terminalReason = reason ?? null;
            this.onEnterTerminalPhase();
        }
        return true;
    }
    onEnterTerminalPhase() {
        this.createEpoch += 1;
        this.flush.cancelPendingFlush();
        this.flush.complete();
        this.disposeShutdownHook?.();
        this.disposeShutdownHook = null;
        if (this.phase === 'terminated' || this.phase === 'creation_failed') {
            (0, tool_use_trace_store_1.clearToolUseTraceRun)(this.deps.sessionKey);
        }
    }
    markToolUseActivity() {
        if (!this.toolUse.startedAt) {
            this.toolUse.startedAt = Date.now();
        }
        this.toolUse.elapsedMs = Date.now() - this.toolUse.startedAt;
        this.toolUse.isActive = true;
    }
    captureToolUseElapsed() {
        if (!this.toolUse.startedAt)
            return;
        this.toolUse.elapsedMs = Date.now() - this.toolUse.startedAt;
        this.toolUse.isActive = false;
    }
    // ------------------------------------------------------------------
    // SDK callback bindings
    // ------------------------------------------------------------------
    /**
     * Handle a deliver() call in streaming card mode.
     *
     * Accumulates text from the SDK's deliver callbacks to build the
     * authoritative "completedText" for the final card.
     */
    async onDeliver(payload) {
        if (!this.shouldProceed('onDeliver'))
            return;
        if (this.activityOnly)
            return;
        const text = payload.text ?? '';
        if (!text.trim())
            return;
        await this.ensureCardCreated();
        if (!this.shouldProceed('onDeliver.postCreate'))
            return;
        if (!this.cardKit.cardMessageId)
            return;
        this.captureToolUseElapsed();
        const split = (0, builder_1.splitReasoningText)(text);
        if (split.reasoningText && !split.answerText) {
            // Pure reasoning payload
            this.reasoning.reasoningElapsedMs = this.reasoning.reasoningStartTime
                ? Date.now() - this.reasoning.reasoningStartTime
                : 0;
            this.reasoning.accumulatedReasoningText = split.reasoningText;
            this.reasoning.isReasoningPhase = true;
            await this.throttledCardUpdate();
            return;
        }
        // Answer payload (may also contain inline reasoning from tags)
        this.reasoning.isReasoningPhase = false;
        if (split.reasoningText) {
            this.reasoning.accumulatedReasoningText = split.reasoningText;
        }
        const answerText = split.answerText ?? text;
        // 累积 deliver 文本用于最终卡片
        this.text.completedText += (this.text.completedText ? '\n\n' : '') + answerText;
        // 没有流式数据时，用 deliver 文本显示在卡片上
        if (!this.text.lastPartialText && !this.text.streamingPrefix) {
            this.text.accumulatedText += (this.text.accumulatedText ? '\n\n' : '') + answerText;
            this.text.streamingPrefix = this.text.accumulatedText;
            await this.throttledCardUpdate();
        }
    }
    async onReasoningStream(payload) {
        if (!this.shouldProceed('onReasoningStream'))
            return;
        if (this.activityOnly)
            return;
        await this.ensureCardCreated();
        if (!this.shouldProceed('onReasoningStream.postCreate'))
            return;
        if (!this.cardKit.cardMessageId)
            return;
        const rawText = payload.text ?? '';
        if (!rawText)
            return;
        if (!this.reasoning.reasoningStartTime) {
            this.reasoning.reasoningStartTime = Date.now();
        }
        this.reasoning.isReasoningPhase = true;
        const split = (0, builder_1.splitReasoningText)(rawText);
        this.reasoning.accumulatedReasoningText = split.reasoningText ?? rawText;
        await this.throttledCardUpdate();
    }
    async onToolStart(payload) {
        if (!this.shouldProceed('onToolStart'))
            return;
        if (!this.shouldDisplayToolUse)
            return;
        const phase = payload.phase ?? 'start';
        // 把工具生命周期写入 trace store，卡片才能渲染出"正在调用什么工具"的步骤。
        if (phase === 'start') {
            (0, tool_use_trace_store_1.recordToolUseStart)({
                sessionKey: this.deps.sessionKey,
                toolName: payload.name,
                toolParams: payload.args,
                toolCallId: payload.toolCallId,
            });
        }
        else if (phase === 'end' || phase === 'error' || phase === 'result') {
            (0, tool_use_trace_store_1.recordToolUseEnd)({
                sessionKey: this.deps.sessionKey,
                toolName: payload.name,
                toolParams: payload.args,
                toolCallId: payload.toolCallId,
                error: phase === 'error' ? 'tool failed' : undefined,
            });
        }
        else {
            return;
        }
        if (phase === 'start') {
            this.markToolUseActivity();
        }
        else {
            this.captureToolUseElapsed();
        }
        await this.ensureCardCreated();
        if (!this.shouldProceed('onToolStart.postCreate'))
            return;
        if (!this.cardKit.cardMessageId)
            return;
        if (this.activityOnly) {
            if (this.cardKit.cardKitCardId) {
                await this.throttledToolUseStatusUpdate();
            }
            else {
                await this.throttledCardUpdate();
            }
            return;
        }
        if (!this.text.accumulatedText && this.cardKit.cardKitCardId) {
            await this.throttledToolUseStatusUpdate();
            return;
        }
        await this.throttledCardUpdate();
    }
    async onToolPayload(_payload) {
        if (!this.shouldProceed('onToolPayload'))
            return;
        if (!this.shouldDisplayToolUse)
            return;
        this.markToolUseActivity();
        await this.ensureCardCreated();
        if (!this.shouldProceed('onToolPayload.postCreate'))
            return;
        if (!this.cardKit.cardMessageId)
            return;
        if (this.activityOnly) {
            if (this.cardKit.cardKitCardId) {
                await this.throttledToolUseStatusUpdate();
            }
            else {
                await this.throttledCardUpdate();
            }
            return;
        }
        if (!this.text.accumulatedText && this.cardKit.cardKitCardId) {
            await this.throttledToolUseStatusUpdate();
            return;
        }
        await this.throttledCardUpdate();
    }
    async onPartialReply(payload) {
        if (!this.shouldProceed('onPartialReply'))
            return;
        if (this.activityOnly)
            return;
        // Use splitReasoningText (consistent with onDeliver/onReasoningStream)
        // to extract <think> tag content before stripping it from the answer.
        // Previously only stripReasoningTags was called, silently discarding
        // any thinking content that the LLM wrapped in <think> tags.
        const rawText = payload.text ?? '';
        const split = (0, builder_1.splitReasoningText)(rawText);
        if (split.reasoningText) {
            if (!this.reasoning.reasoningStartTime) {
                this.reasoning.reasoningStartTime = Date.now();
            }
            this.reasoning.accumulatedReasoningText = split.reasoningText;
            this.reasoning.isReasoningPhase = true;
        }
        const text = split.answerText ?? (0, builder_1.stripReasoningTags)(rawText);
        log.debug('onPartialReply', { len: text.length });
        if (!text)
            return;
        this.captureToolUseElapsed();
        if (!this.reasoning.reasoningStartTime) {
            this.reasoning.reasoningStartTime = Date.now();
        }
        if (this.reasoning.isReasoningPhase) {
            this.reasoning.isReasoningPhase = false;
            this.reasoning.reasoningElapsedMs = this.reasoning.reasoningStartTime
                ? Date.now() - this.reasoning.reasoningStartTime
                : 0;
        }
        // 检测回复边界：文本长度缩短 → 新回复开始
        if (this.text.lastPartialText && text.length < this.text.lastPartialText.length) {
            this.text.streamingPrefix += (this.text.streamingPrefix ? '\n\n' : '') + this.text.lastPartialText;
        }
        this.text.lastPartialText = text;
        this.text.accumulatedText = this.text.streamingPrefix ? this.text.streamingPrefix + '\n\n' + text : text;
        // NO_REPLY 缓冲
        if (!this.text.streamingPrefix && reply_runtime_1.SILENT_REPLY_TOKEN.startsWith(this.text.accumulatedText.trim())) {
            log.debug('onPartialReply: buffering NO_REPLY prefix');
            return;
        }
        await this.ensureCardCreated();
        if (!this.shouldProceed('onPartialReply.postCreate'))
            return;
        if (!this.cardKit.cardMessageId)
            return;
        await this.throttledCardUpdate();
    }
    async onError(err, info) {
        if (this.guard.terminate('onError', err))
            return;
        log.error(`${info.kind} reply failed`, { error: String(err) });
        if (this.activityOnly) {
            await this.deleteActivityCard('onError');
            return;
        }
        this.captureToolUseElapsed();
        this.finalizeCard('onError', 'error');
        await this.flush.waitForFlush();
        if (this.cardCreationPromise)
            await this.cardCreationPromise;
        const errorEffectiveCardId = this.getWritableCardKitId();
        const footerMetrics = this.needsFooterMetrics() ? await this.getFooterSessionMetrics() : undefined;
        const toolUseDisplay = this.computeToolUseDisplay();
        try {
            if (this.cardKit.cardMessageId) {
                const rawErrorText = this.text.accumulatedText
                    ? `${this.text.accumulatedText}\n\n---\n**Error**: An error occurred while generating the response.`
                    : '**Error**: An error occurred while generating the response.';
                const terminalContent = prepareTerminalCardContent({
                    text: rawErrorText,
                    reasoningText: this.reasoning.accumulatedReasoningText || undefined,
                }, this.imageResolver);
                const errorCard = (0, builder_1.buildCardContent)('complete', {
                    text: terminalContent.text,
                    reasoningText: terminalContent.reasoningText,
                    reasoningElapsedMs: this.reasoning.reasoningElapsedMs || undefined,
                    toolUseSteps: toolUseDisplay?.steps,
                    toolUseTitleSuffix: this.computeToolUseTitleSuffix(toolUseDisplay),
                    toolUseElapsedMs: this.visibleToolUseElapsedMs,
                    showToolUse: this.deps.toolUseDisplay.showToolUse,
                    elapsedMs: this.elapsed(),
                    isError: true,
                    footer: this.deps.resolvedFooter,
                    footerMetrics,
                });
                if (errorEffectiveCardId) {
                    await this.closeStreamingAndUpdate(errorEffectiveCardId, errorCard, 'onError');
                }
                else {
                    await (0, send_1.updateCardFeishu)({
                        cfg: this.deps.cfg,
                        messageId: this.cardKit.cardMessageId,
                        card: errorCard,
                        accountId: this.deps.accountId,
                    });
                }
            }
        }
        catch {
            // Ignore update failures during error handling
        }
        finally {
            (0, tool_use_trace_store_1.clearToolUseTraceRun)(this.deps.sessionKey);
        }
    }
    async onIdle() {
        if (this.guard.isTerminated || this.guard.shouldSkip('onIdle'))
            return;
        if (!this.dispatchFullyComplete)
            return;
        if (this.isTerminalPhase)
            return;
        this.captureToolUseElapsed();
        if (this.activityOnly) {
            // 静态模式：最终回复已通过 deliver() 单独发送，删除活动卡即可。
            await this.deleteActivityCard('onIdle');
            return;
        }
        this.finalizeCard('onIdle', 'normal');
        await this.flush.waitForFlush();
        if (this.cardCreationPromise) {
            await this.cardCreationPromise;
            await new Promise((resolve) => setTimeout(resolve, 0));
            await this.flush.waitForFlush();
        }
        const idleEffectiveCardId = this.getWritableCardKitId();
        try {
            if (this.cardKit.cardMessageId) {
                if (idleEffectiveCardId) {
                    const seqBeforeClose = this.cardKit.cardKitSequence;
                    this.cardKit.cardKitSequence += 1;
                    log.info('onIdle: closing streaming mode', {
                        seqBefore: seqBeforeClose,
                        seqAfter: this.cardKit.cardKitSequence,
                    });
                    await (0, cardkit_1.setCardStreamingMode)({
                        cfg: this.deps.cfg,
                        cardId: idleEffectiveCardId,
                        streamingMode: false,
                        sequence: this.cardKit.cardKitSequence,
                        accountId: this.deps.accountId,
                    });
                }
                const isNoReplyLeak = !this.text.completedText && reply_runtime_1.SILENT_REPLY_TOKEN.startsWith(this.text.accumulatedText.trim());
                const displayText = this.text.completedText || (isNoReplyLeak ? '' : this.text.accumulatedText) || reply_dispatcher_types_1.EMPTY_REPLY_FALLBACK_TEXT;
                if (!this.text.completedText && !this.text.accumulatedText) {
                    log.warn('reply completed without visible text, using empty-reply fallback');
                }
                // 等待图片异步解析（最多 15s），避免终态卡片留占位符
                const resolvedDisplayText = await this.imageResolver.resolveImagesAwait(displayText, 15_000);
                const idleToolUseDisplay = this.computeToolUseDisplay();
                const terminalContent = prepareTerminalCardContent({
                    text: resolvedDisplayText,
                    reasoningText: this.reasoning.accumulatedReasoningText || undefined,
                }, this.imageResolver);
                const footerMetrics = this.needsFooterMetrics() ? await this.getFooterSessionMetrics() : undefined;
                const completeCard = (0, builder_1.buildCardContent)('complete', {
                    text: terminalContent.text,
                    reasoningText: terminalContent.reasoningText,
                    reasoningElapsedMs: this.reasoning.reasoningElapsedMs || undefined,
                    toolUseSteps: idleToolUseDisplay?.steps,
                    toolUseTitleSuffix: this.computeToolUseTitleSuffix(idleToolUseDisplay),
                    toolUseElapsedMs: this.visibleToolUseElapsedMs,
                    showToolUse: this.deps.toolUseDisplay.showToolUse,
                    elapsedMs: this.elapsed(),
                    footer: this.deps.resolvedFooter,
                    footerMetrics,
                });
                if (idleEffectiveCardId) {
                    const seqBeforeUpdate = this.cardKit.cardKitSequence;
                    this.cardKit.cardKitSequence += 1;
                    log.info('onIdle: updating final card', {
                        seqBefore: seqBeforeUpdate,
                        seqAfter: this.cardKit.cardKitSequence,
                    });
                    try {
                        await (0, cardkit_1.updateCardKitCard)({
                            cfg: this.deps.cfg,
                            cardId: idleEffectiveCardId,
                            card: (0, builder_1.toCardKit2)(completeCard),
                            sequence: this.cardKit.cardKitSequence,
                            accountId: this.deps.accountId,
                        });
                    }
                    catch (updateErr) {
                        // 300305 元素超限 — 拆分正文为多段，通过 IM patch 逐段发送
                        if ((0, card_error_1.isCardElementExceedsError)(updateErr)) {
                            log.warn('onIdle: final card update hit 300305, splitting and retrying via IM patch', {
                                cardId: idleEffectiveCardId,
                                textLen: terminalContent.text.length,
                            });
                            await this.sendTerminalContentSplit(terminalContent, idleToolUseDisplay, footerMetrics);
                        }
                        else {
                            throw updateErr;
                        }
                    }
                }
                else {
                    try {
                        await (0, send_1.updateCardFeishu)({
                            cfg: this.deps.cfg,
                            messageId: this.cardKit.cardMessageId,
                            card: completeCard,
                            accountId: this.deps.accountId,
                        });
                    }
                    catch (patchErr) {
                        // 300305 元素超限 — 拆分正文为多段 IM patch
                        if ((0, card_error_1.isCardElementExceedsError)(patchErr)) {
                            log.warn('onIdle: IM patch hit 300305, splitting and retrying', {
                                messageId: this.cardKit.cardMessageId,
                                textLen: terminalContent.text.length,
                            });
                            await this.sendTerminalContentSplit(terminalContent, idleToolUseDisplay, footerMetrics);
                        }
                        else {
                            throw patchErr;
                        }
                    }
                }
                log.info('reply completed, card finalized', {
                    elapsedMs: this.elapsed(),
                    isCardKit: !!idleEffectiveCardId,
                });
            }
        }
        catch (err) {
            log.warn('final card update failed', { error: String(err) });
        }
        finally {
            (0, tool_use_trace_store_1.clearToolUseTraceRun)(this.deps.sessionKey);
        }
    }
    // ------------------------------------------------------------------
    // External control
    // ------------------------------------------------------------------
    markFullyComplete() {
        log.debug('markFullyComplete', {
            completedTextLen: this.text.completedText.length,
            accumulatedTextLen: this.text.accumulatedText.length,
        });
        this.dispatchFullyComplete = true;
    }
    /**
     * Activity-only mode terminal: remove the tool-activity card.
     *
     * The final reply is delivered as a separate static message, so the
     * ephemeral activity card must be deleted rather than finalized into
     * the answer. Failure to delete (e.g. permission) is non-fatal.
     */
    async deleteActivityCard(source) {
        try {
            if (this.cardKit.cardMessageId) {
                await (0, send_1.deleteMessageFeishu)({
                    cfg: this.deps.cfg,
                    messageId: this.cardKit.cardMessageId,
                    accountId: this.deps.accountId,
                });
                log.info('activity card removed', { source, messageId: this.cardKit.cardMessageId });
            }
        }
        catch (err) {
            log.warn('activity card delete failed', { source, error: String(err) });
        }
        finally {
            this.transition('completed', 'deleteActivityCard', source);
            (0, tool_use_trace_store_1.clearToolUseTraceRun)(this.deps.sessionKey);
        }
    }
    async abortCard() {
        try {
            if (this.activityOnly) {
                await this.deleteActivityCard('abortCard');
                return;
            }
            this.captureToolUseElapsed();
            if (!this.transition('aborted', 'abortCard', 'abort'))
                return;
            // transition() already executed onEnterTerminalPhase (cancel + complete + dispose hook)
            // Only need to wait for any in-flight flush to finish
            await this.flush.waitForFlush();
            if (this.cardCreationPromise)
                await this.cardCreationPromise;
            const effectiveCardId = this.getWritableCardKitId();
            const elapsedMs = Date.now() - this.dispatchStartTime;
            const abortToolUseDisplay = this.computeToolUseDisplay();
            const terminalContent = prepareTerminalCardContent({
                text: this.text.accumulatedText || 'Aborted.',
                reasoningText: this.reasoning.accumulatedReasoningText || undefined,
            }, this.imageResolver);
            const footerMetrics = this.needsFooterMetrics() ? await this.getFooterSessionMetrics() : undefined;
            if (effectiveCardId) {
                const abortCardContent = (0, builder_1.buildCardContent)('complete', {
                    text: terminalContent.text,
                    reasoningText: terminalContent.reasoningText,
                    reasoningElapsedMs: this.reasoning.reasoningElapsedMs || undefined,
                    toolUseSteps: abortToolUseDisplay?.steps,
                    toolUseTitleSuffix: this.computeToolUseTitleSuffix(abortToolUseDisplay),
                    toolUseElapsedMs: this.visibleToolUseElapsedMs,
                    showToolUse: this.deps.toolUseDisplay.showToolUse,
                    elapsedMs,
                    isAborted: true,
                    footer: this.deps.resolvedFooter,
                    footerMetrics,
                });
                await this.closeStreamingAndUpdate(effectiveCardId, abortCardContent, 'abortCard');
                log.info('abortCard completed', { effectiveCardId });
            }
            else if (this.cardKit.cardMessageId) {
                // IM fallback: 卡片不是通过 CardKit 发的，用 im.message.patch 更新
                const abortCard = (0, builder_1.buildCardContent)('complete', {
                    text: terminalContent.text,
                    reasoningText: terminalContent.reasoningText,
                    reasoningElapsedMs: this.reasoning.reasoningElapsedMs || undefined,
                    toolUseSteps: abortToolUseDisplay?.steps,
                    toolUseTitleSuffix: this.computeToolUseTitleSuffix(abortToolUseDisplay),
                    toolUseElapsedMs: this.visibleToolUseElapsedMs,
                    showToolUse: this.deps.toolUseDisplay.showToolUse,
                    elapsedMs,
                    isAborted: true,
                    footer: this.deps.resolvedFooter,
                    footerMetrics,
                });
                await (0, send_1.updateCardFeishu)({
                    cfg: this.deps.cfg,
                    messageId: this.cardKit.cardMessageId,
                    card: abortCard,
                    accountId: this.deps.accountId,
                });
                log.info('abortCard completed (IM fallback)', {
                    messageId: this.cardKit.cardMessageId,
                });
            }
        }
        catch (err) {
            log.warn('abortCard failed', { error: String(err) });
        }
        finally {
            (0, tool_use_trace_store_1.clearToolUseTraceRun)(this.deps.sessionKey);
        }
    }
    // ------------------------------------------------------------------
    // Internal: card creation
    // ------------------------------------------------------------------
    async ensureCardCreated() {
        if (this.guard.shouldSkip('ensureCardCreated.precheck'))
            return;
        if (this.cardKit.cardMessageId || this.phase === 'creation_failed' || this.isTerminalPhase) {
            return;
        }
        if (this.cardCreationPromise) {
            await this.cardCreationPromise;
            return;
        }
        if (!this.transition('creating', 'ensureCardCreated'))
            return;
        this.createEpoch += 1;
        const epoch = this.createEpoch;
        this.cardCreationPromise = (async () => {
            try {
                try {
                    // Step 1: Create card entity
                    const cId = await (0, cardkit_1.createCardEntity)({
                        cfg: this.deps.cfg,
                        card: (0, builder_1.buildStreamingThinkingCard)(this.deps.toolUseDisplay.showToolUse),
                        accountId: this.deps.accountId,
                    });
                    if (this.isStaleCreate(epoch)) {
                        log.info('ensureCardCreated: stale epoch after createCardEntity, bailing out', {
                            epoch,
                            phase: this.phase,
                        });
                        return;
                    }
                    if (cId) {
                        this.cardKit.cardKitCardId = cId;
                        this.cardKit.originalCardKitCardId = cId;
                        this.cardKit.cardKitSequence = 1;
                        this.disposeShutdownHook = (0, shutdown_hooks_1.registerShutdownHook)(`streaming-card:${cId}`, () => this.abortCard());
                        log.info('created CardKit entity', {
                            cardId: cId,
                            initialSequence: this.cardKit.cardKitSequence,
                        });
                        // Step 2: Send IM message referencing card_id
                        const result = await (0, cardkit_1.sendCardByCardId)({
                            cfg: this.deps.cfg,
                            to: this.deps.chatId,
                            cardId: cId,
                            replyToMessageId: this.deps.replyToMessageId,
                            replyInThread: this.deps.replyInThread,
                            accountId: this.deps.accountId,
                        });
                        if (this.isStaleCreate(epoch)) {
                            log.info('ensureCardCreated: stale epoch after sendCardByCardId, bailing out', {
                                epoch,
                                phase: this.phase,
                            });
                            this.disposeShutdownHook?.();
                            this.disposeShutdownHook = null;
                            return;
                        }
                        this.cardKit.cardMessageId = result.messageId;
                        this.flush.setCardMessageReady(true);
                        if (!this.transition('streaming', 'ensureCardCreated.cardkit')) {
                            this.disposeShutdownHook?.();
                            this.disposeShutdownHook = null;
                            return;
                        }
                        log.info('sent CardKit card', { messageId: result.messageId });
                    }
                    else {
                        throw new Error('card.create returned empty card_id');
                    }
                }
                catch (cardKitErr) {
                    if (this.isStaleCreate(epoch))
                        return;
                    if (this.guard.terminate('ensureCardCreated.cardkitFlow', cardKitErr)) {
                        return;
                    }
                    // CardKit flow failed — fall back to regular IM card
                    const apiDetail = extractApiDetail(cardKitErr);
                    log.warn('CardKit flow failed, falling back to IM', { apiDetail });
                    this.cardKit.cardKitCardId = null;
                    this.cardKit.originalCardKitCardId = null;
                    const fallbackCard = (0, builder_1.buildCardContent)('streaming', {
                        showToolUse: this.deps.toolUseDisplay.showToolUse,
                    });
                    const result = await (0, send_1.sendCardFeishu)({
                        cfg: this.deps.cfg,
                        to: this.deps.chatId,
                        card: fallbackCard,
                        replyToMessageId: this.deps.replyToMessageId,
                        replyInThread: this.deps.replyInThread,
                        accountId: this.deps.accountId,
                    });
                    if (this.isStaleCreate(epoch)) {
                        log.info('ensureCardCreated: stale epoch after IM fallback send, bailing out', {
                            epoch,
                            phase: this.phase,
                        });
                        return;
                    }
                    this.cardKit.cardMessageId = result.messageId;
                    this.flush.setCardMessageReady(true);
                    if (!this.transition('streaming', 'ensureCardCreated.imFallback')) {
                        return;
                    }
                    log.info('sent fallback IM card', { messageId: result.messageId });
                }
            }
            catch (err) {
                if (this.isStaleCreate(epoch))
                    return;
                if (this.guard.terminate('ensureCardCreated.outer', err)) {
                    return;
                }
                log.warn('thinking card failed, falling back to static', {
                    error: String(err),
                });
                this.transition('creation_failed', 'ensureCardCreated.outer', 'creation_failed');
            }
        })();
        await this.cardCreationPromise;
    }
    // ------------------------------------------------------------------
    // Internal: flush
    // ------------------------------------------------------------------
    async performFlush() {
        if (!this.cardKit.cardMessageId || this.isTerminalPhase)
            return;
        // v2 CardKit 卡片不能走 IM patch，如果流式 CardKit 已禁用但 originalCardKitCardId
        // 仍在，说明卡片是通过 CardKit 发的——跳过中间态更新，等终态用 originalCardKitCardId 收尾
        if (!this.cardKit.cardKitCardId && this.cardKit.originalCardKitCardId) {
            log.debug('performFlush: skipping (CardKit streaming disabled, awaiting final update)');
            return;
        }
        log.debug('flushCardUpdate: enter', {
            seq: this.cardKit.cardKitSequence,
            isCardKit: !!this.cardKit.cardKitCardId,
        });
        try {
            if (this.cardKit.cardKitCardId) {
                await this.pushStreamingWindow();
                // 窗口已满：先另起一张卡承接后续正文，避免撞上元素上限
                if (this.isStreamingWindowFull())
                    await this.rolloverFilledChunk();
            }
            else {
                // 冻结卡的 message 不可再 patch —— 否则就是对同一张超限卡的原地重试
                if (this.isFrozenMessage(this.cardKit.cardMessageId)) {
                    log.debug('flushCardUpdate: skipping IM patch on a frozen card message');
                    return;
                }
                log.debug('flushCardUpdate: IM patch fallback');
                const resolvedText = this.imageResolver.resolveImages(this.buildDisplayText());
                const flushDisplay = this.computeToolUseDisplay();
                const card = (0, builder_1.buildCardContent)('streaming', {
                    text: this.reasoning.isReasoningPhase ? '' : resolvedText,
                    reasoningText: this.reasoning.isReasoningPhase ? this.reasoning.accumulatedReasoningText : undefined,
                    toolUseSteps: flushDisplay?.steps,
                    toolUseTitleSuffix: this.computeToolUseTitleSuffix(flushDisplay),
                    showToolUse: this.deps.toolUseDisplay.showToolUse,
                });
                await (0, send_1.updateCardFeishu)({
                    cfg: this.deps.cfg,
                    messageId: this.cardKit.cardMessageId,
                    card: card,
                    accountId: this.deps.accountId,
                });
            }
        }
        catch (err) {
            if (this.guard.terminate('flushCardUpdate', err))
                return;
            const apiCode = (0, api_error_1.extractLarkApiCode)(err);
            // 速率限制（230020）— 跳过此帧，不降级
            if ((0, card_error_1.isCardRateLimitError)(err)) {
                log.info('flushCardUpdate: rate limited (230020), skipping', {
                    seq: this.cardKit.cardKitSequence,
                });
                return;
            }
            // CardKit 流式会话已被服务端关闭（300309）— 新建卡片续流。
            // 同一 messageId 的卡片流已死，降级 im.message.patch 救不回，
            // 必须新建 CardKit 实体并发送新消息才能继续展示后续内容。
            if ((0, card_error_1.isCardStreamingClosedError)(err)) {
                log.warn('flushCardUpdate: streaming mode closed (300309), creating new card to continue', {
                    seq: this.cardKit.cardKitSequence,
                    cardId: this.cardKit.cardKitCardId,
                    continuationCount: this.cardKit.continuationCount,
                });
                await this.continueOnNewCard('300309');
                return;
            }
            // 元素超限（300305）— 该卡已不可写：立即熔断冻结（禁止原地重试），
            // 拆正文到新卡续流，避免在同一张超限卡上连续重试（生产事故 2026-09-12）。
            if ((0, card_error_1.isCardElementExceedsError)(err)) {
                await this.handleStreamingElementExceeds();
                return;
            }
            // 卡片表格数超出飞书限制（230099/11310）— 禁用 CardKit 流式，
            // 保留 originalCardKitCardId 供 onIdle 做最终 CardKit 更新
            if ((0, card_error_1.isCardTableLimitError)(err)) {
                log.warn('flushCardUpdate: card table limit exceeded (230099/11310), disabling CardKit streaming', {
                    seq: this.cardKit.cardKitSequence,
                });
                this.cardKit.cardKitCardId = null;
                return;
            }
            const apiDetail = extractApiDetail(err);
            log.error('card stream update failed', {
                apiCode,
                seq: this.cardKit.cardKitSequence,
                apiDetail,
            });
            if (this.cardKit.cardKitCardId) {
                log.warn('disabling CardKit streaming, falling back to im.message.patch');
                this.cardKit.cardKitCardId = null;
            }
        }
    }
    buildDisplayText() {
        const windowText = this.currentChunk().head;
        if (this.reasoning.isReasoningPhase && this.reasoning.accumulatedReasoningText) {
            const reasoningDisplay = `💭 **Thinking...**\n\n${this.reasoning.accumulatedReasoningText}`;
            return windowText ? windowText + '\n\n' + reasoningDisplay : reasoningDisplay;
        }
        return windowText;
    }
    // ------------------------------------------------------------------
    // Element budget + frozen-card guard (300305)
    // ------------------------------------------------------------------
    /** 当前生效的元素预算（常量可被 deps.elementBudget 覆盖，便于按实测调整）。 */
    elementBudget() {
        const override = this.deps.elementBudget;
        return {
            maxChars: override?.maxChars ?? STREAMING_ELEMENT_BUDGET.maxChars,
            maxBytes: override?.maxBytes ?? STREAMING_ELEMENT_BUDGET.maxBytes,
            maxElements: override?.maxElements ?? STREAMING_ELEMENT_BUDGET.maxElements,
        };
    }
    /**
     * 当前窗口：正文中尚未展示的部分里，能放进一张卡的第一段。
     *
     * 复用与终态一致的拆分 helper（段落优先），因此相邻卡片之间可能在
     * 段落分隔符处有极小重叠 —— 宁可重叠，不可丢失正文。
     */
    currentChunk() {
        const remaining = this.text.accumulatedText.slice(this.text.streamingOffsetChars);
        const [head = ''] = splitTextForCardBudget(remaining, this.elementBudget());
        return { head, hasMore: head.length < remaining.length };
    }
    /** 窗口已被填满（后面还有未展示的正文）→ 需要另起一张卡。 */
    isStreamingWindowFull() {
        return this.currentChunk().hasMore;
    }
    /** 写入前的预算预判：字节数 + 元素（Markdown 块）数。 */
    exceedsElementBudget(text) {
        const budget = this.elementBudget();
        return (text.length > budget.maxChars ||
            Buffer.byteLength(text, 'utf8') > budget.maxBytes ||
            countMarkdownBlocks(text) > budget.maxElements);
    }
    isCardFrozen(cardId) {
        return !!cardId && this.cardKit.frozenCardIds.has(cardId);
    }
    isFrozenMessage(messageId) {
        return !!messageId && this.cardKit.frozenMessageIds.has(messageId);
    }
    /**
     * 熔断一张卡：此后不得再对它发任何 cardElement.content / card.update /
     * card.settings。生产事故（2026-09-12）中同一张超限卡被连续重试 8 次，
     * 这里靠冻结状态从结构上杜绝重试。
     */
    freezeCard(cardId, reason) {
        if (!cardId)
            return;
        this.cardKit.frozenCardIds.add(cardId);
        if (this.cardKit.cardMessageId)
            this.cardKit.frozenMessageIds.add(this.cardKit.cardMessageId);
        if (this.cardKit.cardKitCardId === cardId)
            this.cardKit.cardKitCardId = null;
        if (this.cardKit.originalCardKitCardId === cardId)
            this.cardKit.originalCardKitCardId = null;
        log.warn('card frozen — no further CardKit writes to this card', {
            cardId,
            reason,
            seq: this.cardKit.cardKitSequence,
        });
    }
    /** 可写的 CardKit 卡片 ID（冻结卡一律不可写）。 */
    getWritableCardKitId() {
        const id = this.cardKit.cardKitCardId ?? this.cardKit.originalCardKitCardId;
        if (!id || this.isCardFrozen(id))
            return null;
        return id;
    }
    /** 把当前窗口内容推送到当前卡片（冻结卡直接跳过）。 */
    async pushStreamingWindow() {
        const cardId = this.cardKit.cardKitCardId;
        if (!cardId || this.isCardFrozen(cardId))
            return false;
        const windowText = this.buildDisplayText();
        const resolvedText = this.imageResolver.resolveImages(windowText);
        if (resolvedText === this.text.lastFlushedText)
            return false;
        const prevSeq = this.cardKit.cardKitSequence;
        this.cardKit.cardKitSequence += 1;
        log.debug('flushCardUpdate: answer seq bump', {
            seqBefore: prevSeq,
            seqAfter: this.cardKit.cardKitSequence,
        });
        await (0, cardkit_1.streamCardContent)({
            cfg: this.deps.cfg,
            cardId,
            elementId: builder_1.STREAMING_ELEMENT_ID,
            content: (0, markdown_style_1.optimizeMarkdownStyle)(resolvedText),
            sequence: this.cardKit.cardKitSequence,
            accountId: this.deps.accountId,
        });
        this.text.lastFlushedText = resolvedText;
        this.text.lastPushedWindowLen = windowText.length;
        return true;
    }
    /**
     * 续卡：新建 CardKit 卡片并把当前窗口内容推上去（300309 / 300305 共用）。
     * 超过续卡上限则禁用 CardKit 流式，交由 onIdle 走既有终态路径收尾。
     */
    async continueOnNewCard(reason) {
        if (this.cardKit.continuationCount >= MAX_CARD_CONTINUATIONS) {
            log.warn('flushCardUpdate: continuation limit reached, disabling CardKit streaming', {
                seq: this.cardKit.cardKitSequence,
                reason,
                continuationCount: this.cardKit.continuationCount,
                maxContinuations: MAX_CARD_CONTINUATIONS,
            });
            this.cardKit.cardKitCardId = null;
            return false;
        }
        const continued = await this.createContinuationCard();
        if (!continued) {
            log.warn('flushCardUpdate: continuation card creation failed, disabling CardKit streaming', {
                seq: this.cardKit.cardKitSequence,
                reason,
            });
            this.cardKit.cardKitCardId = null;
            return false;
        }
        await this.pushStreamingWindow();
        return true;
    }
    /**
     * 元素预算预分片：当前窗口已满，冻结当前卡并另起一张卡承接后续正文，
     * 从源头避免撞上元素上限（而不是等 300305 发生后再补救）。
     */
    async rolloverFilledChunk() {
        const currentCardId = this.cardKit.cardKitCardId;
        const { head } = this.currentChunk();
        log.info('flushCardUpdate: element budget reached, rolling over to a new card', {
            seq: this.cardKit.cardKitSequence,
            cardId: currentCardId,
            windowChars: head.length,
        });
        this.text.streamingOffsetChars += head.length;
        this.freezeCard(currentCardId, 'element-budget');
        await this.continueOnNewCard('element-budget');
    }
    /**
     * 300305 熔断：飞书对该卡返回元素超限（且响应体不提供任何细节），
     * 立即冻结（禁止原地重试），把正文后续部分拆到新卡续流。
     */
    async handleStreamingElementExceeds() {
        const frozenCardId = this.cardKit.cardKitCardId ?? this.cardKit.originalCardKitCardId;
        this.text.streamingOffsetChars += this.text.lastPushedWindowLen;
        log.warn('flushCardUpdate: element exceeds (300305), freezing card and continuing on a new one', {
            seq: this.cardKit.cardKitSequence,
            cardId: frozenCardId,
            displayedChars: this.text.lastPushedWindowLen,
        });
        this.freezeCard(frozenCardId, '300305');
        await this.continueOnNewCard('300305');
    }
    /**
     * 300309 续流：新建 CardKit 卡片实体并发送新消息。
     *
     * 飞书 CardKit 流式会话有服务端时限（实测约 10 分钟）。
     * 超时后原卡片流已死，必须新建卡片才能继续展示内容。
     * 成功返回 true 并更新 cardKit 状态；失败返回 false。
     */
    async createContinuationCard() {
        try {
            const cId = await (0, cardkit_1.createCardEntity)({
                cfg: this.deps.cfg,
                card: (0, builder_1.buildStreamingThinkingCard)(this.deps.toolUseDisplay.showToolUse),
                accountId: this.deps.accountId,
            });
            if (!cId) {
                throw new Error('card.create returned empty card_id');
            }
            const result = await (0, cardkit_1.sendCardByCardId)({
                cfg: this.deps.cfg,
                to: this.deps.chatId,
                cardId: cId,
                replyToMessageId: this.deps.replyToMessageId,
                replyInThread: this.deps.replyInThread,
                accountId: this.deps.accountId,
            });
            // 更新 cardKit 状态指向新卡片
            this.cardKit.cardKitCardId = cId;
            this.cardKit.originalCardKitCardId = cId;
            this.cardKit.cardKitSequence = 1;
            this.cardKit.cardMessageId = result.messageId;
            this.cardKit.continuationCount += 1;
            this.text.lastFlushedText = ''; // 强制下一次 flush 推送全量文本
            this.flush.setCardMessageReady(true);
            log.info('created continuation CardKit card', {
                cardId: cId,
                messageId: result.messageId,
                continuationCount: this.cardKit.continuationCount,
            });
            return true;
        }
        catch (err) {
            log.warn('createContinuationCard failed', { error: String(err) });
            return false;
        }
    }
    async throttledCardUpdate() {
        if (this.guard.shouldSkip('throttledCardUpdate'))
            return;
        const throttleMs = this.cardKit.cardKitCardId ? reply_dispatcher_types_1.THROTTLE_CONSTANTS.CARDKIT_MS : reply_dispatcher_types_1.THROTTLE_CONSTANTS.PATCH_MS;
        await this.flush.throttledUpdate(throttleMs);
    }
    // ---- Tool-use status streaming (pre-answer phase) ----
    lastToolUseStatusUpdateTime = 0;
    async throttledToolUseStatusUpdate() {
        if (!this.cardKit.cardKitCardId)
            return;
        const now = Date.now();
        if (now - this.lastToolUseStatusUpdateTime < reply_dispatcher_types_1.THROTTLE_CONSTANTS.REASONING_STATUS_MS)
            return;
        this.lastToolUseStatusUpdateTime = now;
        await this.updateToolUseStatus();
    }
    async updateToolUseStatus() {
        if (!this.cardKit.cardKitCardId || this.isTerminalPhase)
            return;
        try {
            const display = this.computeToolUseDisplay();
            const card = (0, builder_1.buildStreamingPreAnswerCard)({
                steps: display?.steps,
                elapsedMs: this.visibleToolUseElapsedMs,
                showToolUse: this.shouldDisplayToolUse,
            });
            this.cardKit.cardKitSequence += 1;
            await (0, cardkit_1.updateCardKitCard)({
                cfg: this.deps.cfg,
                cardId: this.cardKit.cardKitCardId,
                card,
                sequence: this.cardKit.cardKitSequence,
                accountId: this.deps.accountId,
            });
        }
        catch (err) {
            log.debug('updateToolUseStatus failed', { error: String(err) });
        }
    }
    // ------------------------------------------------------------------
    // Internal: lifecycle helpers
    // ------------------------------------------------------------------
    finalizeCard(source, reason) {
        this.transition('completed', source, reason);
    }
    /**
     * 300305 降级：把终态正文拆分为多段发送，保证用户收到完整内容。
     *
     * ⚠️ `im.message.patch` 是「整条消息替换」语义：对同一个 messageId 逐段
     * patch 只会留下最后一段，前文被静默覆盖。因此
     * ——首段复用原卡片消息（原地替换已经死掉的流式卡）；
     * ——其余各段必须各自新发一条卡片消息（`im.message.create`）。
     *
     * 拆分策略：按段落（\n\n）优先切分；若单段仍超限则按字符硬切。
     */
    async sendTerminalContentSplit(terminalContent, toolUseDisplay, footerMetrics) {
        const text = terminalContent.text;
        const chunks = splitTextForCardLimit(text, FEISHU_TERMINAL_TEXT_CHUNK_TARGET);
        log.info('sendTerminalContentSplit: splitting terminal content', {
            totalLen: text.length,
            chunkCount: chunks.length,
        });
        for (let i = 0; i < chunks.length; i++) {
            await this.deliverTerminalChunk({
                chunk: chunks[i],
                isFirst: i === 0,
                includeFooter: i === chunks.length - 1,
                terminalContent,
                toolUseDisplay,
                footerMetrics,
            });
        }
    }
    /**
     * 交付终态正文的一段。
     *
     * - 首段：对原卡片消息做一次 IM patch（原地替换已死的流式卡）；
     * - 其余段：各发一条新卡片消息 —— patch 同一条消息会互相覆盖；
     * - 单段仍撞 300305（极端情况）：对半再拆，仍按「首段原地 / 其余新发」交付。
     */
    async deliverTerminalChunk({ chunk, isFirst, includeFooter, terminalContent, toolUseDisplay, footerMetrics }) {
        const card = this.buildTerminalChunkCard(chunk, includeFooter, terminalContent, toolUseDisplay, footerMetrics);
        try {
            if (isFirst) {
                await (0, send_1.updateCardFeishu)({
                    cfg: this.deps.cfg,
                    messageId: this.cardKit.cardMessageId,
                    card,
                    accountId: this.deps.accountId,
                });
            }
            else {
                await (0, send_1.sendCardFeishu)({
                    cfg: this.deps.cfg,
                    to: this.deps.chatId,
                    card,
                    replyToMessageId: this.deps.replyToMessageId,
                    replyInThread: this.deps.replyInThread,
                    accountId: this.deps.accountId,
                });
            }
        }
        catch (err) {
            if (!(0, card_error_1.isCardElementExceedsError)(err) || chunk.length <= 1000)
                throw err;
            log.warn('deliverTerminalChunk: chunk still exceeds, halving into separate messages', {
                chunkLen: chunk.length,
                isFirst,
            });
            const halves = splitTextForCardLimit(chunk, Math.floor(chunk.length / 2));
            for (let j = 0; j < halves.length; j++) {
                await this.deliverTerminalChunk({
                    chunk: halves[j],
                    isFirst: isFirst && j === 0,
                    includeFooter: includeFooter && j === halves.length - 1,
                    terminalContent,
                    toolUseDisplay,
                    footerMetrics,
                });
            }
        }
    }
    /** 构建终态正文某一段的卡片（reasoning / footer 只挂在最后一段）。 */
    buildTerminalChunkCard(chunk, includeFooter, terminalContent, toolUseDisplay, footerMetrics) {
        return (0, builder_1.buildCardContent)('complete', {
            text: chunk,
            reasoningText: includeFooter ? terminalContent.reasoningText : undefined,
            reasoningElapsedMs: includeFooter ? this.reasoning.reasoningElapsedMs || undefined : undefined,
            toolUseSteps: includeFooter ? toolUseDisplay?.steps : undefined,
            toolUseTitleSuffix: includeFooter ? this.computeToolUseTitleSuffix(toolUseDisplay) : undefined,
            toolUseElapsedMs: includeFooter ? this.visibleToolUseElapsedMs : undefined,
            showToolUse: this.deps.toolUseDisplay.showToolUse,
            elapsedMs: this.elapsed(),
            footer: this.deps.resolvedFooter,
            footerMetrics: includeFooter ? footerMetrics : undefined,
        });
    }
    /**
     * Close streaming mode then update card content (shared by onError and abortCard).
     */
    async closeStreamingAndUpdate(cardId, card, label) {
        if (this.isCardFrozen(cardId)) {
            log.warn(`${label}: card is frozen (element limit), skipping CardKit close/update`, {
                cardId,
            });
            return;
        }
        const seqBeforeClose = this.cardKit.cardKitSequence;
        this.cardKit.cardKitSequence += 1;
        log.info(`${label}: closing streaming mode`, {
            seqBefore: seqBeforeClose,
            seqAfter: this.cardKit.cardKitSequence,
        });
        await (0, cardkit_1.setCardStreamingMode)({
            cfg: this.deps.cfg,
            cardId,
            streamingMode: false,
            sequence: this.cardKit.cardKitSequence,
            accountId: this.deps.accountId,
        });
        const seqBeforeUpdate = this.cardKit.cardKitSequence;
        this.cardKit.cardKitSequence += 1;
        log.info(`${label}: updating card`, {
            seqBefore: seqBeforeUpdate,
            seqAfter: this.cardKit.cardKitSequence,
        });
        await (0, cardkit_1.updateCardKitCard)({
            cfg: this.deps.cfg,
            cardId,
            card: (0, builder_1.toCardKit2)(card),
            sequence: this.cardKit.cardKitSequence,
            accountId: this.deps.accountId,
        });
    }
}
exports.StreamingCardController = StreamingCardController;
// ---------------------------------------------------------------------------
// Error detail extraction helpers (replacing `any` casts)
// ---------------------------------------------------------------------------
/**
 * 终态卡片的正文和 reasoning 都会被飞书按 markdown 渲染，
 * 因此两者都要先做图片替换与表格降级，避免再次撞到 230099/11310。
 */
function prepareTerminalCardContent(content, imageResolver, tableLimit = card_error_1.FEISHU_CARD_TABLE_LIMIT) {
    const resolvedReasoningText = content.reasoningText ? imageResolver.resolveImages(content.reasoningText) : undefined;
    const resolvedText = imageResolver.resolveImages(content.text);
    const sanitizedSegments = (0, card_error_1.sanitizeTextSegmentsForCard)(resolvedReasoningText ? [resolvedReasoningText, resolvedText] : [resolvedText], tableLimit);
    if (resolvedReasoningText) {
        return {
            reasoningText: sanitizedSegments[0],
            text: sanitizedSegments[1],
        };
    }
    return { text: sanitizedSegments[0] };
}
function extractApiDetail(err) {
    if (!err || typeof err !== 'object')
        return String(err);
    const e = err;
    return e.response?.data ? JSON.stringify(e.response.data) : String(err);
}
// ---------------------------------------------------------------------------
// Text splitting helper for 300305 element-exceeds fallback
// ---------------------------------------------------------------------------
/** 经验性的单卡片正文安全上限 -- 留足 markdown 渲染余量（2026-09 实测 30K 字符安全）。 */
const FEISHU_TERMINAL_TEXT_CHUNK_TARGET = 30000;
/**
 * 把长文本按预算拆分为多段（通用版：终态正文与流式窗口共用同一拆分逻辑）。
 *
 * 优先按段落（\n\n）切分保持语义完整；若单段仍超 maxChars 则按字符硬切。
 * 返回的每段长度均 ≤ maxChars，块数 ≤ maxElements。
 */
function splitTextForCardBudget(text, budget) {
    const maxChars = budget?.maxChars ?? FEISHU_TERMINAL_TEXT_CHUNK_TARGET;
    const maxElements = budget?.maxElements ?? Number.POSITIVE_INFINITY;
    if (text.length <= maxChars && countMarkdownBlocks(text) <= maxElements)
        return [text];
    // 先按段落切
    const paragraphs = text.split('\n\n');
    const chunks = [];
    let current = '';
    let currentBlocks = 0;
    for (const para of paragraphs) {
        const next = current ? current + '\n\n' + para : para;
        const overChars = next.length > maxChars;
        const overElements = currentBlocks >= maxElements;
        if ((overChars || overElements) && current) {
            chunks.push(current);
            current = para;
            currentBlocks = 1;
        }
        else {
            current = next;
            currentBlocks += 1;
        }
        // 单段仍超 maxChars — 硬切
        while (current.length > maxChars) {
            chunks.push(current.slice(0, maxChars));
            current = current.slice(maxChars);
        }
    }
    if (current)
        chunks.push(current);
    return chunks;
}
/**
 * 把长文本拆分为不超限的多段（终态口径：30K 字符）。
 *
 * 优先按段落（\n\n）切分保持语义完整；若单段仍超 target 则按字符硬切。
 * 返回的每段长度均 ≤ target。
 */
function splitTextForCardLimit(text, target = FEISHU_TERMINAL_TEXT_CHUNK_TARGET) {
    return splitTextForCardBudget(text, {
        maxChars: target,
        maxElements: Number.POSITIVE_INFINITY,
    });
}
/** 统计 markdown 正文块数（\n\n 分隔）—— 卡片元素数量的经验近似。 */
function countMarkdownBlocks(text) {
    return text.split('\n\n').filter((block) => block.trim().length > 0).length;
}
