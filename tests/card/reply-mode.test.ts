import { describe, it, expect } from "vitest";
// eslint-disable-next-line @typescript-eslint/no-var-requires
const {
  resolveReplyMode,
  expandAutoMode,
  shouldUseCard,
  isStreamingEnabled,
} = require("../../src/card/reply-mode.js");

describe("reply-mode", () => {
  describe("resolveReplyMode", () => {
    it("returns static when streaming is not enabled", () => {
      expect(resolveReplyMode({ feishuCfg: {}, chatType: "p2p" })).toBe("static");
      expect(resolveReplyMode({ feishuCfg: { streaming: false }, chatType: "p2p" })).toBe("static");
      expect(resolveReplyMode({ feishuCfg: undefined, chatType: "group" })).toBe("static");
    });

    it("returns auto when streaming enabled but no replyMode set", () => {
      expect(resolveReplyMode({ feishuCfg: { streaming: true }, chatType: "p2p" })).toBe("auto");
    });

    it("returns string replyMode directly", () => {
      expect(resolveReplyMode({ feishuCfg: { streaming: true, replyMode: "streaming" }, chatType: "group" })).toBe("streaming");
    });

    it("prefers scene override over default over string", () => {
      const cfg = { streaming: true, replyMode: { default: "static", group: "streaming", direct: "streaming" } };
      expect(resolveReplyMode({ feishuCfg: cfg, chatType: "group" })).toBe("streaming");
      expect(resolveReplyMode({ feishuCfg: cfg, chatType: "p2p" })).toBe("streaming");
      expect(resolveReplyMode({ feishuCfg: cfg, chatType: "other" })).toBe("static");
    });

    it("falls back to default when scene missing", () => {
      const cfg = { streaming: true, replyMode: { default: "static" } };
      expect(resolveReplyMode({ feishuCfg: cfg, chatType: "group" })).toBe("static");
    });

    it("accepts OpenClaw 9.3 unified object form { mode } for streaming", () => {
      expect(resolveReplyMode({ feishuCfg: { streaming: { mode: "partial" } }, chatType: "p2p" })).toBe("auto");
      expect(resolveReplyMode({ feishuCfg: { streaming: { mode: "block" } }, chatType: "p2p" })).toBe("auto");
      expect(resolveReplyMode({ feishuCfg: { streaming: { mode: "progress" } }, chatType: "p2p" })).toBe("auto");
      expect(resolveReplyMode({ feishuCfg: { streaming: { mode: "off" } }, chatType: "p2p" })).toBe("static");
      expect(resolveReplyMode({ feishuCfg: { streaming: { mode: "partial" }, replyMode: "streaming" }, chatType: "group" })).toBe("streaming");
    });
  });

  describe("expandAutoMode", () => {
    it("passes through non-auto modes", () => {
      expect(expandAutoMode({ mode: "streaming", streaming: true, chatType: "group" })).toBe("streaming");
      expect(expandAutoMode({ mode: "static", streaming: true, chatType: "p2p" })).toBe("static");
    });

    it("expands auto: group→static, p2p→streaming when streaming enabled", () => {
      expect(expandAutoMode({ mode: "auto", streaming: true, chatType: "group" })).toBe("static");
      expect(expandAutoMode({ mode: "auto", streaming: true, chatType: "p2p" })).toBe("streaming");
    });

    it("expands auto to static when streaming not enabled", () => {
      expect(expandAutoMode({ mode: "auto", streaming: false, chatType: "p2p" })).toBe("static");
      expect(expandAutoMode({ mode: "auto", streaming: undefined, chatType: "group" })).toBe("static");
    });

    it("expands auto with unified object streaming form", () => {
      expect(expandAutoMode({ mode: "auto", streaming: { mode: "partial" }, chatType: "group" })).toBe("static");
      expect(expandAutoMode({ mode: "auto", streaming: { mode: "partial" }, chatType: "p2p" })).toBe("streaming");
      expect(expandAutoMode({ mode: "auto", streaming: { mode: "off" }, chatType: "p2p" })).toBe("static");
    });
  });

  describe("isStreamingEnabled normalization", () => {
    it("normalizes legacy boolean and 9.3 object forms", () => {
      expect(isStreamingEnabled(true)).toBe(true);
      expect(isStreamingEnabled(false)).toBe(false);
      expect(isStreamingEnabled(undefined)).toBe(false);
      expect(isStreamingEnabled({ mode: "partial" })).toBe(true);
      expect(isStreamingEnabled({ mode: "block" })).toBe(true);
      expect(isStreamingEnabled({ mode: "progress" })).toBe(true);
      expect(isStreamingEnabled({ mode: "off" })).toBe(false);
      expect(isStreamingEnabled({})).toBe(false);
      expect(isStreamingEnabled("partial")).toBe(false);
    });
  });

  describe("shouldUseCard", () => {
    it("always returns false (native post rendering preserves bot-at-bot @)", () => {
      expect(shouldUseCard("hello")).toBe(false);
      expect(shouldUseCard("```code\nblock```")).toBe(false);
      expect(shouldUseCard("| a | b |")).toBe(false);
      expect(shouldUseCard("")).toBe(false);
    });
  });
});
