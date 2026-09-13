import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const cardkit = require("../../src/card/cardkit.js");
// eslint-disable-next-line @typescript-eslint/no-var-requires
const send = require("../../src/messaging/outbound/send.js");
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { StreamingCardController } = require("../../src/card/streaming-card-controller.js");

// Save original implementations
const originalStreamCardContent = cardkit.streamCardContent;
const originalCreateCardEntity = cardkit.createCardEntity;
const originalSendCardByCardId = cardkit.sendCardByCardId;
const originalUpdateCardKitCard = cardkit.updateCardKitCard;
const originalSetCardStreamingMode = cardkit.setCardStreamingMode;
const originalUpdateCardFeishu = send.updateCardFeishu;
const originalSendCardFeishu = send.sendCardFeishu;

// A terminal split sends its tail segments as brand-new card messages; keep
// the real network path out of every test in this file.
afterEach(() => {
  send.sendCardFeishu = originalSendCardFeishu;
});

function createMockDeps(overrides = {}) {
  return {
    cfg: {},
    agentId: "test-agent",
    sessionKey: "test-session",
    accountId: "test-account",
    chatId: "oc_test_chat",
    replyToMessageId: "om_test_msg",
    replyInThread: false,
    toolUseDisplay: { showToolUse: false },
    resolvedFooter: {},
    activityOnly: false,
    ...overrides,
  };
}

function createController(deps = createMockDeps()) {
  return new StreamingCardController(deps);
}

describe("StreamingCardController — 300309 streaming closed", () => {
  beforeEach(() => {
    // Restore originals
    cardkit.streamCardContent = originalStreamCardContent;
    cardkit.createCardEntity = originalCreateCardEntity;
    cardkit.sendCardByCardId = originalSendCardByCardId;
    cardkit.updateCardKitCard = originalUpdateCardKitCard;
    cardkit.setCardStreamingMode = originalSetCardStreamingMode;
    send.updateCardFeishu = originalUpdateCardFeishu;
  });

  it("performFlush catches 300309 and creates a new card to continue streaming", async () => {
    const controller = createController();
    controller.cardKit.cardMessageId = "om_old_msg";
    controller.cardKit.cardKitCardId = "card_old";
    controller.cardKit.originalCardKitCardId = "card_old";
    controller.cardKit.cardKitSequence = 5;
    controller.text.accumulatedText = "Hello world";
    controller.flush.setCardMessageReady(true);

    // First streamCardContent call throws 300309
    const mockStream = vi.fn()
      .mockRejectedValueOnce({ code: 300309, msg: "streaming mode is closed" })
      // Second call (on new card) succeeds
      .mockResolvedValueOnce({ code: 0 });
    cardkit.streamCardContent = mockStream;

    const mockCreate = vi.fn().mockResolvedValue("card_new");
    cardkit.createCardEntity = mockCreate;

    const mockSend = vi.fn().mockResolvedValue({ messageId: "om_new_msg", chatId: "oc_test" });
    cardkit.sendCardByCardId = mockSend;

    await controller.performFlush();

    // Should have created a new card entity
    expect(mockCreate).toHaveBeenCalledTimes(1);
    expect(mockSend).toHaveBeenCalledTimes(1);
    // Should have updated to the new card ID
    expect(controller.cardKit.cardKitCardId).toBe("card_new");
    expect(controller.cardKit.cardMessageId).toBe("om_new_msg");
    // Sequence should be reset for new card then bumped by continuation flush
    expect(controller.cardKit.cardKitSequence).toBe(2);
    // Should have streamed content to the new card
    expect(mockStream).toHaveBeenCalledTimes(2);
    expect(mockStream).toHaveBeenLastCalledWith(
      expect.objectContaining({ cardId: "card_new" })
    );
  });

  it("performFlush catches 300309 and falls back to disabling CardKit if new card creation fails", async () => {
    const controller = createController();
    controller.cardKit.cardMessageId = "om_old_msg";
    controller.cardKit.cardKitCardId = "card_old";
    controller.cardKit.originalCardKitCardId = "card_old";
    controller.cardKit.cardKitSequence = 5;
    controller.text.accumulatedText = "Hello world";
    controller.flush.setCardMessageReady(true);

    const mockStream = vi.fn().mockRejectedValue({ code: 300309, msg: "streaming mode is closed" });
    cardkit.streamCardContent = mockStream;

    const mockCreate = vi.fn().mockRejectedValue(new Error("create failed"));
    cardkit.createCardEntity = mockCreate;

    await controller.performFlush();

    // Should have attempted to create a new card
    expect(mockCreate).toHaveBeenCalledTimes(1);
    // Should have fallen back to disabling CardKit streaming
    expect(controller.cardKit.cardKitCardId).toBeNull();
    // Should NOT have updated the old card via IM patch (same messageId stream is dead)
    // updateCardFeishu should not have been called (it's not mocked in this test)
    expect(controller.cardKit.cardKitCardId).toBeNull();
  });

  it("performFlush does not create new card if already in terminal phase", async () => {
    const controller = createController();
    controller.cardKit.cardMessageId = "om_msg";
    controller.cardKit.cardKitCardId = "card_1";
    controller.cardKit.cardKitSequence = 1;
    controller.text.accumulatedText = "text";
    controller.flush.setCardMessageReady(true);
    controller.transition("completed", "test", "done");

    const mockCreate = vi.fn();
    cardkit.createCardEntity = mockCreate;

    await controller.performFlush();

    // Terminal phase — performFlush returns early
    expect(mockCreate).not.toHaveBeenCalled();
  });
});

describe("StreamingCardController — onIdle 300305 element exceeds", () => {
  beforeEach(() => {
    // Restore originals
    cardkit.streamCardContent = originalStreamCardContent;
    cardkit.createCardEntity = originalCreateCardEntity;
    cardkit.sendCardByCardId = originalSendCardByCardId;
    cardkit.updateCardKitCard = originalUpdateCardKitCard;
    cardkit.setCardStreamingMode = originalSetCardStreamingMode;
    send.updateCardFeishu = originalUpdateCardFeishu;
  });

  it("onIdle catches 300305 on final update and splits text into multiple messages", async () => {
    const controller = createController();
    controller.cardKit.cardMessageId = "om_msg";
    controller.cardKit.cardKitCardId = "card_1";
    controller.cardKit.originalCardKitCardId = "card_1";
    controller.cardKit.cardKitSequence = 10;
    controller.text.completedText = "A".repeat(50000); // Very long text
    controller.dispatchFullyComplete = true;
    controller.flush.setCardMessageReady(true);

    // setCardStreamingMode succeeds
    const mockSetStreaming = vi.fn().mockResolvedValue({ code: 0 });
    cardkit.setCardStreamingMode = mockSetStreaming;

    // updateCardKitCard throws 300305
    const mockUpdate = vi.fn().mockRejectedValue({ code: 300305, msg: "element exceeds the limit" });
    cardkit.updateCardKitCard = mockUpdate;

    // updateCardFeishu (IM fallback) succeeds
    const mockPatch = vi.fn().mockResolvedValue({ code: 0 });
    send.updateCardFeishu = mockPatch;
    const mockSendCard = vi.fn().mockResolvedValue({ messageId: "om_cont" });
    send.sendCardFeishu = mockSendCard;

    await controller.onIdle();

    // CardKit final update was attempted once, then hit 300305.
    expect(mockUpdate).toHaveBeenCalledTimes(1);
    // 2026-09-13 fix: the first split chunk must NOT patch the (dead, schema-2.0)
    // card message — that triggers "schemaV2 card can not change schemaV1" and
    // froze the card. All chunks now land on brand-new messages.
    expect(mockPatch).not.toHaveBeenCalled();
    // 50000 chars → 2 chunks, each on its own new message.
    expect(mockSendCard).toHaveBeenCalledTimes(2);
    for (const call of mockSendCard.mock.calls) {
      const card = call[0].card;
      const textContent = card.elements?.find((e: any) => e.tag === "markdown")?.content ?? "";
      expect(textContent.length).toBeLessThan(50000);
    }
  });

  it("onIdle catches 300305 and retries with smaller chunks on IM patch", async () => {
    const controller = createController();
    controller.cardKit.cardMessageId = "om_msg";
    controller.cardKit.cardKitCardId = null;
    controller.cardKit.originalCardKitCardId = null;
    controller.cardKit.cardKitSequence = 0;
    controller.text.completedText = "B".repeat(60000);
    controller.dispatchFullyComplete = true;
    controller.flush.setCardMessageReady(true);

    // updateCardFeishu throws 300305 on the initial full-card patch attempt.
    const mockPatch = vi.fn()
      .mockRejectedValueOnce({ code: 300305, msg: "element exceeds the limit" })
      .mockResolvedValue({ code: 0 });
    send.updateCardFeishu = mockPatch;
    const mockSendCard = vi.fn().mockResolvedValue({ messageId: "om_cont" });
    send.sendCardFeishu = mockSendCard;

    await controller.onIdle();

    // Only the initial full-card patch attempt hits the old message (once).
    expect(mockPatch).toHaveBeenCalledTimes(1);
    // 2026-09-13 fix: the split retry no longer re-patches the old card —
    // every split chunk is delivered as a new message instead.
    // 60000 chars → 2 chunks on new messages.
    expect(mockSendCard).toHaveBeenCalledTimes(2);
  });
});

describe("StreamingCardController — 300309 continuation limit guard", () => {
  beforeEach(() => {
    cardkit.streamCardContent = originalStreamCardContent;
    cardkit.createCardEntity = originalCreateCardEntity;
    cardkit.sendCardByCardId = originalSendCardByCardId;
    cardkit.updateCardKitCard = originalUpdateCardKitCard;
    cardkit.setCardStreamingMode = originalSetCardStreamingMode;
    send.updateCardFeishu = originalUpdateCardFeishu;
  });

  it("still creates continuation card when under the limit", async () => {
    const controller = createController();
    controller.cardKit.cardMessageId = "om_old";
    controller.cardKit.cardKitCardId = "card_1";
    controller.cardKit.originalCardKitCardId = "card_1";
    controller.cardKit.cardKitSequence = 3;
    controller.text.accumulatedText = "text";
    controller.flush.setCardMessageReady(true);

    const mockStream = vi.fn()
      .mockRejectedValueOnce({ code: 300309, msg: "streaming mode is closed" })
      .mockResolvedValueOnce({ code: 0 });
    cardkit.streamCardContent = mockStream;

    const mockCreate = vi.fn().mockResolvedValue("card_2");
    cardkit.createCardEntity = mockCreate;
    cardkit.sendCardByCardId = vi.fn().mockResolvedValue({ messageId: "om_new" });

    await controller.performFlush();

    expect(mockCreate).toHaveBeenCalledTimes(1);
    expect(controller.cardKit.cardKitCardId).toBe("card_2");
    expect(controller.cardKit.continuationCount).toBe(1);
  });

  it("stops creating new cards after reaching MAX_CARD_CONTINUATIONS and degrades gracefully", async () => {
    const controller = createController();
    controller.cardKit.cardMessageId = "om_old";
    controller.cardKit.cardKitCardId = "card_1";
    controller.cardKit.originalCardKitCardId = "card_1";
    controller.cardKit.cardKitSequence = 3;
    controller.cardKit.continuationCount = 3; // already at limit
    controller.text.accumulatedText = "text";
    controller.flush.setCardMessageReady(true);

    const mockStream = vi.fn().mockRejectedValue({ code: 300309, msg: "streaming mode is closed" });
    cardkit.streamCardContent = mockStream;

    const mockCreate = vi.fn();
    cardkit.createCardEntity = mockCreate;

    await controller.performFlush();

    // Should NOT attempt to create a new card
    expect(mockCreate).not.toHaveBeenCalled();
    // Should have disabled CardKit streaming
    expect(controller.cardKit.cardKitCardId).toBeNull();
    // Count should stay at limit (not increment)
    expect(controller.cardKit.continuationCount).toBe(3);
  });

  it("onIdle still delivers final content after continuation limit reached", async () => {
    const controller = createController();
    controller.cardKit.cardMessageId = "om_msg";
    controller.cardKit.cardKitCardId = null; // CardKit disabled after limit
    controller.cardKit.originalCardKitCardId = "card_orig";
    controller.cardKit.cardKitSequence = 5;
    controller.cardKit.continuationCount = 3;
    controller.text.completedText = "Final answer content";
    controller.dispatchFullyComplete = true;
    controller.flush.setCardMessageReady(true);

    const mockSetStreaming = vi.fn().mockResolvedValue({ code: 0 });
    cardkit.setCardStreamingMode = mockSetStreaming;

    const mockUpdate = vi.fn().mockResolvedValue({ code: 0 });
    cardkit.updateCardKitCard = mockUpdate;

    await controller.onIdle();

    // Should have closed streaming on the original card
    expect(mockSetStreaming).toHaveBeenCalledWith(
      expect.objectContaining({ cardId: "card_orig", streamingMode: false })
    );
    // Should have updated the original card with final content
    expect(mockUpdate).toHaveBeenCalledWith(
      expect.objectContaining({ cardId: "card_orig" })
    );
  });

  it("continuationCount resets to 0 for each new controller instance (per-run)", async () => {
    const controller1 = createController();
    controller1.cardKit.cardMessageId = "om_1";
    controller1.cardKit.cardKitCardId = "card_a";
    controller1.cardKit.originalCardKitCardId = "card_a";
    controller1.cardKit.cardKitSequence = 1;
    controller1.text.accumulatedText = "text";
    controller1.flush.setCardMessageReady(true);

    const mockStream1 = vi.fn()
      .mockRejectedValueOnce({ code: 300309, msg: "closed" })
      .mockResolvedValueOnce({ code: 0 });
    cardkit.streamCardContent = mockStream1;
    cardkit.createCardEntity = vi.fn().mockResolvedValue("card_b");
    cardkit.sendCardByCardId = vi.fn().mockResolvedValue({ messageId: "om_2" });

    await controller1.performFlush();
    expect(controller1.cardKit.continuationCount).toBe(1);

    // New controller instance (new run) — count starts at 0
    const controller2 = createController();
    expect(controller2.cardKit.continuationCount).toBe(0);
  });
});

describe("StreamingCardController — existing error handling regression", () => {
  beforeEach(() => {
    // Restore originals
    cardkit.streamCardContent = originalStreamCardContent;
    cardkit.createCardEntity = originalCreateCardEntity;
    cardkit.sendCardByCardId = originalSendCardByCardId;
    cardkit.updateCardKitCard = originalUpdateCardKitCard;
    cardkit.setCardStreamingMode = originalSetCardStreamingMode;
    send.updateCardFeishu = originalUpdateCardFeishu;
  });

  it("performFlush still skips on 230020 rate limit", async () => {
    const controller = createController();
    controller.cardKit.cardMessageId = "om_msg";
    controller.cardKit.cardKitCardId = "card_1";
    controller.cardKit.cardKitSequence = 1;
    controller.text.accumulatedText = "text";
    controller.flush.setCardMessageReady(true);

    const mockStream = vi.fn().mockRejectedValue({ code: 230020, msg: "rate limited" });
    cardkit.streamCardContent = mockStream;

    const mockCreate = vi.fn();
    cardkit.createCardEntity = mockCreate;

    await controller.performFlush();

    // Should NOT have disabled CardKit streaming
    expect(controller.cardKit.cardKitCardId).toBe("card_1");
    expect(mockCreate).not.toHaveBeenCalled();
  });

  it("performFlush still disables CardKit on 230099/11310 table limit", async () => {
    const controller = createController();
    controller.cardKit.cardMessageId = "om_msg";
    controller.cardKit.cardKitCardId = "card_1";
    controller.cardKit.originalCardKitCardId = "card_1";
    controller.cardKit.cardKitSequence = 1;
    controller.text.accumulatedText = "text";
    controller.flush.setCardMessageReady(true);

    const mockStream = vi.fn().mockRejectedValue({
      code: 230099,
      msg: "Failed to create card content, ext=ErrCode: 11310; ErrMsg: card table number over limit; ErrorValue: table;",
    });
    cardkit.streamCardContent = mockStream;

    const mockCreate = vi.fn();
    cardkit.createCardEntity = mockCreate;

    await controller.performFlush();

    // Should have disabled CardKit streaming but kept originalCardKitCardId
    expect(controller.cardKit.cardKitCardId).toBeNull();
    expect(controller.cardKit.originalCardKitCardId).toBe("card_1");
    expect(mockCreate).not.toHaveBeenCalled();
  });
});
