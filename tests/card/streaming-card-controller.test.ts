import { describe, it, expect, beforeEach, vi } from "vitest";

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

    await controller.onIdle();

    // Should have tried CardKit update first
    expect(mockUpdate).toHaveBeenCalledTimes(1);
    // Should have fallen back to IM patch with split content
    expect(mockPatch).toHaveBeenCalled();
    // The IM patch should have been called with a smaller chunk
    const patchCalls = mockPatch.mock.calls;
    expect(patchCalls.length).toBeGreaterThan(0);
    // Each patch call should have text smaller than the original
    for (const call of patchCalls) {
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

    // updateCardFeishu throws 300305 first, then succeeds
    const mockPatch = vi.fn()
      .mockRejectedValueOnce({ code: 300305, msg: "element exceeds the limit" })
      .mockResolvedValue({ code: 0 });
    send.updateCardFeishu = mockPatch;

    await controller.onIdle();

    // Should have retried with split chunks
    expect(mockPatch.mock.calls.length).toBeGreaterThan(1);
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
