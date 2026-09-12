import { describe, it, expect, beforeEach, vi } from "vitest";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const cardkit = require("../../src/card/cardkit.js");
// eslint-disable-next-line @typescript-eslint/no-var-requires
const send = require("../../src/messaging/outbound/send.js");
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { StreamingCardController } = require("../../src/card/streaming-card-controller.js");

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

function createStreamingController(deps = createMockDeps()) {
  return new StreamingCardController(deps);
}

/** Seed a CardKit-backed controller with a live card. */
function seedLiveCard(controller, { cardId = "card_1", messageId = "om_1", seq = 1, text = "" } = {}) {
  controller.cardKit.cardKitCardId = cardId;
  controller.cardKit.originalCardKitCardId = cardId;
  controller.cardKit.cardMessageId = messageId;
  controller.cardKit.cardKitSequence = seq;
  controller.text.accumulatedText = text;
  controller.flush.setCardMessageReady(true);
  return controller;
}

const PRODUCTION_300305 = {
  code: 300305,
  msg: "ErrMsg: element exceeds the limit; ",
};

beforeEach(() => {
  cardkit.streamCardContent = originalStreamCardContent;
  cardkit.createCardEntity = originalCreateCardEntity;
  cardkit.sendCardByCardId = originalSendCardByCardId;
  cardkit.updateCardKitCard = originalUpdateCardKitCard;
  cardkit.setCardStreamingMode = originalSetCardStreamingMode;
  send.updateCardFeishu = originalUpdateCardFeishu;
});

describe("StreamingCardController — 300305 in the streaming flush path", () => {
  it("freezes the offending card and continues on a new card (no in-place retry)", async () => {
    const controller = seedLiveCard(createStreamingController(), {
      cardId: "card_old",
      messageId: "om_old",
      seq: 24,
      text: "Hello world",
    });

    const mockStream = vi
      .fn()
      .mockRejectedValueOnce(PRODUCTION_300305)
      .mockResolvedValue({ code: 0 });
    cardkit.streamCardContent = mockStream;
    cardkit.createCardEntity = vi.fn().mockResolvedValue("card_new");
    cardkit.sendCardByCardId = vi.fn().mockResolvedValue({ messageId: "om_new" });

    await controller.performFlush();

    // The over-limit card is written exactly once — never retried in place.
    const oldCardWrites = mockStream.mock.calls.filter((call: any[]) => call[0].cardId === "card_old");
    expect(oldCardWrites.length).toBe(1);
    // It is marked frozen...
    expect(controller.isCardFrozen("card_old")).toBe(true);
    // ...and the stream continues on a brand new card.
    expect(cardkit.createCardEntity).toHaveBeenCalledTimes(1);
    expect(controller.cardKit.cardKitCardId).toBe("card_new");
    expect(controller.cardKit.continuationCount).toBe(1);
    expect(mockStream).toHaveBeenLastCalledWith(
      expect.objectContaining({ cardId: "card_new" })
    );
  });

  it("never writes to the frozen card again on later flushes", async () => {
    const controller = seedLiveCard(createStreamingController(), {
      cardId: "card_old",
      messageId: "om_old",
      seq: 24,
      text: "Hello",
    });

    const mockStream = vi
      .fn()
      .mockRejectedValueOnce(PRODUCTION_300305)
      .mockResolvedValue({ code: 0 });
    cardkit.streamCardContent = mockStream;
    cardkit.createCardEntity = vi.fn().mockResolvedValue("card_new");
    cardkit.sendCardByCardId = vi.fn().mockResolvedValue({ messageId: "om_new" });

    await controller.performFlush();

    // Streaming moved to the continuation card, not the frozen one.
    expect(controller.cardKit.cardKitCardId).toBe("card_new");

    // More content arrives after the freeze.
    controller.text.accumulatedText = "Hello world, more content";
    await controller.performFlush();
    await controller.performFlush();

    const frozenWrites = mockStream.mock.calls.filter((call: any[]) => call[0].cardId === "card_old");
    expect(frozenWrites.length).toBe(1);
  });

  it("stops after MAX_CARD_CONTINUATIONS instead of retrying the over-limit card", async () => {
    const controller = seedLiveCard(createStreamingController(), {
      cardId: "card_old",
      messageId: "om_old",
      seq: 24,
      text: "Hello",
    });
    controller.cardKit.continuationCount = 3;

    const mockStream = vi.fn().mockRejectedValue(PRODUCTION_300305);
    cardkit.streamCardContent = mockStream;
    const mockCreate = vi.fn();
    cardkit.createCardEntity = mockCreate;

    await controller.performFlush();

    expect(mockCreate).not.toHaveBeenCalled();
    expect(controller.isCardFrozen("card_old")).toBe(true);
    expect(controller.cardKit.cardKitCardId).toBeNull();
  });

  it("still delivers the final conclusion when the active card was frozen", async () => {
    const controller = seedLiveCard(createStreamingController(), {
      cardId: "card_old",
      messageId: "om_old",
      seq: 24,
      text: "Partial content",
    });

    const mockStream = vi.fn().mockRejectedValue(PRODUCTION_300305);
    cardkit.streamCardContent = mockStream;
    // Continuation card creation fails — CardKit streaming is disabled for this run.
    cardkit.createCardEntity = vi.fn().mockRejectedValue(new Error("create failed"));

    await controller.performFlush();

    expect(controller.isCardFrozen("card_old")).toBe(true);

    const mockPatch = vi.fn().mockResolvedValue({ code: 0 });
    send.updateCardFeishu = mockPatch;
    const mockUpdate = vi.fn().mockResolvedValue({ code: 0 });
    cardkit.updateCardKitCard = mockUpdate;
    cardkit.setCardStreamingMode = vi.fn().mockResolvedValue({ code: 0 });

    controller.text.completedText = "Final conclusion for the user";
    controller.dispatchFullyComplete = true;
    await controller.onIdle();

    // The frozen card must not receive any further CardKit write...
    expect(mockUpdate).not.toHaveBeenCalled();
    // ...but the conclusion still reaches the user.
    expect(mockPatch).toHaveBeenCalled();
  });
});

describe("StreamingCardController — element budget pre-slicing", () => {
  it("rolls over to a new card before the element limit is hit", async () => {
    const controller = seedLiveCard(createStreamingController(), {
      cardId: "card_1",
      messageId: "om_1",
      seq: 1,
      text: "X".repeat(60000), // far beyond any element budget
    });

    const mockStream = vi.fn().mockResolvedValue({ code: 0 });
    cardkit.streamCardContent = mockStream;
    cardkit.createCardEntity = vi.fn().mockResolvedValue("card_2");
    cardkit.sendCardByCardId = vi.fn().mockResolvedValue({ messageId: "om_2" });

    await controller.performFlush();

    // A continuation card is created proactively — no 300305 involved.
    expect(cardkit.createCardEntity).toHaveBeenCalledTimes(1);
    expect(controller.isCardFrozen("card_1")).toBe(true);
    expect(controller.cardKit.cardKitCardId).toBe("card_2");

    // Every push stays within the configured budget.
    const budget = controller.elementBudget();
    for (const call of mockStream.mock.calls) {
      expect(call[0].content.length).toBeLessThanOrEqual(budget.maxChars);
    }
  });

  it("allows the element budget to be overridden per run (configurable)", () => {
    const controller = createStreamingController(
      createMockDeps({ elementBudget: { maxChars: 100, maxBytes: 999, maxElements: 2 } })
    );
    const budget = controller.elementBudget();
    expect(budget.maxChars).toBe(100);
    expect(budget.maxBytes).toBe(999);
    expect(budget.maxElements).toBe(2);
  });

  it("keeps short answers on a single card (no premature rollover)", async () => {
    const controller = seedLiveCard(createStreamingController(), {
      cardId: "card_1",
      messageId: "om_1",
      seq: 1,
      text: "A short answer.",
    });

    const mockStream = vi.fn().mockResolvedValue({ code: 0 });
    cardkit.streamCardContent = mockStream;
    const mockCreate = vi.fn();
    cardkit.createCardEntity = mockCreate;

    await controller.performFlush();
    controller.text.accumulatedText = "A short answer, extended.";
    await controller.performFlush();

    expect(mockCreate).not.toHaveBeenCalled();
    expect(controller.isCardFrozen("card_1")).toBe(false);
    expect(controller.cardKit.cardKitCardId).toBe("card_1");
  });
});
