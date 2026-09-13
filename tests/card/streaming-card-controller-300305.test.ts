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
const originalSendCardFeishu = send.sendCardFeishu;

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
  send.sendCardFeishu = originalSendCardFeishu;
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

describe("StreamingCardController — terminal split must not overwrite earlier chunks", () => {
  /** Markdown body of a rendered card (the piece the user actually reads). */
  const markdownOf = (card) =>
    card?.elements?.find((e) => e.tag === "markdown")?.content ?? "";

  it("delivers every chunk on its own message instead of re-patching one messageId", async () => {
    const controller = createStreamingController();
    controller.cardKit.cardMessageId = "om_card";

    const patch = vi.fn().mockResolvedValue({ code: 0 });
    const sendCard = vi.fn().mockResolvedValue({ messageId: "om_new" });
    send.updateCardFeishu = patch;
    send.sendCardFeishu = sendCard;

    // Three segments, each at the terminal chunk target — forces 3 chunks.
    const text =
      "A".repeat(30000) + "\n\n" + "B".repeat(30000) + "\n\n" + "C".repeat(30000);
    await controller.sendTerminalContentSplit({ text }, undefined, undefined);

    // Fix for the 2026-09-13 production incident: the FIRST chunk must no
    // longer patch the (dead, possibly schema-2.0) card message in place.
    // Patching it with a schema-1.0 payload triggers
    // "schemaV2 card can not change schemaV1" (230099/200830 → HTTP 400),
    // which aborted the whole split loop and froze the card. All chunks —
    // including the first — are now delivered as brand-new messages.
    expect(patch).not.toHaveBeenCalled();

    // Every chunk reaches its OWN new message; nothing is lost or overwritten.
    expect(sendCard).toHaveBeenCalledTimes(3);
    const sent = sendCard.mock.calls.map((c) => markdownOf(c[0].card));
    expect(sent.some((t) => t.includes("A".repeat(100)))).toBe(true);
    expect(sent.some((t) => t.includes("B".repeat(100)))).toBe(true);
    expect(sent.some((t) => t.includes("C".repeat(100)))).toBe(true);
  });

  it("onIdle 300305 fallback: first chunk lands via a new message, never a patch, and does not throw", async () => {
    // Reproduce the production path: a live CardKit (schema 2.0) card whose
    // final update hits 300305. The fallback must deliver the first chunk on a
    // NEW message (not by patching the schema-2.0 card) and must not surface a
    // hard failure to the caller.
    const controller = seedLiveCard(createStreamingController(), {
      cardId: "card_kit",
      messageId: "om_kit",
      seq: 48,
      text: "",
    });
    controller.text.completedText = "A".repeat(50000); // > 30000 → 2 chunks
    controller.dispatchFullyComplete = true;

    cardkit.setCardStreamingMode = vi.fn().mockResolvedValue({ code: 0 });
    // CardKit final update rejects with the production 300305.
    cardkit.updateCardKitCard = vi.fn().mockRejectedValue(PRODUCTION_300305);

    const patch = vi.fn().mockResolvedValue({ code: 0 });
    const sendCard = vi.fn().mockResolvedValue({ messageId: "om_split" });
    send.updateCardFeishu = patch;
    send.sendCardFeishu = sendCard;

    // Must not throw — the outer onIdle used to swallow the 400 here, leaving
    // the card frozen. Now the split lands cleanly.
    await expect(controller.onIdle()).resolves.toBeUndefined();

    // CardKit final update was attempted exactly once...
    expect(cardkit.updateCardKitCard).toHaveBeenCalledTimes(1);
    // ...and the first chunk was NOT delivered by patching the old card.
    expect(patch).not.toHaveBeenCalled();
    // Content successfully landed: both chunks on new messages.
    expect(sendCard).toHaveBeenCalledTimes(2);
    const sent = sendCard.mock.calls.map((c) => markdownOf(c[0].card));
    expect(sent.every((t) => t.length > 0)).toBe(true);
    expect(sent.some((t) => t.includes("A".repeat(100)))).toBe(true);
  });
});
