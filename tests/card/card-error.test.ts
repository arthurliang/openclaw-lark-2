import { describe, it, expect, beforeEach } from "vitest";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const cardError = require("../../src/card/card-error.js");

describe("card-error", () => {
  describe("CARD_ERROR constants", () => {
    it("includes 300309 and 300305", () => {
      expect(cardError.CARD_ERROR.STREAMING_CLOSED).toBe(300309);
      expect(cardError.CARD_ERROR.ELEMENT_EXCEEDS).toBe(300305);
    });
  });

  describe("isCardStreamingClosedError", () => {
    it("returns true for code 300309", () => {
      const err = { code: 300309, msg: "streaming mode is closed" };
      expect(cardError.isCardStreamingClosedError(err)).toBe(true);
    });

    it("returns true for Axios-style nested code 300309", () => {
      const err = {
        response: { data: { code: 300309, msg: "streaming mode is closed" } },
      };
      expect(cardError.isCardStreamingClosedError(err)).toBe(true);
    });

    it("returns false for other codes", () => {
      expect(cardError.isCardStreamingClosedError({ code: 230020 })).toBe(false);
      expect(cardError.isCardStreamingClosedError({ code: 300305 })).toBe(false);
      expect(cardError.isCardStreamingClosedError({ code: 230099 })).toBe(false);
      expect(cardError.isCardStreamingClosedError({})).toBe(false);
      expect(cardError.isCardStreamingClosedError(null)).toBe(false);
    });
  });

  describe("isCardElementExceedsError", () => {
    it("returns true for code 300305", () => {
      const err = { code: 300305, msg: "element exceeds the limit" };
      expect(cardError.isCardElementExceedsError(err)).toBe(true);
    });

    it("returns true for Axios-style nested code 300305", () => {
      const err = {
        response: { data: { code: 300305, msg: "element exceeds the limit" } },
      };
      expect(cardError.isCardElementExceedsError(err)).toBe(true);
    });

    it("returns false for other codes", () => {
      expect(cardError.isCardElementExceedsError({ code: 230020 })).toBe(false);
      expect(cardError.isCardElementExceedsError({ code: 300309 })).toBe(false);
      expect(cardError.isCardElementExceedsError({ code: 230099 })).toBe(false);
      expect(cardError.isCardElementExceedsError({})).toBe(false);
      expect(cardError.isCardElementExceedsError(null)).toBe(false);
    });
  });

  describe("parseCardApiError", () => {
    it("parses code 300309 with msg", () => {
      const err = { code: 300309, msg: "streaming mode is closed" };
      const parsed = cardError.parseCardApiError(err);
      expect(parsed).toEqual({
        code: 300309,
        subCode: null,
        errMsg: "streaming mode is closed",
      });
    });

    it("parses code 300305 with msg", () => {
      const err = { code: 300305, msg: "element exceeds the limit" };
      const parsed = cardError.parseCardApiError(err);
      expect(parsed).toEqual({
        code: 300305,
        subCode: null,
        errMsg: "element exceeds the limit",
      });
    });
  });
});
