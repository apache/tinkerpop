'use strict';

/** @abstract */
class Authenticator {
  constructor(options) {
    this._options = options;
  }
  
  /**
   * @abstract
   * Evaluates the challenge from the server and returns appropriate response.
   * @param {String} challenge Challenge string presented by the server.
   */
  evaluateChallenge(challenge) {
    throw new Error("evaluateChallenge should be implemented");
  }
}

module.exports = Authenticator;
