'use strict';

/** @abstract */
class Authenticator {
  constructor(credentials) {
    this._credentials = credentials;
  }
  
  async evaluateChallenge(challenge) {
    throw new Error("evaluateChallenge should be implemented");
  }
}

module.exports = Authenticator;
