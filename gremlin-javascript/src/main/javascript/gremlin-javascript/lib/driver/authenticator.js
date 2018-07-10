'use strict';

/** @abstract */
class Authenticator {
  constructor(credentials) {
    this._credentials = credentials;
  }
  
  evaluateChallenge(challenge) {
    throw new Error("evaluateChallenge should be implemented");
  }
}

module.exports = Authenticator;
