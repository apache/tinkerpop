'use strict';

/** @abstract */
class Authenticator {
  constructor(credentials) {
    this._credentials = credentials;
  }
  
  evaluateChallenge(ws, header) {
    throw new Error("evaluateChallenge should be implemented");
  }
}

module.exports = Authenticator;