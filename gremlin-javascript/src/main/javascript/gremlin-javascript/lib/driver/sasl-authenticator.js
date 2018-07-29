'use strict';

const Authenticator = require('./authenticator');

class SaslAuthenticator extends Authenticator {
  /**
   * Creates a new instance of SaslAuthenticator.
   * @param {Object} [options] The authentication options.
   * @param {Object} [options.mechanism] The mechanism to be used for authentication.
   * @param {String} [options.hostname] The hostname of the client.
   * @param {*} [options] Other mechanism specific options.
   * @constructor
   */
  constructor(options) {
    super(options);

    if (options.mechanism === null || options.mechanism === undefined) {
      throw new Error('No Sasl Mechanism Specified');
    }

    this._options = options;
    this._options.mechanism.setopts(this._options);
  }
  
  evaluateChallenge(challenge) {
    return Promise.resolve(this._options.mechanism.evaluateChallenge(challenge));
  }
}

module.exports = SaslAuthenticator;
