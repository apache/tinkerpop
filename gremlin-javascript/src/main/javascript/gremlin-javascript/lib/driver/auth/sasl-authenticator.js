'use strict';

const Authenticator = require('./authenticator');

class SaslAuthenticator extends Authenticator {
  /**
   * Creates a new instance of SaslAuthenticator.
   * @param {Object} [options] The authentication options.
   * @param {Object} [options.mechanism] The mechanism to be used for authentication.
   * @constructor
   */
  constructor(options) {
    super(options);

    if (options.mechanism === null || options.mechanism === undefined) {
      throw new Error('No Sasl Mechanism Specified');
    }
  }
  
  /**
   * Evaluates the challenge from the server and returns appropriate response.
   * @param {String} challenge Challenge string presented by the server.
   * @return {Object} A Promise that resolves to a valid sasl response object.
   */
  evaluateChallenge(challenge) {
    return Promise.resolve(this._options.mechanism.evaluateChallenge(challenge));
  }
}

module.exports = SaslAuthenticator;
