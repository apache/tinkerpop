'use strict';

const Authenticator = require('./authenticator');

class SaslAuthenticator extends Authenticator {
  /**
   * Creates a new instance of SaslAuthenticator.
   * @param {Object} [credentials] The authentication credential options.
   * @param {String} [credentials.username] The user for the authentication response.
   * @param {String} [credentials.password] The plaintext password for authentication response.
   * @constructor
   */
  constructor(credentials) {
    super(credentials);
  }
  
  evaluateChallenge(challenge) {
    return Promise.resolve({ 'sasl': this.saslArgument() });
  }

  saslArgument() {
    if (typeof this._credentials.username === "undefined" || this._credentials.username.length === 0 
      || typeof this._credentials.password === "undefined" || this._credentials.password.length === 0 ) {
        throw new Error('No Credentials Supplied');
    }
    return new Buffer(`\0${this._credentials.username}\0${this._credentials.password}`).toString('base64');
  }
}

module.exports = SaslAuthenticator;
