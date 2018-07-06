'use strict';

const Authenticator = require('./authenticator');
const utils = require('../utils');

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
  
  evaluateChallenge(ws, header) {
    const message = bufferFromString(header + JSON.stringify({
      'requestId': { '@type': 'g:UUID', '@value': utils.getUuid() },
      'op': 'authentication',
      'processor': 'traversal',
      'args': {
        'sasl': this.saslArgument()
      }
    }));
    
    return ws.send(message);
  }

  saslArgument() {
    if (this._credentials.username === null || this._credentials.username.length === 0 
      || this._credentials.password === null || this._credentials.password.length === 0 ) {
      return '';
    }
    return new Buffer(`\0${this._credentials.username}\0${this._credentials.password}`).toString('base64');
  }
}


const bufferFromString = (Int8Array.from !== Buffer.from && Buffer.from) || function newBuffer(text) {
  return new Buffer(text, 'utf8');
};

module.exports = SaslAuthenticator;