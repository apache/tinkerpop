'use strict';

const SaslMechanismBase = require('./sasl-mechanism-base');

class SaslMechanismPlain extends SaslMechanismBase {
  get name() {
    return 'PLAIN';
  }
  
  evaluateChallenge(challenge) {
    if (this._hasInitialResponse(challenge)) {
      return Promise.resolve({ 'saslMechanism': this.name, 'sasl': this._saslArgument() });
    }
    
    return Promise.resolve({ 'sasl': this._saslArgument() });
  }

  _saslArgument() {
    if (this._options.username === undefined || this._options.username.length === 0 
      || this._options.password === undefined || this._options.password.length === 0 ) {
        throw new Error('No Credentials Supplied');
    }

    const authstr = ((this._options.authId !== undefined && this._options.authId.length) ? this._options.authId : '')
      + `\0${this._options.username}\0${this._options.password}`;
    return new Buffer(authstr).toString('base64');
  }

  _hasInitialResponse(challenge) {
    if (challenge === undefined || challenge === null) {
      return false;
    }
    return true;
  }
}

module.exports = SaslMechanismPlain;