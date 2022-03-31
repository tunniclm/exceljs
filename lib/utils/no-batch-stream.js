const {Transform} = require('readable-stream');
const StringBuf = require('./string-buf');

class NoBatchStream extends Transform {
  constructor(options = {}) {
    super({ ...options, writableObjectMode: true });
  }

  _transform(chunk, encoding, callback) {
    if (chunk instanceof StringBuf) {
      callback(null, chunk.toBuffer());
    } else {
      callback(null, chunk);
    }
  }
}

module.exports = NoBatchStream;

