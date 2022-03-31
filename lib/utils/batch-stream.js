const {Transform} = require('readable-stream');
const StringBuf = require('./string-buf');

// This stream understands how to consume StringBuf objects, Buffers and strings
// Short chunks will be consolidated into buffers batchSize in length for performance
class BatchStream extends Transform {
  constructor(options = {}) {
    super({ ...options, writableObjectMode: true });
    this.bufferSize = options.batchSize || 65536;
    this.writeIdx = 0;
    this.activeBuffer = Buffer.alloc(this.bufferSize);
  }

  _transform(chunk, encoding, callback) {
    try {
      const normalizedChunk = () => {
        if (chunk instanceof StringBuf) return chunk.toBuffer();
        if (typeof chunk === 'string') return Buffer.from(chunk, encoding);
        return chunk;
      };
      const writableChunk = normalizedChunk();

      // We expect the invariant that there is always at least one byte unused in the buffer at this point
      if (this.bufferSize <= this.writeIdx) throw new Error(`Unexpectedly no space in buffer (buffer size=${this.bufferSize}, write index=${this.writeIdx})`);

      const remainingBytes = this.bufferSize - this.writeIdx;
      const copyBytes = Math.min(writableChunk.length, remainingBytes);
      if (copyBytes > 0) {
        writableChunk.copy(this.activeBuffer, this.writeIdx, 0, copyBytes);
        this.writeIdx += copyBytes;

        if (copyBytes === remainingBytes) {
          // activeBuffer is full, so retire it
          this.push(this.activeBuffer);
          this.activeBuffer = Buffer.alloc(this.bufferSize);
          this.writeIdx = 0;

          const remainingChunkBytes = writableChunk.length - copyBytes;
          if (remainingChunkBytes >= this.bufferSize) {
            // remaining bytes are at least as big as the buffer, so might as well retire them all
            this.push(writableChunk.subarray(copyBytes));
          } else {
            // remaining bytes small enough to batch together with next chunk, so copy them into the
            // the buffer for next time
            writableChunk.copy(this.activeBuffer, this.writeIdx, copyBytes, copyBytes + remainingChunkBytes);
            this.writeIdx += remainingChunkBytes;
          }
        }
      }
      callback();
    } catch (err) {
      callback(err);
    }
  }

  _flush(callback) {
    // retire anything left in the buffer
    if (this.writeIdx > 0) {
      this.push(this.activeBuffer.subarray(0, this.writeIdx));
    }
    callback();
  }
}

module.exports = BatchStream;

