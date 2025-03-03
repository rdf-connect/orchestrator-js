import { Readable } from 'stream'

interface Reader {
  next(): AsyncIteratorObject<string>
  next_stream(): AsyncIteratorObject<Readable>
  done(): boolean
}

export default class ShaclValidator {
  constructor(reader: Reader) {
    console.log('ShaclValidator init', reader);
  }
}
