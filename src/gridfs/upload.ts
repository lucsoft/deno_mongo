import {
  BucketInfo,
  Chunk,
  File,
  GridFSUploadOptions,
} from "../types/gridfs.ts";
import { ObjectId } from "../../deps.ts";
import { Collection } from "../collection/mod.ts";

export class GridFSUploadStreamProcessor {
  #chunksCollection: Collection<Chunk>;
  #filesCollection: Collection<File>;
  #filename: string;
  #options: GridFSUploadOptions | undefined;
  #chunkSizeBytes: number;
  #uploadBuffer: Uint8Array;
  #bufferPosition: number;
  #chunksInserted: number;
  #fileSizeBytes: number;
  #files_id: ObjectId;

  //TODO documentation
  constructor(
    bucketInfo: BucketInfo,
    filename: string,
    options?: GridFSUploadOptions,
  ) {
    this.#filesCollection = bucketInfo.filesCollection;
    this.#chunksCollection = bucketInfo.chunksCollection;

    this.#filename = filename;
    this.#chunkSizeBytes = options?.chunkSizeBytes ?? bucketInfo.chunkSizeBytes;
    this.#uploadBuffer = new Uint8Array(new ArrayBuffer(this.#chunkSizeBytes));
    this.#bufferPosition = 0;
    this.#chunksInserted = 0;
    this.#fileSizeBytes = 0;
    this.#files_id = new ObjectId();
  }

  #processChunk(chunk: ArrayBuffer): Promise<void> {
    return new Promise((resolve) => {
      const int8 = new Uint8Array(chunk);
      const spaceRemaining = this.#chunkSizeBytes - this.#bufferPosition;
      if (chunk.byteLength < spaceRemaining) {
        this.#uploadBuffer.set(int8, this.#bufferPosition);
        this.#bufferPosition += chunk.byteLength;
        this.#fileSizeBytes += chunk.byteLength;
        return resolve();
      } else {
        const sliced = int8.slice(0, spaceRemaining);
        const remnant = int8.slice(spaceRemaining);

        this.#uploadBuffer.set(sliced, this.#bufferPosition);

        this.#chunksCollection.insertOne({
          "files_id": this.#files_id,
          n: this.#chunksInserted,
          data: this.#uploadBuffer,
        });

        this.#bufferPosition = 0;
        this.#fileSizeBytes += sliced.byteLength;
        ++this.#chunksInserted;

        return resolve(this.#processChunk(remnant));
      }
    });
  }

  #handleStreamFinalization() {
    // Write the last bytes that are left in the buffer
    if (this.#bufferPosition) {
      this.#chunksCollection.insertOne({
        "files_id": this.#files_id,
        n: this.#chunksInserted,
        data: this.#uploadBuffer.slice(0, this.#bufferPosition),
      });
    }

    this.#filesCollection.insertOne(
      this.#createFileDocument(),
    );
  }

  get writable() {
    return new WritableStream({
      write: this.#processChunk.bind(this),
      close: this.#handleStreamFinalization.bind(this),
    });
  }

  #createFileDocument(): File {
    return {
      _id: this.#files_id,
      chunkSize: this.#chunkSizeBytes,
      filename: this.#filename,
      uploadDate: new Date(),
      length: this.#fileSizeBytes,
      metadata: this.#options?.metadata,
    };
  }
}
