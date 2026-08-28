import { readFile } from "node:fs/promises";
import https from "node:https";

export interface HTTPSResponse {
  body: Buffer;
  status: number;
}

interface HTTPSRequestOptions {
  body?: Buffer | string;
  ca: Buffer | string;
  headers?: Record<string, string>;
  method?: string;
  timeoutMs?: number;
}

const BODY_LIMIT = 1024 * 1024;

export async function requestHTTPS(
  url: string,
  options: HTTPSRequestOptions,
): Promise<HTTPSResponse> {
  return new Promise((resolve, reject) => {
    const request = https.request(url, {
      ca: options.ca,
      headers: options.headers,
      method: options.method ?? "GET",
      minVersion: "TLSv1.2",
      rejectUnauthorized: true,
      timeout: options.timeoutMs ?? 15_000,
    });
    request.once("timeout", () =>
      request.destroy(new Error("HTTPS request timed out")),
    );
    request.once("error", reject);
    request.once("response", (response) => {
      const chunks: Buffer[] = [];
      let size = 0;
      response.on("data", (chunk: Buffer) => {
        if (size >= BODY_LIMIT) return;
        const remaining = BODY_LIMIT - size;
        const kept =
          chunk.length > remaining ? chunk.subarray(0, remaining) : chunk;
        chunks.push(kept);
        size += kept.length;
      });
      response.once("end", () =>
        resolve({
          body: Buffer.concat(chunks),
          status: response.statusCode ?? 0,
        }),
      );
      response.once("error", reject);
    });
    if (options.body !== undefined) request.write(options.body);
    request.end();
  });
}

export async function requestWithCAFile(
  url: string,
  caFile: string,
  options: Omit<HTTPSRequestOptions, "ca"> = {},
): Promise<HTTPSResponse> {
  return requestHTTPS(url, { ...options, ca: await readFile(caFile) });
}
