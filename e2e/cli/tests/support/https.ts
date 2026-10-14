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

/** Maximum response body `requestHTTPS` accepts; larger responses are rejected. */
export const BODY_LIMIT = 1024 * 1024;

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
    let settled = false;
    const fail = (error: Error) => {
      if (settled) return;
      settled = true;
      request.destroy();
      reject(error);
    };
    request.once("timeout", () => fail(new Error("HTTPS request timed out")));
    request.once("error", fail);
    request.once("response", (response) => {
      const chunks: Buffer[] = [];
      let size = 0;
      response.on("data", (chunk: Buffer) => {
        if (settled) return;
        size += chunk.length;
        if (size > BODY_LIMIT) {
          response.destroy();
          fail(new Error(`HTTPS response exceeded ${BODY_LIMIT} bytes`));
          return;
        }
        chunks.push(chunk);
      });
      response.once("end", () => {
        if (settled) return;
        settled = true;
        resolve({
          body: Buffer.concat(chunks),
          status: response.statusCode ?? 0,
        });
      });
      response.once("error", fail);
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
