import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import type { ServerResponse } from "node:http";
import https from "node:https";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { BODY_LIMIT, requestHTTPS } from "./https";

async function temporaryCertificate(): Promise<{
  ca: Buffer;
  cert: Buffer;
  dir: string;
  key: Buffer;
}> {
  const dir = await mkdtemp(path.join(tmpdir(), "fleetshift-https-"));
  const keyFile = path.join(dir, "key.pem");
  const certFile = path.join(dir, "cert.pem");
  execFileSync("openssl", [
    "req",
    "-x509",
    "-newkey",
    "ec",
    "-pkeyopt",
    "ec_paramgen_curve:P-256",
    "-keyout",
    keyFile,
    "-out",
    certFile,
    "-days",
    "1",
    "-nodes",
    "-subj",
    "/CN=localhost",
    "-addext",
    "subjectAltName=DNS:localhost,IP:127.0.0.1",
  ]);
  const pem = await readFile(certFile);
  return { ca: pem, cert: pem, dir, key: await readFile(keyFile) };
}

function listen(
  tls: { cert: Buffer; key: Buffer },
  handler: (res: ServerResponse) => void,
): Promise<{ close: () => Promise<void>; url: string }> {
  const server = https.createServer(tls, (_req, res) => {
    handler(res);
  });
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        reject(new Error("expected TCP listen address"));
        return;
      }
      resolve({
        close: () =>
          new Promise((done, fail) => {
            server.closeAllConnections();
            server.close((error) => (error ? fail(error) : done()));
          }),
        url: `https://127.0.0.1:${address.port}/`,
      });
    });
  });
}

describe("requestHTTPS", () => {
  let tls: { ca: Buffer; cert: Buffer; dir: string; key: Buffer };

  beforeAll(async () => {
    tls = await temporaryCertificate();
  });

  afterAll(async () => {
    await rm(tls.dir, { force: true, recursive: true });
  });

  it("returns a small HTTPS body", async () => {
    const server = await listen(tls, (res) => {
      res.writeHead(200);
      res.end("ok");
    });
    try {
      const response = await requestHTTPS(server.url, { ca: tls.ca });
      expect(response.status).toBe(200);
      expect(response.body.toString("utf8")).toBe("ok");
    } finally {
      await server.close();
    }
  });

  it("rejects a complete response of BODY_LIMIT + 1 bytes", async () => {
    const server = await listen(tls, (res) => {
      res.writeHead(200);
      res.end(Buffer.alloc(BODY_LIMIT + 1, 0x61));
    });
    try {
      await expect(requestHTTPS(server.url, { ca: tls.ca })).rejects.toThrow(
        /exceeded/,
      );
    } finally {
      await server.close();
    }
  });

  it(
    "rejects an actively streaming response over BODY_LIMIT",
    { timeout: 5_000 },
    async () => {
      const server = await listen(tls, (res) => {
        res.writeHead(200);
        res.write(Buffer.alloc(BODY_LIMIT, 0x61));
        const timer = setInterval(() => {
          res.write("x");
        }, 20);
        res.once("close", () => clearInterval(timer));
      });
      try {
        await expect(
          requestHTTPS(server.url, { ca: tls.ca, timeoutMs: 15_000 }),
        ).rejects.toThrow(/exceeded/);
      } finally {
        await server.close();
      }
    },
  );
});
