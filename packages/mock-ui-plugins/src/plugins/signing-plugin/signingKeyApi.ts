/**
 * Browser signing API using Web Crypto.
 *
 * Key pair is ECDSA P-256, stored in IndexedDB as non-extractable CryptoKey.
 * The signature format matches the CLI: ASN.1 DER over the canonical envelope.
 *
 * Web Crypto's sign({hash:"SHA-256"}, key, data) hashes internally, so we
 * pass the raw envelope bytes — same end result as the CLI's pre-hash +
 * SignASN1 approach.
 */

const DB_NAME = "fleetshift-signing";
const STORE_NAME = "keys";
const KEY_ID = "signing-key";

// --- IndexedDB helpers ---

function openDB(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, 1);
    req.onupgradeneeded = () => {
      req.result.createObjectStore(STORE_NAME);
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function storeKeyPair(keyPair: CryptoKeyPair): Promise<void> {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, "readwrite");
    tx.objectStore(STORE_NAME).put(keyPair, KEY_ID);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

async function loadKeyPair(): Promise<CryptoKeyPair | null> {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, "readonly");
    const req = tx.objectStore(STORE_NAME).get(KEY_ID);
    req.onsuccess = () => resolve(req.result ?? null);
    req.onerror = () => reject(req.error);
  });
}

async function deleteKeyPair(): Promise<void> {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE_NAME, "readwrite");
    tx.objectStore(STORE_NAME).delete(KEY_ID);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
  });
}

// --- Key generation ---

/** Generate an ECDSA P-256 key pair, store in IndexedDB, return SSH public key string. */
export async function generateSigningKey(): Promise<string> {
  const keyPair = await crypto.subtle.generateKey(
    { name: "ECDSA", namedCurve: "P-256" },
    false, // non-extractable: JS cannot read the private key material
    ["sign", "verify"],
  );
  // Public keys can always be exported regardless of the extractable flag.
  await storeKeyPair(keyPair);
  return exportSSHPublicKey(keyPair.publicKey);
}

/** Check whether a signing key exists in IndexedDB. */
export async function getSigningKeyStatus(): Promise<{
  enrolled: boolean;
  sshPublicKey?: string;
}> {
  const kp = await loadKeyPair();
  if (!kp) return { enrolled: false };
  const sshPub = await exportSSHPublicKey(kp.publicKey);
  return { enrolled: true, sshPublicKey: sshPub };
}

/** Delete the signing key from IndexedDB. */
export async function removeSigningKey(): Promise<void> {
  await deleteKeyPair();
}

// --- Signing ---

/**
 * Sign deployment envelope bytes. Returns base64-encoded ASN.1 DER signature.
 *
 * Pass the raw canonical envelope string as UTF-8 bytes. Web Crypto hashes
 * internally with SHA-256 before signing.
 */
export async function signDeployment(
  envelopeBytes: Uint8Array,
): Promise<string> {
  const kp = await loadKeyPair();
  if (!kp) throw new Error("No signing key found — enroll first");

  const rawSig = await crypto.subtle.sign(
    { name: "ECDSA", hash: "SHA-256" },
    kp.privateKey,
    envelopeBytes.buffer as ArrayBuffer,
  );

  const derSig = rawSignatureToASN1(new Uint8Array(rawSig));
  return uint8ToBase64(derSig);
}

// --- Public key export ---

/**
 * Export CryptoKey as SSH authorized-key format:
 *   ecdsa-sha2-nistp256 AAAA...
 */
export async function exportSSHPublicKey(
  publicKey: CryptoKey,
): Promise<string> {
  // "raw" export gives 65 bytes: 0x04 || X(32) || Y(32)
  const rawBytes = new Uint8Array(
    await crypto.subtle.exportKey("raw", publicKey),
  );

  const keyType = new TextEncoder().encode("ecdsa-sha2-nistp256");
  const curveName = new TextEncoder().encode("nistp256");

  // SSH wire format: each field is uint32be length + data
  const wireLen =
    4 + keyType.length + 4 + curveName.length + 4 + rawBytes.length;
  const wire = new Uint8Array(wireLen);
  const view = new DataView(wire.buffer);
  let offset = 0;

  view.setUint32(offset, keyType.length);
  offset += 4;
  wire.set(keyType, offset);
  offset += keyType.length;

  view.setUint32(offset, curveName.length);
  offset += 4;
  wire.set(curveName, offset);
  offset += curveName.length;

  view.setUint32(offset, rawBytes.length);
  offset += 4;
  wire.set(rawBytes, offset);

  return `ecdsa-sha2-nistp256 ${uint8ToBase64(wire)}`;
}

/**
 * Export public key as base64 DER (SPKI) for IdP user attribute storage.
 */
export async function exportPublicKeyDER(
  publicKey: CryptoKey,
): Promise<string> {
  const spki = new Uint8Array(await crypto.subtle.exportKey("spki", publicKey));
  return uint8ToBase64(spki);
}

/**
 * Get the stored public key CryptoKey, if any.
 * Used by enrollment to write to IdP or export for GitHub.
 */
export async function getStoredPublicKey(): Promise<CryptoKey | null> {
  const kp = await loadKeyPair();
  return kp?.publicKey ?? null;
}

// --- ASN.1 DER conversion ---

/**
 * Convert a 64-byte raw ECDSA signature (r||s) to ASN.1 DER format.
 *
 * Web Crypto returns P-256 signatures as 64 bytes: r(32) || s(32).
 * The server expects ASN.1 DER: SEQUENCE { INTEGER r, INTEGER s }.
 */
export function rawSignatureToASN1(raw: Uint8Array): Uint8Array {
  if (raw.length !== 64) {
    throw new Error(`Expected 64-byte raw signature, got ${raw.length}`);
  }

  const r = raw.slice(0, 32);
  const s = raw.slice(32, 64);

  const rDer = integerToDER(r);
  const sDer = integerToDER(s);

  // SEQUENCE tag (0x30) + length + contents
  const seqLen = rDer.length + sDer.length;
  const result = new Uint8Array(2 + seqLen);
  result[0] = 0x30; // SEQUENCE
  result[1] = seqLen;
  result.set(rDer, 2);
  result.set(sDer, 2 + rDer.length);

  return result;
}

/** Encode a big-endian unsigned integer as a DER INTEGER. */
function integerToDER(value: Uint8Array): Uint8Array {
  // Strip leading zeros
  let start = 0;
  while (start < value.length - 1 && value[start] === 0) {
    start++;
  }
  const trimmed = value.slice(start);

  // If high bit is set, prepend 0x00 to keep it positive
  const needsPad = trimmed[0] & 0x80;
  const len = trimmed.length + (needsPad ? 1 : 0);

  const result = new Uint8Array(2 + len);
  result[0] = 0x02; // INTEGER tag
  result[1] = len;
  if (needsPad) {
    result[2] = 0x00;
    result.set(trimmed, 3);
  } else {
    result.set(trimmed, 2);
  }

  return result;
}

// --- Base64 helpers ---

function uint8ToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}
