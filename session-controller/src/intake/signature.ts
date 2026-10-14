import { createHmac, timingSafeEqual } from 'node:crypto';
import { ControllerError, systemClock, type Clock } from '../domain.js';
export interface SignatureHeaders { readonly timestamp: string | undefined; readonly signature: string | undefined }
export function createSignatureVerifier(secret: string, freshnessSeconds: number, clock: Clock = systemClock): (body: Buffer, headers: SignatureHeaders) => void {
  if (secret.trim() === '' || secret === 'INSECURE_NO_AUTH' || Buffer.byteLength(secret, 'utf8') < 32 || !Number.isSafeInteger(freshnessSeconds) || freshnessSeconds < 1 || freshnessSeconds > 3600) throw new ControllerError('config_invalid');
  const key = Buffer.from(secret, 'utf8');
  return (body, headers) => {
    const t = headers.timestamp, s = headers.signature;
    if (t === undefined || s === undefined || !/^(0|[1-9]\d{0,11})$/.test(t) || !/^[0-9a-f]{64}$/.test(s)) throw new ControllerError('signature_invalid');
    const signedAt = Number(t), now = Math.floor(clock.now() / 1000);
    if (!Number.isSafeInteger(now) || Math.abs(now - signedAt) > freshnessSeconds) throw new ControllerError('signature_invalid');
    const actual = Buffer.from(s, 'hex');
    const expected = createHmac('sha256', key).update(t + '.', 'utf8').update(body).digest();
    // Shape checking guarantees equal 32-byte operands before constant-time comparison.
    if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) throw new ControllerError('signature_invalid');
  };
}
