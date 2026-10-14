import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { createSignatureVerifier } from '../src/intake/signature.js';
const testKey = 'TEST_ONLY_NON_SENSITIVE_HMAC_FIXTURE';
const seconds = 2000000000;
const body = Buffer.from('{"test":"é"}');
function headers(timestamp = String(seconds), raw = body) {
  return { timestamp, signature: createHmac('sha256', testKey).update(timestamp + '.').update(raw).digest('hex') };
}
describe('raw-body HMAC and freshness', () => {
  const verify = createSignatureVerifier(testKey, 300, { now: () => seconds * 1000 });
  it('authenticates exact UTF-8 bytes, not parsed or reserialized JSON', () => {
    expect(() => verify(body, headers())).not.toThrow();
    expect(() => verify(Buffer.from('{ "test":"é"}'), headers())).toThrow('signature_invalid');
    expect(() => verify(body, headers(String(seconds), Buffer.from('{"test":"different"}')))).toThrow('signature_invalid');
  });
  it.each([-301, -300, 0, 300, 301])('enforces freshness at offset %i', offset => {
    const call = () => verify(body, headers(String(seconds + offset)));
    if (Math.abs(offset) <= 300) expect(call).not.toThrow(); else expect(call).toThrow('signature_invalid');
  });
  it.each(['', ' 2000000000', '+2000000000', '02000000000', '2000000000.0', '2e9', '-1', '9999999999999'])('rejects malformed timestamp shape', timestamp => {
    expect(() => verify(body, headers(timestamp))).toThrow('signature_invalid');
  });
  it.each(['', 'ab', '0'.repeat(63), '0'.repeat(65), 'g'.repeat(64), 'A'.repeat(64), '0'.repeat(64)])('rejects malformed or nonmatching digest without length errors', signature => {
    expect(() => verify(body, { timestamp: String(seconds), signature })).toThrow('signature_invalid');
  });
  it('rejects missing headers, stale replay and invalid clocks, but allows a stable fresh retry', () => {
    expect(() => verify(body, { timestamp: undefined, signature: undefined })).toThrow('signature_invalid');
    expect(() => verify(body, headers())).not.toThrow();
    expect(() => verify(body, headers())).not.toThrow();
    expect(() => createSignatureVerifier(testKey, 300, { now: () => (seconds + 301) * 1000 })(body, headers())).toThrow('signature_invalid');
    expect(() => createSignatureVerifier(testKey, 300, { now: () => NaN })(body, headers())).toThrow('signature_invalid');
  });
  it('fails closed on bypass keys, short keys and invalid freshness', () => {
    for (const key of ['', 'INSECURE_NO_AUTH', 'TEST_SHORT']) expect(() => createSignatureVerifier(key, 300)).toThrow('config_invalid');
    expect(() => createSignatureVerifier(testKey, 0)).toThrow('config_invalid');
  });
});
