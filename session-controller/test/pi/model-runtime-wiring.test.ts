import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import { ModelRuntime } from '@earendil-works/pi-coding-agent';
import type { ControllerConfig } from '../../src/config.js';
import { createPromptModelRuntime, type PromptModelRuntimeOptions } from '../../src/main.js';

function config(agentDir: string): ControllerConfig {
  return { pi: { agentDir } } as ControllerConfig;
}

describe('prompt model runtime credential snapshot wiring', () => {
  it('uses an offline create-time refresh instead of suppressing snapshot initialization', async () => {
    const agentDir = '/synthetic/controller-agent';
    const supplied = {} as Awaited<ReturnType<typeof createPromptModelRuntime>>;
    const create = vi.fn(async (_options: PromptModelRuntimeOptions) => supplied);

    await expect(createPromptModelRuntime(config(agentDir), create)).resolves.toBe(supplied);
    expect(create).toHaveBeenCalledWith({
      authPath: join(agentDir, 'auth.json'),
      modelsPath: join(agentDir, 'models.json'),
      allowModelNetwork: false
    });
    expect(Object.hasOwn(create.mock.calls[0]![0], 'refreshOnCreate')).toBe(false);
  });

  it('publishes configured auth to the snapshot while retaining direct request auth', async () => {
    const root = await mkdtemp(join(tmpdir(), 'controller-model-runtime-'));
    const agentDir = join(root, 'agent');
    try {
      await mkdir(agentDir, { recursive: true });
      await writeFile(join(agentDir, 'models.json'), JSON.stringify({
        providers: {
          synthetic_provider: {
            name: 'Synthetic provider',
            baseUrl: 'http://127.0.0.1:1/v1',
            api: 'openai-completions',
            apiKey: 'SYNTHETIC_NON_SECRET',
            authHeader: true,
            models: [{
              id: 'synthetic-model', name: 'Synthetic model', reasoning: false, input: ['text'],
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
              contextWindow: 4096, maxTokens: 256
            }]
          }
        }
      }), { mode: 0o600 });

      const stale = await ModelRuntime.create({
        authPath: join(agentDir, 'auth.json'), modelsPath: join(agentDir, 'models.json'),
        allowModelNetwork: false, refreshOnCreate: false
      });
      const staleModel = stale.getModel('synthetic_provider', 'synthetic-model');
      expect(staleModel).toBeDefined();
      expect(stale.hasConfiguredAuth('synthetic_provider')).toBe(false);
      await expect(stale.getAuth(staleModel!)).resolves.toMatchObject({ auth: { apiKey: expect.any(String) } });

      const runtime = await createPromptModelRuntime(config(agentDir));
      const model = runtime.getModel('synthetic_provider', 'synthetic-model');
      expect(model).toBeDefined();
      expect(runtime.hasConfiguredAuth('synthetic_provider')).toBe(true);
      await expect(runtime.checkAuth('synthetic_provider')).resolves.toBeDefined();
      await expect(runtime.getAuth(model!)).resolves.toMatchObject({ auth: { apiKey: expect.any(String) } });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  });
});
