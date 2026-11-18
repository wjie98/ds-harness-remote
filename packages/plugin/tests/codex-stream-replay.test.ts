import { describe, expect, it, vi } from 'vitest'
import type { CodexAppServerInbound, CodexAppServerInboundHandler, CodexAppServerLike, CodexAppServerUnavailableHandler } from '../src/codex/app-server.js'
import { CodexFrameReplay } from '../src/codex/frame-replay.js'
import { CodexRemoteDomain } from '../src/codex/domain.js'
import { CodexPeerBridge } from '../src/codex/peer-bridge.js'
import type { SafeLogger } from '../src/logging.js'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

function logger(): SafeLogger {
  return { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() } as unknown as SafeLogger
}

interface Published {
  event: string
  data: unknown
}

function recorder(published: Published[]) {
  return async (event: string, data: unknown) => {
    published.push({ event, data })
  }
}

describe('Codex stream replay across reconnects', () => {
  it('replays terminal frames missed while no Stream was subscribed', async () => {
    const replay = new CodexFrameReplay()
    const domain = {
      useThread: (_threadId: string, action: () => Promise<unknown>) => action(),
      call: async () => ({ thread: { id: 'thread-1' } }),
      releaseThread: async () => undefined,
      takeReplay: (threadId: string) => replay.take(threadId),
    }
    const published: Published[] = []
    const bridge = new CodexPeerBridge(
      domain as never,
      { connectionId: 'peer-1' } as never,
      recorder(published) as never,
    )

    // The turn finished while the phone was away.
    replay.record('thread-1', 'item/completed', { item: { id: 'item-1', type: 'agentMessage' } })
    replay.record('thread-1', 'turn/completed', { turn: { id: 'turn-1' } })

    await bridge.openStream({ streamId: 'stream-1', threadId: 'thread-1' })

    const frames = published
      .filter(entry => entry.event === 'codex.app.frame')
      .map(entry => (entry.data as { frame: { method: string } }).frame.method)
    expect(frames).toEqual(['item/completed', 'turn/completed'])
  })

  it('replays only once and still delivers live frames afterwards', async () => {
    const replay = new CodexFrameReplay()
    const domain = {
      useThread: (_threadId: string, action: () => Promise<unknown>) => action(),
      call: async () => ({ thread: { id: 'thread-1' } }),
      releaseThread: async () => undefined,
      takeReplay: (threadId: string) => replay.take(threadId),
    }
    const published: Published[] = []
    const bridge = new CodexPeerBridge(
      domain as never,
      { connectionId: 'peer-1' } as never,
      recorder(published) as never,
    )

    replay.record('thread-1', 'turn/completed', { turn: { id: 'turn-1' } })
    await bridge.openStream({ streamId: 'stream-1', threadId: 'thread-1' })
    await bridge.publishInbound('thread-1', { method: 'thread/name/updated', params: { threadName: 'Title' } })

    const frames = published
      .filter(entry => entry.event === 'codex.app.frame')
      .map(entry => (entry.data as { frame: { method: string } }).frame.method)
    expect(frames).toEqual(['turn/completed', 'thread/name/updated'])
  })

  it('marks truncation with an in-band frame instead of a new event', async () => {
    const replay = new CodexFrameReplay(1)
    const domain = {
      useThread: (_threadId: string, action: () => Promise<unknown>) => action(),
      call: async () => ({ thread: { id: 'thread-1' } }),
      releaseThread: async () => undefined,
      takeReplay: (threadId: string) => replay.take(threadId),
    }
    const published: Published[] = []
    const bridge = new CodexPeerBridge(
      domain as never,
      { connectionId: 'peer-1' } as never,
      recorder(published) as never,
    )

    replay.record('thread-1', 'item/completed', { item: { id: 'item-1' } })
    replay.record('thread-1', 'turn/completed', { turn: { id: 'turn-1' } })
    await bridge.openStream({ streamId: 'stream-1', threadId: 'thread-1' })

    const methods = published
      .filter(entry => entry.event === 'codex.app.frame')
      .map(entry => (entry.data as { frame: { method: string } }).frame.method)
    expect(methods).toEqual(['turn/completed', 'codex/stream/truncated'])
    expect(published.every(entry => entry.event === 'codex.app.frame')).toBe(true)
  })
})

class FakeAppServer implements CodexAppServerLike {
  private inbound?: CodexAppServerInboundHandler
  private unavailable?: CodexAppServerUnavailableHandler
  private ready = false
  constructor(private readonly root: string) {}
  async start(): Promise<void> { this.ready = true }
  isReady(): boolean { return this.ready }
  async call(method: string, params: unknown): Promise<unknown> {
    if (method === 'account/read') return { account: { type: 'chatgpt', email: 'test@example.com' }, requiresOpenaiAuth: true }
    if (method === 'project/list') {
      return { data: [{ id: 'project-1', name: 'Project', roots: [{ path: this.root }], position: 0, createdAt: 1, updatedAt: 2 }], nextCursor: null }
    }
    if (method === 'thread/read') {
      const threadId = (params as { threadId?: string } | undefined)?.threadId
      return { thread: { id: threadId, cwd: this.root, turns: [] } }
    }
    if (method === 'thread/goal/get') return { goal: null }
    if (method === 'thread/unsubscribe') {
      queueMicrotask(() => this.emit({ kind: 'notification', method: 'thread/closed', params }))
      return { status: 'unsubscribed' }
    }
    return {}
  }
  async respond(): Promise<void> {}
  async respondError(): Promise<void> {}
  onInbound(handler: CodexAppServerInboundHandler): () => void {
    this.inbound = handler
    return () => { this.inbound = undefined }
  }
  onUnavailable(handler: CodexAppServerUnavailableHandler): () => void {
    this.unavailable = handler
    return () => { this.unavailable = undefined }
  }
  async close(): Promise<void> { this.ready = false }
  emit(message: CodexAppServerInbound): void { this.inbound?.(message) }
}

const tick = () => new Promise(resolve => setTimeout(resolve, 0))

describe('CodexRemoteDomain replay bookkeeping', () => {
  it('records terminal notifications and clears them when the thread closes', async () => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-codex-replay-'))
    try {
      const app = new FakeAppServer(root)
      const domain = new CodexRemoteDomain({ enabled: true, binary: 'codex' }, logger(), () => app)
      await domain.start()

      app.emit({ kind: 'notification', method: 'turn/started', params: { threadId: 'thread-1', turn: { id: 'turn-1' } } })
      app.emit({ kind: 'notification', method: 'item/agentMessage/delta', params: { threadId: 'thread-1', delta: 'x' } })
      app.emit({ kind: 'notification', method: 'turn/completed', params: { threadId: 'thread-1', turn: { id: 'turn-1' } } })
      await tick()

      const batch = domain.takeReplay('thread-1')
      expect(batch.frames.map(frame => frame.method)).toEqual(['turn/completed'])
      expect(batch.truncated).toBe(false)

      app.emit({ kind: 'notification', method: 'thread/closed', params: { threadId: 'thread-1' } })
      await tick()
      expect(domain.takeReplay('thread-1').frames).toHaveLength(0)
      await domain.close()
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  })
})
