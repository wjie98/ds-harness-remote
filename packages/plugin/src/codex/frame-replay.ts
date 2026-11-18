/**
 * Replayable terminal frames for the Codex domain.
 *
 * Deltas are deliberately excluded: the Client re-reads `dsh/sessionHistory`
 * when it reopens a Stream, so replaying deltas would append the same text
 * twice. `turn/started` is also excluded because a replay would advance the
 * Client-side turn counter past the turn history already contains.
 */
export const CODEX_REPLAY_METHODS: ReadonlySet<string> = new Set([
  'item/completed',
  'turn/completed',
  'thread/status/changed',
  'thread/name/updated',
])

export const CODEX_REPLAY_LIMIT = 200
export const CODEX_REPLAY_TTL_MS = 5 * 60_000

export interface CodexReplayFrame {
  method: string
  params: unknown
  at: number
}

export interface CodexReplayBatch {
  frames: CodexReplayFrame[]
  truncated: boolean
}

/**
 * Process-scoped replay cache.
 *
 * The cache lives on the domain, not on a Peer connection: the frames that a
 * reconnecting Client needs are produced while no Peer is subscribed, so a
 * per-connection buffer would be discarded exactly when it is needed.
 */
export class CodexFrameReplay {
  private readonly threads = new Map<string, CodexReplayFrame[]>()
  private readonly truncated = new Set<string>()

  constructor(
    private readonly limit: number = CODEX_REPLAY_LIMIT,
    private readonly ttlMs: number = CODEX_REPLAY_TTL_MS,
    private readonly now: () => number = Date.now,
  ) {}

  record(threadId: string, method: string, params: unknown): void {
    if (!CODEX_REPLAY_METHODS.has(method)) return
    const list = this.threads.get(threadId) ?? []
    list.push({ method, params, at: this.now() })
    if (list.length > this.limit) {
      list.shift()
      this.truncated.add(threadId)
    }
    this.threads.set(threadId, list)
  }

  /** Consumes the batch: a later reconnect falls back to history instead. */
  take(threadId: string): CodexReplayBatch {
    const list = this.threads.get(threadId)
    if (list === undefined) return { frames: [], truncated: false }
    this.threads.delete(threadId)
    const truncated = this.truncated.delete(threadId)
    const validAfter = this.now() - this.ttlMs
    const frames = list.filter(frame => frame.at >= validAfter)
    return { frames, truncated: truncated || frames.length < list.length }
  }

  clear(threadId: string): void {
    this.threads.delete(threadId)
    this.truncated.delete(threadId)
  }

  clearAll(): void {
    this.threads.clear()
    this.truncated.clear()
  }
}
