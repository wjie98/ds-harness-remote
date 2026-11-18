import { describe, expect, it } from 'vitest'
import {
  CODEX_REPLAY_LIMIT,
  CodexFrameReplay,
} from '../src/codex/frame-replay.js'

describe('CodexFrameReplay', () => {
  it('keeps terminal frames and drops deltas and turn starts', () => {
    const replay = new CodexFrameReplay()
    replay.record('thread-1', 'turn/started', { turn: { id: 'turn-1' } })
    replay.record('thread-1', 'item/agentMessage/delta', { delta: 'x' })
    replay.record('thread-1', 'item/completed', { item: { id: 'item-1' } })
    replay.record('thread-1', 'turn/completed', { turn: { id: 'turn-1' } })

    const batch = replay.take('thread-1')
    expect(batch.frames.map(frame => frame.method)).toEqual([
      'item/completed',
      'turn/completed',
    ])
    expect(batch.truncated).toBe(false)
  })

  it('consumes the batch so a later reconnect falls back to history', () => {
    const replay = new CodexFrameReplay()
    replay.record('thread-1', 'turn/completed', {})
    expect(replay.take('thread-1').frames).toHaveLength(1)
    expect(replay.take('thread-1')).toEqual({ frames: [], truncated: false })
  })

  it('drops the oldest frames and reports truncation', () => {
    const replay = new CodexFrameReplay(2)
    replay.record('thread-1', 'item/completed', { n: 1 })
    replay.record('thread-1', 'item/completed', { n: 2 })
    replay.record('thread-1', 'item/completed', { n: 3 })

    const batch = replay.take('thread-1')
    expect(batch.frames.map(frame => (frame.params as { n: number }).n)).toEqual([2, 3])
    expect(batch.truncated).toBe(true)
  })

  it('keeps the default bound bounded', () => {
    const replay = new CodexFrameReplay()
    for (let index = 0; index < CODEX_REPLAY_LIMIT + 5; index += 1) {
      replay.record('thread-1', 'item/completed', { index })
    }
    expect(replay.take('thread-1').frames).toHaveLength(CODEX_REPLAY_LIMIT)
  })

  it('drops frames older than the TTL and reports truncation', () => {
    let now = 1_000
    const replay = new CodexFrameReplay(10, 1_000, () => now)
    replay.record('thread-1', 'turn/completed', {})
    now += 1_500
    const batch = replay.take('thread-1')
    expect(batch.frames).toHaveLength(0)
    expect(batch.truncated).toBe(true)
  })

  it('isolates threads and clears them individually or wholesale', () => {
    const replay = new CodexFrameReplay()
    replay.record('thread-1', 'turn/completed', {})
    replay.record('thread-2', 'turn/completed', {})
    replay.clear('thread-1')
    expect(replay.take('thread-1').frames).toHaveLength(0)
    expect(replay.take('thread-2').frames).toHaveLength(1)

    replay.record('thread-3', 'turn/completed', {})
    replay.clearAll()
    expect(replay.take('thread-3').frames).toHaveLength(0)
  })
})
