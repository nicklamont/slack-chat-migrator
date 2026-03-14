import { useState, useEffect, useRef } from 'react';

const FRAMES = [
  {
    phase: 'Validating',
    channels: { done: 0, total: 47 },
    stats: { spaces: 0, messages: 0, files: 0, reactions: 0, members: 0 },
    channel: null as string | null,
    channelProgress: null as { done: number; total: number } | null,
  },
  {
    phase: 'Creating spaces',
    channels: { done: 3, total: 47 },
    stats: { spaces: 12, messages: 0, files: 0, reactions: 0, members: 0 },
    channel: 'general',
    channelProgress: null,
  },
  {
    phase: 'Importing messages',
    channels: { done: 8, total: 47 },
    stats: { spaces: 47, messages: 3841, files: 94, reactions: 512, members: 218 },
    channel: 'engineering',
    channelProgress: { done: 1204, total: 4823 },
  },
  {
    phase: 'Importing messages',
    channels: { done: 19, total: 47 },
    stats: { spaces: 47, messages: 11207, files: 283, reactions: 1847, members: 218 },
    channel: 'product-launches',
    channelProgress: { done: 389, total: 1156 },
  },
  {
    phase: 'Importing messages',
    channels: { done: 34, total: 47 },
    stats: { spaces: 47, messages: 22184, files: 491, reactions: 3219, members: 218 },
    channel: 'design-reviews',
    channelProgress: { done: 712, total: 2043 },
  },
  {
    phase: 'Adding members',
    channels: { done: 44, total: 47 },
    stats: { spaces: 47, messages: 27634, files: 612, reactions: 4108, members: 847 },
    channel: 'random',
    channelProgress: { done: 41, total: 63 },
  },
  {
    phase: 'Complete',
    channels: { done: 47, total: 47 },
    stats: { spaces: 47, messages: 28491, files: 647, reactions: 4293, members: 1038 },
    channel: null,
    channelProgress: null,
  },
];

function ProgressBar({ done, total, label }: { done: number; total: number; label: string }) {
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  const filled = Math.round(pct / 2.5);
  const bar = '\u2588'.repeat(filled) + '\u2591'.repeat(40 - filled);
  return (
    <div className="text-[13px] leading-relaxed">
      <span className="font-bold">{label.padEnd(12)}</span>
      <span className="text-accent">{bar}</span>
      <span className="text-[#A8A29E]"> {pct.toString().padStart(3)}% {done.toLocaleString()}/{total.toLocaleString()}</span>
    </div>
  );
}

export default function TerminalDemo() {
  const [frame, setFrame] = useState(0);
  const [prefersReducedMotion, setPrefersReducedMotion] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    const mq = window.matchMedia('(prefers-reduced-motion: reduce)');
    setPrefersReducedMotion(mq.matches);
    if (mq.matches) {
      setFrame(FRAMES.length - 1);
      return;
    }

    intervalRef.current = setInterval(() => {
      setFrame(prev => {
        if (prev >= FRAMES.length - 1) {
          // Pause on final frame, then restart
          return 0;
        }
        return prev + 1;
      });
    }, 2000);

    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, []);

  const f = FRAMES[frame];
  const isComplete = f.phase === 'Complete';

  return (
    <div className="overflow-hidden rounded-xl border border-border shadow-2xl">
      {/* macOS title bar */}
      <div className="flex items-center gap-2 bg-[#2A2A2A] px-4 py-3">
        <span className="h-3 w-3 rounded-full bg-[#FF5F57]" />
        <span className="h-3 w-3 rounded-full bg-[#FEBC2E]" />
        <span className="h-3 w-3 rounded-full bg-[#28C840]" />
        <span className="ml-4 font-mono text-xs text-[#999]">slack-chat-migrator migrate</span>
      </div>

      {/* Terminal content */}
      <div className="bg-code p-4 sm:p-6 font-mono text-sm text-[#E8E6E3]">
        {/* Header */}
        <div className={`mb-4 rounded border px-3 py-2 text-center ${
          isComplete
            ? 'border-green-500/40 text-green-400'
            : 'border-accent/40 text-accent'
        }`}>
          {isComplete ? (
            <span className="font-bold">Migration Complete</span>
          ) : (
            <>
              <span className={`inline-block ${prefersReducedMotion ? '' : 'animate-pulse'}`}>
                {'\u25CF'}{' '}
              </span>
              Migrating Slack {'\u2192'} Google Chat &mdash;{' '}
              <span className="text-[#A8A29E]">{f.phase}</span>
            </>
          )}
        </div>

        {/* Progress bars — always rendered to maintain consistent height */}
        <ProgressBar done={f.channels.done} total={f.channels.total} label="Channels" />
        <div className={`mt-1 ${f.channel && f.channelProgress ? '' : 'invisible'}`}>
          <div className="text-[13px] text-cyan-400 mb-1">#{f.channel || 'general'}</div>
          <ProgressBar done={f.channelProgress?.done ?? 0} total={f.channelProgress?.total ?? 1} label="  Messages" />
        </div>

        {/* Stats table */}
        <div className="mt-4 border-t border-white/10 pt-3">
          <div className="grid grid-cols-2 gap-x-8 gap-y-1 text-[13px]">
            <div className="flex justify-between">
              <span className="text-cyan-400">Spaces created</span>
              <span className="text-green-400 tabular-nums">{f.stats.spaces.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-cyan-400">Files uploaded</span>
              <span className="text-green-400 tabular-nums">{f.stats.files.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-cyan-400">Messages sent</span>
              <span className="text-green-400 tabular-nums">{f.stats.messages.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-cyan-400">Reactions added</span>
              <span className="text-green-400 tabular-nums">{f.stats.reactions.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-cyan-400">Members added</span>
              <span className="text-green-400 tabular-nums">{f.stats.members.toLocaleString()}</span>
            </div>
            <div className={`flex justify-between ${isComplete ? 'invisible' : ''}`}>
              <span className="text-cyan-400">Throughput</span>
              <span className="text-green-400 tabular-nums">4.2 msgs/sec</span>
            </div>
          </div>
        </div>

        {/* Completion message — always rendered to maintain consistent height */}
        <div className={`mt-4 border-t border-white/10 pt-3 text-[13px] text-green-400 ${isComplete ? '' : 'invisible'}`}>
          {'\u2714'} 47 channels migrated successfully in 1h42m
        </div>
      </div>
    </div>
  );
}
