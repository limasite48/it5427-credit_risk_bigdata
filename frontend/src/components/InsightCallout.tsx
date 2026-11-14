import type { ReactNode } from 'react'
import { cn } from '@/lib/utils'

export function InsightCallout({
  title,
  children,
  tone = 'neutral',
}: {
  title: string
  children: ReactNode
  tone?: 'neutral' | 'positive' | 'caution' | 'negative'
}) {
  return (
    <div
      className={cn(
        'rounded-md border px-3 py-2.5',
        tone === 'neutral' && 'border-border bg-white',
        tone === 'positive' && 'border-emerald-200 bg-emerald-50/60',
        tone === 'caution' && 'border-amber-200 bg-amber-50/60',
        tone === 'negative' && 'border-red-200 bg-red-50/60',
      )}
    >
      <div className="text-[11px] font-semibold uppercase tracking-wide text-navy-muted">{title}</div>
      <div className="mt-1 text-sm text-navy">{children}</div>
    </div>
  )
}
