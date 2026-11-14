import { cn } from '@/lib/utils'

export function TierBadge({ tier }: { tier: string }) {
  const t = tier.toUpperCase()
  return (
    <span
      className={cn(
        'inline-flex items-center rounded px-2 py-0.5 text-xs font-semibold',
        t === 'LOW' && 'bg-emerald-50 text-low',
        t === 'MEDIUM' && 'bg-amber-50 text-medium',
        t === 'HIGH' && 'bg-red-50 text-high',
        !['LOW', 'MEDIUM', 'HIGH'].includes(t) && 'bg-slate-100 text-navy-muted',
      )}
    >
      {t}
    </span>
  )
}
