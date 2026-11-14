import { useQuery } from '@tanstack/react-query'
import { useMemo } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { InsightCallout } from '@/components/InsightCallout'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/lib/api'
import { formatNumber, formatPercent } from '@/lib/utils'
import type { SegmentBucket } from '@/types/api'

function rateColor(rate: number, benchmark: number) {
  if (rate >= benchmark * 1.25) return '#b91c1c'
  if (rate >= benchmark) return '#b45309'
  return '#15803d'
}

function SegmentRateChart({
  title,
  description,
  buckets,
  benchmark,
  limit,
}: {
  title: string
  description: string
  buckets: SegmentBucket[]
  benchmark: number
  limit?: number
}) {
  const data = (limit ? buckets.slice(0, limit) : buckets).map((b) => ({
    label: b.label,
    rate: Number((b.defaultRate * 100).toFixed(2)),
    rawRate: b.defaultRate,
    volume: b.totalCount,
    defaults: b.defaultCount,
  }))

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="h-80">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} layout="vertical" margin={{ left: 28, right: 16 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis type="number" unit="%" tick={{ fill: '#3a4f6a', fontSize: 11 }} />
            <YAxis
              type="category"
              dataKey="label"
              width={128}
              tick={{ fill: '#3a4f6a', fontSize: 11 }}
            />
            <Tooltip
              formatter={(value, name) => {
                if (name === 'rate') return [`${value}%`, 'Default rate']
                return [value, String(name)]
              }}
              labelFormatter={(label) => String(label)}
            />
            <Bar dataKey="rate" radius={[0, 4, 4, 0]}>
              {data.map((entry) => (
                <Cell key={entry.label} fill={rateColor(entry.rawRate, benchmark)} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}

function SegmentVolumeChart({
  title,
  description,
  buckets,
}: {
  title: string
  description: string
  buckets: SegmentBucket[]
}) {
  const data = [...buckets]
    .sort((a, b) => b.totalCount - a.totalCount)
    .slice(0, 8)
    .map((b) => ({
      label: b.label,
      volume: b.totalCount,
      defaults: b.defaultCount,
    }))

  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="h-80">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="label"
              tick={{ fill: '#3a4f6a', fontSize: 10 }}
              interval={0}
              angle={-25}
              textAnchor="end"
              height={70}
            />
            <YAxis tick={{ fill: '#3a4f6a', fontSize: 11 }} allowDecimals={false} />
            <Tooltip formatter={(value) => formatNumber(Number(value))} />
            <Legend />
            <Bar dataKey="volume" name="Applicants" fill="#1d4ed8" radius={[4, 4, 0, 0]} />
            <Bar dataKey="defaults" name="Defaults" fill="#b91c1c" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  )
}

export function SegmentsPage() {
  const query = useQuery({
    queryKey: ['segments'],
    queryFn: () => api.segments(),
  })
  const overviewQuery = useQuery({
    queryKey: ['overview'],
    queryFn: () => api.overview(),
  })

  const insights = useMemo(() => {
    if (!query.data) return null
    const occ = query.data.byOccupation.filter((b) => b.totalCount >= 20)
    const highest = [...occ].sort((a, b) => b.defaultRate - a.defaultRate)[0]
    const lowest = [...occ].sort((a, b) => a.defaultRate - b.defaultRate)[0]
    const largest = [...query.data.byOccupation].sort((a, b) => b.totalCount - a.totalCount)[0]
    const ext = [...query.data.byExtSource2].sort((a, b) => b.defaultRate - a.defaultRate)
    return { highest, lowest, largest, worstExt: ext[0], bestExt: ext[ext.length - 1] }
  }, [query.data])

  if (query.isLoading) {
    return <p className="text-sm text-navy-muted">Loading segment analytics…</p>
  }
  if (query.isError || !query.data) {
    return <p className="text-sm text-high">Failed to load segment analytics.</p>
  }

  const data = query.data
  const benchmark = overviewQuery.data?.nplRate ?? 0.0772

  return (
    <div className="space-y-4">
      {insights && (
        <div className="grid gap-3 lg:grid-cols-3">
          <InsightCallout title="Highest-risk occupation" tone="negative">
            <strong>{insights.highest.label}</strong> defaults at{' '}
            {insights.highest.defaultPercent} (n={formatNumber(insights.highest.totalCount)};
            occupations with n≥20).
          </InsightCallout>
          <InsightCallout title="Lowest-risk occupation" tone="positive">
            <strong>{insights.lowest.label}</strong> defaults at{' '}
            {insights.lowest.defaultPercent} (n={formatNumber(insights.lowest.totalCount)}).
          </InsightCallout>
          <InsightCallout title="External score signal" tone="caution">
            {insights.worstExt.label} default rate {insights.worstExt.defaultPercent} vs{' '}
            {insights.bestExt.label} at {insights.bestExt.defaultPercent}. EXT_SOURCE_2 remains a
            strong separator.
          </InsightCallout>
        </div>
      )}

      <div className="rounded-md border border-border bg-white px-3 py-2 text-xs text-navy-muted">
        Bar colors vs portfolio NPL benchmark ({formatPercent(benchmark)}): green below, amber at/above,
        red ≥1.25× benchmark.
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <SegmentRateChart
          title="Default rate by occupation"
          description="Top occupations by default rate (color vs portfolio NPL)"
          buckets={data.byOccupation}
          benchmark={benchmark}
          limit={12}
        />
        <SegmentVolumeChart
          title="Occupation volume & defaults"
          description="Largest occupation groups by applicant count"
          buckets={data.byOccupation}
        />
        <SegmentRateChart
          title="Default rate by income type"
          description="Income category risk vs portfolio NPL"
          buckets={data.byIncomeType}
          benchmark={benchmark}
        />
        <SegmentRateChart
          title="Default rate by education"
          description="Education level risk"
          buckets={data.byEducation}
          benchmark={benchmark}
        />
        <SegmentRateChart
          title="Default rate by EXT_SOURCE_2 quintile"
          description="External bureau-style score bins"
          buckets={data.byExtSource2}
          benchmark={benchmark}
        />
        <Card>
          <CardHeader>
            <CardTitle>Segment concentration</CardTitle>
            <CardDescription>Where portfolio volume and default mass sit</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            {insights && (
              <>
                <div className="rounded-md border border-border px-3 py-2">
                  <div className="text-xs uppercase text-navy-muted">Largest occupation cohort</div>
                  <div className="mt-1 font-medium">
                    {insights.largest.label} · {formatNumber(insights.largest.totalCount)} apps ·{' '}
                    {insights.largest.defaultPercent} default
                  </div>
                </div>
                <div className="rounded-md border border-border px-3 py-2">
                  <div className="text-xs uppercase text-navy-muted">Income-type leader</div>
                  <div className="mt-1 font-medium">
                    {data.byIncomeType[0]?.label}: {data.byIncomeType[0]?.defaultPercent} default
                    rate on {formatNumber(data.byIncomeType[0]?.totalCount ?? 0)} apps
                  </div>
                </div>
                <div className="rounded-md border border-border px-3 py-2">
                  <div className="text-xs uppercase text-navy-muted">Education spread</div>
                  <div className="mt-1 font-medium">
                    {data.byEducation[0]?.label} ({data.byEducation[0]?.defaultPercent}) vs{' '}
                    {data.byEducation[data.byEducation.length - 1]?.label} (
                    {data.byEducation[data.byEducation.length - 1]?.defaultPercent})
                  </div>
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
