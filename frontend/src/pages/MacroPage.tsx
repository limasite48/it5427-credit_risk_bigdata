import { useMutation, useQuery } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { InsightCallout } from '@/components/InsightCallout'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { api } from '@/lib/api'
import { formatNumber, formatPercent } from '@/lib/utils'

export function MacroPage() {
  const regionsQuery = useQuery({
    queryKey: ['regions'],
    queryFn: () => api.regions(),
  })
  const [unempDelta, setUnempDelta] = useState(2)
  const [cpiShock, setCpiShock] = useState(3)
  const [ratingFilter, setRatingFilter] = useState<string>('')

  const stressMutation = useMutation({
    mutationFn: () =>
      api.macroStress({
        unemploymentDeltaPp: unempDelta,
        cpiShockPct: cpiShock,
        regionRating: ratingFilter ? Number(ratingFilter) : null,
      }),
  })

  const ratingChart = useMemo(() => {
    if (!regionsQuery.data) return []
    return regionsQuery.data.byRegionRating.map((r) => ({
      label: `Rating ${r.regionRating}`,
      highRate: Number((r.highRate * 100).toFixed(1)),
      avgPd: Number((r.averagePd * 100).toFixed(1)),
      unemp: Number(r.averageUnemployment.toFixed(2)),
      volume: r.applicationCount,
    }))
  }, [regionsQuery.data])

  const unempChart = useMemo(() => {
    if (!regionsQuery.data) return []
    return regionsQuery.data.byUnemploymentQuintile.map((b) => ({
      label: `${b.label}\n${b.binStart.toFixed(1)}-${b.binEnd.toFixed(1)}%`,
      short: b.label,
      highRate: Number((b.highRate * 100).toFixed(1)),
      avgPd: Number((b.averagePd * 100).toFixed(1)),
      unemp: b.averageMacroValue,
      volume: b.applicationCount,
    }))
  }, [regionsQuery.data])

  const gdpChart = useMemo(() => {
    if (!regionsQuery.data) return []
    return regionsQuery.data.byGdpQuintile.map((b) => ({
      label: b.label,
      highRate: Number((b.highRate * 100).toFixed(1)),
      avgPd: Number((b.averagePd * 100).toFixed(1)),
      gdp: b.averageMacroValue,
      volume: b.applicationCount,
    }))
  }, [regionsQuery.data])

  const topRegions = useMemo(() => {
    if (!regionsQuery.data) return []
    return [...regionsQuery.data.topRegions]
      .sort((a, b) => b.highRate - a.highRate)
      .slice(0, 10)
      .map((r) => ({
        code: r.regionCode,
        highRate: Number((r.highRate * 100).toFixed(1)),
        unemp: Number(r.averageUnemployment.toFixed(2)),
        volume: r.applicationCount,
      }))
  }, [regionsQuery.data])

  const stressCompare = useMemo(() => {
    const result = stressMutation.data
    if (!result) return []
    return [
      {
        tier: 'LOW',
        baseline: result.baselineTiers.low,
        stressed: result.stressedTiers.low,
      },
      {
        tier: 'MEDIUM',
        baseline: result.baselineTiers.medium,
        stressed: result.stressedTiers.medium,
      },
      {
        tier: 'HIGH',
        baseline: result.baselineTiers.high,
        stressed: result.stressedTiers.high,
      },
    ]
  }, [stressMutation.data])

  if (regionsQuery.isLoading) {
    return <p className="text-sm text-navy-muted">Loading macro & regional insights…</p>
  }
  if (regionsQuery.isError || !regionsQuery.data) {
    return <p className="text-sm text-high">Failed to load regional macro insights.</p>
  }

  const data = regionsQuery.data

  return (
    <div className="space-y-4">
      <div className="grid gap-3 lg:grid-cols-3">
        <InsightCallout title="Integrated macro join" tone="neutral">
          Matched <strong>{formatNumber(data.matchedWithMacro)}</strong> /{' '}
          {formatNumber(data.scoredSampleSize)} scored apps via {data.joinKey}.
        </InsightCallout>
        <InsightCallout title="Highest-risk rating band" tone="negative">
          {data.insights.highestRiskRating} carries a HIGH-tier rate of{' '}
          <strong>{formatPercent(data.insights.highestRiskRatingHighRate)}</strong>.
        </InsightCallout>
        <InsightCallout title="Unemployment signal" tone="caution">
          Bucket {data.insights.highestUnemploymentBucket} shows HIGH rate{' '}
          <strong>{formatPercent(data.insights.highestUnemploymentBucketHighRate)}</strong>. Rating
          HIGH-rate spread ≈ {formatPercent(data.insights.correlationHint)}.
        </InsightCallout>
      </div>

      <p className="text-xs text-navy-muted">{data.note}</p>

      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Risk by region rating</CardTitle>
            <CardDescription>HOME CREDIT REGION_RATING_CLIENT with joined unemployment</CardDescription>
          </CardHeader>
          <CardContent className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={ratingChart}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="label" tick={{ fill: '#3a4f6a', fontSize: 12 }} />
                <YAxis tick={{ fill: '#3a4f6a', fontSize: 11 }} />
                <Tooltip />
                <Legend />
                <Bar dataKey="highRate" name="HIGH rate %" fill="#b91c1c" radius={[4, 4, 0, 0]} />
                <Bar dataKey="avgPd" name="Avg PD %" fill="#1d4ed8" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>HIGH rate by unemployment quintile</CardTitle>
            <CardDescription>Joined latest regional unemployment (%)</CardDescription>
          </CardHeader>
          <CardContent className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={unempChart}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="short" tick={{ fill: '#3a4f6a', fontSize: 12 }} />
                <YAxis tick={{ fill: '#3a4f6a', fontSize: 11 }} />
                <Tooltip
                  formatter={(value, name) => [`${value}${String(name).includes('%') || String(name).includes('rate') || String(name).includes('PD') ? '' : ''}`, String(name)]}
                  labelFormatter={(_, payload) => {
                    const row = payload?.[0]?.payload as { label?: string } | undefined
                    return row?.label?.replace('\n', ' ') ?? ''
                  }}
                />
                <Legend />
                <Bar dataKey="highRate" name="HIGH rate %" fill="#b45309" radius={[4, 4, 0, 0]} />
                <Bar dataKey="avgPd" name="Avg PD %" fill="#93c5fd" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>HIGH rate by GDP growth quintile</CardTitle>
            <CardDescription>Joined latest regional GDP YoY growth</CardDescription>
          </CardHeader>
          <CardContent className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={gdpChart}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="label" tick={{ fill: '#3a4f6a', fontSize: 12 }} />
                <YAxis tick={{ fill: '#3a4f6a', fontSize: 11 }} />
                <Tooltip />
                <Legend />
                <Bar dataKey="highRate" name="HIGH rate %" fill="#b91c1c" radius={[4, 4, 0, 0]} />
                <Bar dataKey="gdp" name="Avg GDP YoY %" fill="#1d4ed8" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Top regions by HIGH-tier rate</CardTitle>
            <CardDescription>Among largest joined region codes in the scored sample</CardDescription>
          </CardHeader>
          <CardContent className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={topRegions} layout="vertical" margin={{ left: 16, right: 12 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis type="number" unit="%" tick={{ fill: '#3a4f6a', fontSize: 11 }} />
                <YAxis
                  type="category"
                  dataKey="code"
                  width={48}
                  tick={{ fill: '#3a4f6a', fontSize: 11 }}
                />
                <Tooltip
                  formatter={(value, name) => {
                    if (name === 'highRate') return [`${value}%`, 'HIGH rate']
                    return [value, String(name)]
                  }}
                />
                <Bar dataKey="highRate" name="HIGH rate %" fill="#b91c1c" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Macro stress simulator</CardTitle>
          <CardDescription>
            Apply unemployment / CPI shocks to scored PDs and re-tier (deterministic MVP — no model
            retrain)
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-3 sm:grid-cols-4">
            <div className="space-y-1.5">
              <Label htmlFor="unemp">Unemployment Δ (pp)</Label>
              <Input
                id="unemp"
                type="number"
                step="0.5"
                value={unempDelta}
                onChange={(e) => setUnempDelta(Number(e.target.value))}
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="cpi">CPI shock (%)</Label>
              <Input
                id="cpi"
                type="number"
                step="0.5"
                value={cpiShock}
                onChange={(e) => setCpiShock(Number(e.target.value))}
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="rating">Region rating filter</Label>
              <Input
                id="rating"
                placeholder="All (1/2/3)"
                value={ratingFilter}
                onChange={(e) => setRatingFilter(e.target.value)}
              />
            </div>
            <div className="flex items-end">
              <Button
                className="w-full"
                onClick={() => stressMutation.mutate()}
                disabled={stressMutation.isPending}
              >
                {stressMutation.isPending ? 'Simulating…' : 'Run stress'}
              </Button>
            </div>
          </div>

          {stressMutation.isError && (
            <p className="text-sm text-high">Stress simulation failed.</p>
          )}

          {stressMutation.data && (
            <>
              <div className="grid gap-3 sm:grid-cols-4">
                <Card>
                  <CardContent className="py-3">
                    <div className="text-xs uppercase text-navy-muted">In scope</div>
                    <div className="text-lg font-semibold">
                      {formatNumber(stressMutation.data.applicantsInScope)}
                    </div>
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="py-3">
                    <div className="text-xs uppercase text-navy-muted">Baseline HIGH rate</div>
                    <div className="text-lg font-semibold">
                      {formatPercent(stressMutation.data.baselineHighRate)}
                    </div>
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="py-3">
                    <div className="text-xs uppercase text-navy-muted">Stressed HIGH rate</div>
                    <div className="text-lg font-semibold text-high">
                      {formatPercent(stressMutation.data.stressedHighRate)}
                    </div>
                  </CardContent>
                </Card>
                <Card>
                  <CardContent className="py-3">
                    <div className="text-xs uppercase text-navy-muted">Δ HIGH count</div>
                    <div className="text-lg font-semibold">
                      {stressMutation.data.highTierDelta > 0 ? '+' : ''}
                      {stressMutation.data.highTierDelta}
                    </div>
                  </CardContent>
                </Card>
              </div>

              <div className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={stressCompare}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                    <XAxis dataKey="tier" tick={{ fill: '#3a4f6a', fontSize: 12 }} />
                    <YAxis tick={{ fill: '#3a4f6a', fontSize: 11 }} allowDecimals={false} />
                    <Tooltip />
                    <Legend />
                    <Bar dataKey="baseline" name="Baseline" fill="#94a3b8" radius={[4, 4, 0, 0]} />
                    <Bar dataKey="stressed" name="Stressed" fill="#b91c1c" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              <p className="text-xs text-navy-muted">{stressMutation.data.methodology}</p>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
