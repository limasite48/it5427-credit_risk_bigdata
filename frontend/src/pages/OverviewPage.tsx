import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { useEffect, useMemo, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { InsightCallout } from '@/components/InsightCallout'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/lib/api'
import { formatNumber, formatPercent } from '@/lib/utils'

const TIER_COLORS = {
  LOW: '#15803d',
  MEDIUM: '#b45309',
  HIGH: '#b91c1c',
} as const

export function OverviewPage() {
  const overviewQuery = useQuery({
    queryKey: ['overview'],
    queryFn: () => api.overview(),
  })
  const pipelineQuery = useQuery({
    queryKey: ['pipeline'],
    queryFn: () => api.pipeline(),
  })
  const [rocUrl, setRocUrl] = useState<string | null>(null)

  useEffect(() => {
    let active = true
    let objectUrl: string | null = null
    api
      .rocCurveUrl()
      .then((url) => {
        objectUrl = url
        if (active) setRocUrl(url)
      })
      .catch(() => {
        if (active) setRocUrl(null)
      })
    return () => {
      active = false
      if (objectUrl) URL.revokeObjectURL(objectUrl)
    }
  }, [])

  const data = overviewQuery.data

  const tierPie = useMemo(() => {
    if (!data) return []
    return [
      { name: 'LOW', value: data.riskDistribution.low },
      { name: 'MEDIUM', value: data.riskDistribution.medium },
      { name: 'HIGH', value: data.riskDistribution.high },
    ]
  }, [data])

  const outcomeBars = useMemo(() => {
    if (!data) return []
    return [
      { label: 'Defaults', count: data.defaultCount },
      { label: 'Non-defaults', count: data.nonDefaultCount },
    ]
  }, [data])

  const financialCompare = useMemo(() => {
    if (!data) return []
    return [
      {
        metric: 'Loan',
        average: Math.round(data.averageLoanAmount),
        median: Math.round(data.medianLoanAmount),
      },
      {
        metric: 'Income',
        average: Math.round(data.averageIncome),
        median: Math.round(data.medianIncome),
      },
    ]
  }, [data])

  if (overviewQuery.isLoading) {
    return <p className="text-sm text-navy-muted">Loading portfolio metrics…</p>
  }
  if (overviewQuery.isError || !data) {
    return <p className="text-sm text-high">Failed to load portfolio overview.</p>
  }

  const highShare =
    data.scoredSampleSize > 0 ? data.riskDistribution.high / data.scoredSampleSize : 0
  const lowShare =
    data.scoredSampleSize > 0 ? data.riskDistribution.low / data.scoredSampleSize : 0
  const aucLift =
    data.modelValAuc != null && data.baselineValAuc != null
      ? data.modelValAuc - data.baselineValAuc
      : null

  const kpis = [
    { label: 'NPL rate', value: data.nplPercent || formatPercent(data.nplRate) },
    { label: 'Applications', value: formatNumber(data.totalApplications) },
    { label: 'Defaults', value: formatNumber(data.defaultCount) },
    { label: 'Avg loan', value: formatNumber(data.averageLoanAmount) },
    { label: 'Avg income', value: formatNumber(data.averageIncome) },
    { label: 'Credit / income', value: `${data.averageCreditToIncomeRatio.toFixed(2)}x` },
  ]

  return (
    <div className="space-y-4">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
        {kpis.map((kpi) => (
          <Card key={kpi.label}>
            <CardContent className="py-4">
              <div className="text-xs uppercase tracking-wide text-navy-muted">{kpi.label}</div>
              <div className="mt-1 text-xl font-semibold tabular-nums">{kpi.value}</div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid gap-3 lg:grid-cols-2 xl:grid-cols-4">
        <InsightCallout title="Portfolio quality" tone={data.nplRate > 0.08 ? 'caution' : 'neutral'}>
          Observed NPL is <strong>{data.nplPercent}</strong> on{' '}
          {formatNumber(data.totalApplications)} applications (
          {formatNumber(data.defaultCount)} defaults).
        </InsightCallout>
        <InsightCallout title="Scored-book mix" tone={highShare > 0.4 ? 'caution' : 'neutral'}>
          In the scored sample (n={formatNumber(data.scoredSampleSize)}),{' '}
          <strong>{formatPercent(highShare)}</strong> are HIGH and{' '}
          <strong>{formatPercent(lowShare)}</strong> are LOW.
        </InsightCallout>
        {pipelineQuery.data && (
          <InsightCallout title="Spark integration" tone={pipelineQuery.data.fixtureRun ? 'caution' : 'positive'}>
            Gold holds <strong>{formatNumber(pipelineQuery.data.goldCustomers)}</strong> integrated
            profiles and <strong>{formatNumber(pipelineQuery.data.featureCount)}</strong> features.{' '}
            <Link to="/pipeline" className="font-medium text-accent underline-offset-2 hover:underline">
              Open Data Pipeline
            </Link>
            .
          </InsightCallout>
        )}
        <InsightCallout title="Model discrimination" tone="positive">
          {data.modelValAuc != null ? (
            <>
              LightGBM validation AUC <strong>{data.modelValAuc.toFixed(4)}</strong>
              {aucLift != null && data.baselineValAuc != null && (
                <>
                  {' '}
                  (+{aucLift.toFixed(4)} vs baseline {data.baselineValAuc.toFixed(4)})
                </>
              )}
              .
            </>
          ) : (
            <>Model evaluation metrics unavailable.</>
          )}
        </InsightCallout>
      </div>

      <div className="grid gap-4 xl:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle>Risk tier mix</CardTitle>
            <CardDescription>
              Pie of scored applications (n={formatNumber(data.scoredSampleSize)})
            </CardDescription>
          </CardHeader>
          <CardContent className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={tierPie}
                  dataKey="value"
                  nameKey="name"
                  innerRadius={55}
                  outerRadius={90}
                  paddingAngle={2}
                >
                  {tierPie.map((entry) => (
                    <Cell
                      key={entry.name}
                      fill={TIER_COLORS[entry.name as keyof typeof TIER_COLORS]}
                    />
                  ))}
                </Pie>
                <Tooltip
                  formatter={(value) => [
                    formatNumber(Number(value)),
                    'Applications',
                  ]}
                />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Default vs repaid</CardTitle>
            <CardDescription>Outcome counts from the 5,000-row risk summary</CardDescription>
          </CardHeader>
          <CardContent className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={outcomeBars}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="label" tick={{ fill: '#3a4f6a', fontSize: 12 }} />
                <YAxis tick={{ fill: '#3a4f6a', fontSize: 12 }} allowDecimals={false} />
                <Tooltip formatter={(value) => [formatNumber(Number(value)), 'Count']} />
                <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                  <Cell fill="#b91c1c" />
                  <Cell fill="#15803d" />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Average vs median</CardTitle>
            <CardDescription>Loan and income skew (mean above median ⇒ right-skew)</CardDescription>
          </CardHeader>
          <CardContent className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={financialCompare}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="metric" tick={{ fill: '#3a4f6a', fontSize: 12 }} />
                <YAxis tick={{ fill: '#3a4f6a', fontSize: 11 }} />
                <Tooltip formatter={(value) => formatNumber(Number(value))} />
                <Legend />
                <Bar dataKey="average" name="Average" fill="#1d4ed8" radius={[4, 4, 0, 0]} />
                <Bar dataKey="median" name="Median" fill="#93c5fd" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Risk tier counts</CardTitle>
            <CardDescription>Absolute volumes in the scored sample</CardDescription>
          </CardHeader>
          <CardContent className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={tierPie.map((t) => ({ tier: t.name, count: t.value }))}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="tier" tick={{ fill: '#3a4f6a', fontSize: 12 }} />
                <YAxis tick={{ fill: '#3a4f6a', fontSize: 12 }} allowDecimals={false} />
                <Tooltip />
                <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                  {tierPie.map((entry) => (
                    <Cell
                      key={entry.name}
                      fill={TIER_COLORS[entry.name as keyof typeof TIER_COLORS]}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Model ROC curve</CardTitle>
            <CardDescription>
              Discrimination curve
              {data.modelValAuc != null ? ` · val AUC ${data.modelValAuc.toFixed(4)}` : ''}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {rocUrl ? (
              <img
                src={rocUrl}
                alt="ROC curve"
                className="max-h-72 w-full rounded border border-border object-contain bg-white"
              />
            ) : (
              <p className="text-sm text-navy-muted">ROC figure unavailable.</p>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardContent className="py-4">
            <div className="text-xs uppercase tracking-wide text-navy-muted">Avg annuity</div>
            <div className="mt-1 text-lg font-semibold tabular-nums">
              {formatNumber(data.averageAnnuity)}
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="py-4">
            <div className="text-xs uppercase tracking-wide text-navy-muted">
              Social circle DEF 30+
            </div>
            <div className="mt-1 text-lg font-semibold tabular-nums">
              {formatPercent(data.def30SocialCircleRate)}
            </div>
            <div className="mt-1 text-xs text-navy-muted">
              Share of applicants with positive DEF_30 social-circle flag
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="py-4">
            <div className="text-xs uppercase tracking-wide text-navy-muted">
              Social circle DEF 60+
            </div>
            <div className="mt-1 text-lg font-semibold tabular-nums">
              {formatPercent(data.def60SocialCircleRate)}
            </div>
            <div className="mt-1 text-xs text-navy-muted">
              Share with positive DEF_60 social-circle flag
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
