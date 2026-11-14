import { useQuery } from '@tanstack/react-query'
import { useMemo } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { InsightCallout } from '@/components/InsightCallout'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { api } from '@/lib/api'
import { formatNumber } from '@/lib/utils'

function formatBytes(bytes: number) {
  if (bytes >= 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  }
  if (bytes >= 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`
  }
  return `${formatNumber(bytes)} B`
}

function stageLabel(stage: string) {
  return stage.replace(/_/g, ' ')
}

export function PipelinePage() {
  const pipelineQuery = useQuery({
    queryKey: ['pipeline'],
    queryFn: () => api.pipeline(),
  })

  const data = pipelineQuery.data

  const stageChart = useMemo(() => {
    if (!data) return []
    return data.stages.map((stage) => ({
      stage: stageLabel(stage.stage),
      seconds: Number(stage.seconds.toFixed(2)),
    }))
  }, [data])

  const sourceChart = useMemo(() => {
    if (!data) return []
    return [...data.sources]
      .sort((a, b) => b.rows - a.rows)
      .map((source) => ({
        table: source.table,
        rows: source.rows,
      }))
  }, [data])

  if (pipelineQuery.isLoading) {
    return <p className="text-sm text-navy-muted">Loading Spark pipeline results…</p>
  }
  if (pipelineQuery.isError || !data) {
    return <p className="text-sm text-high">Failed to load the Spark pipeline report.</p>
  }

  const retained =
    data.bronzeRows > 0 ? data.silverRows / data.bronzeRows : 0
  const kpis = [
    { label: 'Bronze rows', value: formatNumber(data.bronzeRows) },
    { label: 'Bronze size', value: formatBytes(data.bronzeBytes) },
    { label: 'Silver rows', value: formatNumber(data.silverRows) },
    { label: 'Gold customers', value: formatNumber(data.goldCustomers) },
    { label: 'Gold features', value: formatNumber(data.featureCount) },
    {
      label: 'DQ checks',
      value: `${formatNumber(data.checksPassed)}/${formatNumber(data.checksTotal)}`,
    },
  ]

  return (
    <div className="space-y-4">
      <div className="grid gap-3 lg:grid-cols-3">
        <InsightCallout title="Integrated profile" tone="positive">
          Spark built <strong>{formatNumber(data.goldCustomers)}</strong> customer profiles and{' '}
          <strong>{formatNumber(data.goldLoans)}</strong> prior-loan rows, with{' '}
          <strong>{formatNumber(data.featureCount)}</strong> features and no TARGET on the feature table.
        </InsightCallout>
        <InsightCallout
          title={data.fixtureRun ? 'Fixture run' : 'Dataset'}
          tone={data.fixtureRun ? 'caution' : 'neutral'}
        >
          {data.datasetLabel}. Silver kept <strong>{(retained * 100).toFixed(2)}%</strong> of bronze rows
          after exact-duplicate removal and quarantine.
        </InsightCallout>
        <InsightCallout
          title="Quality gates"
          tone={data.checksPassed === data.checksTotal ? 'positive' : 'negative'}
        >
          {formatNumber(data.checksPassed)} of {formatNumber(data.checksTotal)} gold checks passed,
          including one-row-per-key and no post-application leakage.
        </InsightCallout>
      </div>

      <p className="text-xs text-navy-muted">{data.note}</p>

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

      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Stage time</CardTitle>
            <CardDescription>
              {data.startedAt ?? 'start'} → {data.finishedAt ?? 'finish'}
            </CardDescription>
          </CardHeader>
          <CardContent className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={stageChart} layout="vertical" margin={{ left: 24, right: 12 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis type="number" tick={{ fill: '#3a4f6a', fontSize: 11 }} unit="s" />
                <YAxis
                  type="category"
                  dataKey="stage"
                  width={120}
                  tick={{ fill: '#3a4f6a', fontSize: 11 }}
                />
                <Tooltip />
                <Bar dataKey="seconds" name="Seconds" fill="#1d4ed8" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Bronze volume by source</CardTitle>
            <CardDescription>Seven logical sources, including the synthetic traffic profile</CardDescription>
          </CardHeader>
          <CardContent className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={sourceChart} layout="vertical" margin={{ left: 16, right: 12 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis type="number" tick={{ fill: '#3a4f6a', fontSize: 11 }} />
                <YAxis
                  type="category"
                  dataKey="table"
                  width={150}
                  tick={{ fill: '#3a4f6a', fontSize: 11 }}
                />
                <Tooltip />
                <Bar dataKey="rows" name="Rows" fill="#0f766e" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Features on the gold profile</CardTitle>
            <CardDescription>Application columns plus aggregates from bureau, payments, POS, cards, and traffic</CardDescription>
          </CardHeader>
          <CardContent className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={data.featureGroups}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="name" tick={{ fill: '#3a4f6a', fontSize: 10 }} interval={0} angle={-25} height={70} textAnchor="end" />
                <YAxis allowDecimals={false} tick={{ fill: '#3a4f6a', fontSize: 11 }} />
                <Tooltip />
                <Bar dataKey="count" name="Columns" fill="#b45309" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Gold tables and lake sync</CardTitle>
            <CardDescription>One row per customer or prior loan after aggregation, then MinIO upload</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <table className="w-full text-left text-sm">
              <thead className="text-xs uppercase tracking-wide text-navy-muted">
                <tr>
                  <th className="pb-2 font-medium">Gold table</th>
                  <th className="pb-2 text-right font-medium">Rows</th>
                </tr>
              </thead>
              <tbody>
                {data.goldTables.map((table) => (
                  <tr key={table.name} className="border-t border-border">
                    <td className="py-1.5 font-mono text-xs">{table.name}</td>
                    <td className="py-1.5 text-right tabular-nums">{formatNumber(table.rows)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <table className="w-full text-left text-sm">
              <thead className="text-xs uppercase tracking-wide text-navy-muted">
                <tr>
                  <th className="pb-2 font-medium">Lake layer</th>
                  <th className="pb-2 text-right font-medium">Files</th>
                  <th className="pb-2 text-right font-medium">Uploaded</th>
                </tr>
              </thead>
              <tbody>
                {data.lake.map((layer) => (
                  <tr key={layer.layer} className="border-t border-border">
                    <td className="py-1.5 capitalize">{layer.layer}</td>
                    <td className="py-1.5 text-right tabular-nums">{formatNumber(layer.filesUploaded)}</td>
                    <td className="py-1.5 text-right tabular-nums">{formatBytes(layer.bytesUploaded)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            {data.syntheticFeatures.length > 0 && (
              <p className="text-xs text-navy-muted">
                Synthetic columns, kept separate from Home Credit originals:{' '}
                {data.syntheticFeatures.join(', ')}.
              </p>
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Silver cleaning</CardTitle>
          <CardDescription>
            Rows in must reconcile to rows kept, exact duplicates removed, and quarantine. Training labels:{' '}
            {formatNumber(data.trainingLabels)}.
          </CardDescription>
        </CardHeader>
        <CardContent className="overflow-x-auto">
          <table className="w-full min-w-[640px] text-left text-sm">
            <thead className="text-xs uppercase tracking-wide text-navy-muted">
              <tr>
                <th className="pb-2 font-medium">Table</th>
                <th className="pb-2 text-right font-medium">In</th>
                <th className="pb-2 text-right font-medium">Out</th>
                <th className="pb-2 text-right font-medium">Duplicates</th>
                <th className="pb-2 text-right font-medium">Quarantine</th>
              </tr>
            </thead>
            <tbody>
              {data.cleaning.map((row) => (
                <tr key={row.table} className="border-t border-border">
                  <td className="py-1.5 font-mono text-xs">{row.table}</td>
                  <td className="py-1.5 text-right tabular-nums">{formatNumber(row.rowsIn)}</td>
                  <td className="py-1.5 text-right tabular-nums">{formatNumber(row.rowsOut)}</td>
                  <td className="py-1.5 text-right tabular-nums">{formatNumber(row.exactDuplicatesRemoved)}</td>
                  <td className="py-1.5 text-right tabular-nums">{formatNumber(row.quarantined)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </CardContent>
      </Card>

      <div className="grid gap-4 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Streaming replay</CardTitle>
            <CardDescription>
              Sink: {data.stream.sink || 'n/a'} · {formatNumber(data.stream.eventsPerSec, 1)} events/s
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-3 text-sm">
              <Metric label="Events sent" value={formatNumber(data.stream.eventsSent)} />
              <Metric label="Micro-batches" value={formatNumber(data.stream.microBatches)} />
              <Metric label="Input rows" value={formatNumber(data.stream.inputRows)} />
              <Metric label="Written" value={formatNumber(data.stream.eventsWritten)} />
              <Metric label="Duplicates dropped" value={formatNumber(data.stream.duplicatesDropped)} />
              <Metric label="Late rows dropped" value={formatNumber(data.stream.lateRowsDropped)} />
              <Metric label="Quarantined" value={formatNumber(data.stream.quarantinedRows)} />
              <Metric label="Customers in snapshot" value={formatNumber(data.stream.snapshotCustomers)} />
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Gold checks</CardTitle>
            <CardDescription>Join grain, key uniqueness, and leakage gates from the Spark job</CardDescription>
          </CardHeader>
          <CardContent className="max-h-80 space-y-1 overflow-y-auto">
            {data.checks.map((check) => (
              <div key={check.name} className="flex items-start justify-between gap-3 border-b border-border py-1.5 text-sm">
                <span>{check.name}</span>
                <span className={check.passed ? 'text-emerald-700' : 'text-high'}>
                  {check.passed ? 'Pass' : 'Fail'}
                </span>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-border px-3 py-2">
      <div className="text-xs text-navy-muted">{label}</div>
      <div className="mt-0.5 font-semibold tabular-nums">{value}</div>
    </div>
  )
}
