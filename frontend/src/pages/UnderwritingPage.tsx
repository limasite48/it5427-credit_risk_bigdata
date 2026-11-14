import { zodResolver } from '@hookform/resolvers/zod'
import { useMutation } from '@tanstack/react-query'
import { useMemo, useState } from 'react'
import { useForm } from 'react-hook-form'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { z } from 'zod'
import fullApplicant from '@/data/sample_full_applicant.json'
import subprimeApplicant from '@/data/sample_subprime_applicant.json'
import { TierBadge } from '@/components/TierBadge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { api, ApiError } from '@/lib/api'
import { formatPercent } from '@/lib/utils'
import type { ApplicantPayload, ApplicationResponse, ScoreResponse } from '@/types/api'

const numberField = (schema: z.ZodNumber) =>
  z.preprocess((value) => {
    if (typeof value === 'number') return value
    if (typeof value === 'string' && value.trim() !== '') return Number(value)
    return value
  }, schema)

const formSchema = z.object({
  AMT_INCOME_TOTAL: numberField(z.number().positive()),
  AMT_CREDIT: numberField(z.number().positive()),
  AMT_ANNUITY: numberField(z.number().positive()),
  ageYears: numberField(z.number().min(18).max(100)),
  employmentYears: numberField(z.number().min(0).max(60)),
  OCCUPATION_TYPE: z.string().min(1),
  NAME_EDUCATION_TYPE: z.string().min(1),
  EXT_SOURCE_1: numberField(z.number().min(0).max(1)),
  EXT_SOURCE_2: numberField(z.number().min(0).max(1)),
  EXT_SOURCE_3: numberField(z.number().min(0).max(1)),
})

type FormValues = z.infer<typeof formSchema>

function daysFromYears(years: number) {
  return -Math.round(years * 365.25)
}

function yearsFromDays(days: number) {
  return Math.abs(days) / 365.25
}

function toFormValues(applicant: ApplicantPayload): FormValues {
  return {
    AMT_INCOME_TOTAL: Number(applicant.AMT_INCOME_TOTAL),
    AMT_CREDIT: Number(applicant.AMT_CREDIT),
    AMT_ANNUITY: Number(applicant.AMT_ANNUITY),
    ageYears: Number(yearsFromDays(Number(applicant.DAYS_BIRTH)).toFixed(1)),
    employmentYears: Number(yearsFromDays(Number(applicant.DAYS_EMPLOYED)).toFixed(1)),
    OCCUPATION_TYPE: String(applicant.OCCUPATION_TYPE ?? ''),
    NAME_EDUCATION_TYPE: String(applicant.NAME_EDUCATION_TYPE ?? ''),
    EXT_SOURCE_1: Number(applicant.EXT_SOURCE_1),
    EXT_SOURCE_2: Number(applicant.EXT_SOURCE_2),
    EXT_SOURCE_3: Number(applicant.EXT_SOURCE_3),
  }
}

const emptyDefaults: FormValues = {
  AMT_INCOME_TOTAL: 0,
  AMT_CREDIT: 0,
  AMT_ANNUITY: 0,
  ageYears: 30,
  employmentYears: 1,
  OCCUPATION_TYPE: '',
  NAME_EDUCATION_TYPE: '',
  EXT_SOURCE_1: 0.5,
  EXT_SOURCE_2: 0.5,
  EXT_SOURCE_3: 0.5,
}

export function UnderwritingPage() {
  const [baseApplicant, setBaseApplicant] = useState<ApplicantPayload | null>(null)
  const [result, setResult] = useState<ScoreResponse | null>(null)
  const [lookupId, setLookupId] = useState('101676')
  const [lookupResult, setLookupResult] = useState<ApplicationResponse | null>(null)
  const [lookupError, setLookupError] = useState<string | null>(null)

  const form = useForm<FormValues>({
    // Zod preprocess + RHF resolver typing is intentionally loosened here.
    resolver: zodResolver(formSchema) as never,
    defaultValues: emptyDefaults,
  })

  const scoreMutation = useMutation({
    mutationFn: (payload: ApplicantPayload) => api.score(payload),
    onSuccess: (data) => setResult(data),
  })

  const loadPreset = (preset: ApplicantPayload) => {
    setBaseApplicant({ ...preset })
    form.reset(toFormValues(preset))
    setResult(null)
  }

  const onSubmit = form.handleSubmit((values) => {
    const payload: ApplicantPayload = {
      ...(baseApplicant ?? {}),
      AMT_INCOME_TOTAL: values.AMT_INCOME_TOTAL,
      AMT_CREDIT: values.AMT_CREDIT,
      AMT_ANNUITY: values.AMT_ANNUITY,
      DAYS_BIRTH: daysFromYears(values.ageYears),
      DAYS_EMPLOYED: daysFromYears(values.employmentYears),
      OCCUPATION_TYPE: values.OCCUPATION_TYPE,
      NAME_EDUCATION_TYPE: values.NAME_EDUCATION_TYPE,
      EXT_SOURCE_1: values.EXT_SOURCE_1,
      EXT_SOURCE_2: values.EXT_SOURCE_2,
      EXT_SOURCE_3: values.EXT_SOURCE_3,
    }
    scoreMutation.mutate(payload)
  })

  const fieldError = useMemo(() => form.formState.errors, [form.formState.errors])

  async function lookupApplication() {
    setLookupError(null)
    setLookupResult(null)
    try {
      const data = await api.application(lookupId.trim())
      setLookupResult(data)
    } catch (err) {
      setLookupError(err instanceof ApiError ? err.message : 'Lookup failed')
    }
  }

  return (
    <div className="grid gap-4 xl:grid-cols-[1.4fr_1fr]">
      <Card>
        <CardHeader>
          <CardTitle>Applicant assessment</CardTitle>
          <CardDescription>
            Load a demo profile or enter values. Full Home Credit fields from presets are preserved
            for scoring.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="mb-4 flex flex-wrap gap-2">
            <Button
              type="button"
              variant="secondary"
              onClick={() => loadPreset(fullApplicant as ApplicantPayload)}
            >
              Load Full Applicant
            </Button>
            <Button
              type="button"
              variant="secondary"
              onClick={() => loadPreset(subprimeApplicant as ApplicantPayload)}
            >
              Load Subprime Applicant
            </Button>
            <Button
              type="button"
              variant="ghost"
              onClick={() => {
                setBaseApplicant(null)
                form.reset(emptyDefaults)
                setResult(null)
              }}
            >
              Reset
            </Button>
          </div>

          <form className="grid gap-3 sm:grid-cols-2" onSubmit={onSubmit}>
            {(
              [
                ['AMT_INCOME_TOTAL', 'Income'],
                ['AMT_CREDIT', 'Credit amount'],
                ['AMT_ANNUITY', 'Annuity'],
                ['ageYears', 'Age (years)'],
                ['employmentYears', 'Employment (years)'],
                ['OCCUPATION_TYPE', 'Occupation'],
                ['NAME_EDUCATION_TYPE', 'Education'],
                ['EXT_SOURCE_1', 'EXT_SOURCE_1'],
                ['EXT_SOURCE_2', 'EXT_SOURCE_2'],
                ['EXT_SOURCE_3', 'EXT_SOURCE_3'],
              ] as const
            ).map(([name, label]) => (
              <div key={name} className="space-y-1.5">
                <Label htmlFor={name}>{label}</Label>
                <Input id={name} {...form.register(name)} />
                {fieldError[name] && (
                  <p className="text-xs text-high">{String(fieldError[name]?.message)}</p>
                )}
              </div>
            ))}
            <div className="sm:col-span-2">
              <Button type="submit" disabled={scoreMutation.isPending}>
                {scoreMutation.isPending ? 'Assessing…' : 'Assess Applicant'}
              </Button>
              {scoreMutation.isError && (
                <p className="mt-2 text-sm text-high">
                  {scoreMutation.error instanceof ApiError
                    ? scoreMutation.error.message
                    : 'Scoring failed'}
                </p>
              )}
            </div>
          </form>
        </CardContent>
      </Card>

      <div className="space-y-4">
        <Card>
          <CardHeader>
            <CardTitle>Decision</CardTitle>
            <CardDescription>Live model output from the Python scoring service</CardDescription>
          </CardHeader>
          <CardContent>
            {!result && <p className="text-sm text-navy-muted">No assessment yet.</p>}
            {result && (
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <div>
                    <div className="text-xs uppercase text-navy-muted">Probability of default</div>
                    <div className="text-2xl font-semibold tabular-nums">
                      {formatPercent(result.defaultProbability)}
                    </div>
                  </div>
                  <TierBadge tier={result.riskTier} />
                </div>
                <div>
                  <div className="text-xs uppercase text-navy-muted">Action</div>
                  <div className="text-sm font-semibold">{result.action}</div>
                  {result.tierDescription && (
                    <div className="text-xs text-navy-muted">{result.tierDescription}</div>
                  )}
                </div>
                <div>
                  <div className="mb-2 text-xs uppercase text-navy-muted">SHAP contribution</div>
                  <div className="mb-3 h-44">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart
                        data={result.topRiskDrivers.map((d) => ({
                          feature: d.feature,
                          shap: d.shapValue ?? 0,
                          raises: d.raisesRisk,
                        }))}
                        layout="vertical"
                        margin={{ left: 8, right: 12 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                        <XAxis type="number" tick={{ fill: '#3a4f6a', fontSize: 10 }} />
                        <YAxis
                          type="category"
                          dataKey="feature"
                          width={110}
                          tick={{ fill: '#3a4f6a', fontSize: 10 }}
                        />
                        <ReferenceLine x={0} stroke="#94a3b8" />
                        <Tooltip
                          formatter={(value) => [Number(value).toFixed(4), 'SHAP']}
                        />
                        <Bar dataKey="shap" radius={[0, 3, 3, 0]}>
                          {result.topRiskDrivers.map((d) => (
                            <Cell
                              key={d.feature}
                              fill={d.raisesRisk ? '#b91c1c' : '#15803d'}
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                  <div className="mb-2 text-xs uppercase text-navy-muted">Top risk drivers</div>
                  <ul className="space-y-2">
                    {result.topRiskDrivers.map((driver) => (
                      <li
                        key={`${driver.feature}-${driver.shapValue}`}
                        className="rounded-md border border-border px-3 py-2"
                      >
                        <div className="flex items-center justify-between gap-2">
                          <span className="text-sm font-medium">{driver.feature}</span>
                          <span
                            className={
                              driver.raisesRisk ? 'text-xs font-medium text-high' : 'text-xs font-medium text-low'
                            }
                          >
                            {driver.raisesRisk ? 'Raises risk' : 'Lowers risk'}
                          </span>
                        </div>
                        <div className="mt-1 text-xs text-navy-muted">
                          Value: {String(driver.featureValue)}
                          {driver.shapValue != null ? ` · SHAP ${driver.shapValue}` : ''}
                        </div>
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Application lookup</CardTitle>
            <CardDescription>
              Track A sample, or a Gold-model score when the id was in the Home Credit subset
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex gap-2">
              <Input value={lookupId} onChange={(e) => setLookupId(e.target.value)} />
              <Button type="button" variant="secondary" onClick={() => void lookupApplication()}>
                Lookup
              </Button>
            </div>
            {lookupError && <p className="text-sm text-high">{lookupError}</p>}
            {lookupResult && (
              <div className="rounded-md border border-border p-3 text-sm">
                <div className="flex items-center justify-between">
                  <span className="font-medium">ID {lookupResult.applicationId}</span>
                  <TierBadge tier={lookupResult.riskTier} />
                </div>
                <div className="mt-2 grid gap-1 text-xs text-navy-muted">
                  <div>PD: {formatPercent(lookupResult.predictedPd)}</div>
                  <div>Action: {lookupResult.actionRecommendation}</div>
                  {lookupResult.scoreSource && <div>Score source: {lookupResult.scoreSource}</div>}
                  {lookupResult.occupation && <div>Occupation: {lookupResult.occupation}</div>}
                  {lookupResult.education && <div>Education: {lookupResult.education}</div>}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
