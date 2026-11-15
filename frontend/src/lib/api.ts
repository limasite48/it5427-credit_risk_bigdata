import type {
  ApplicantPayload,
  ApplicationResponse,
  LoginResponse,
  MacroStressRequest,
  MacroStressResponse,
  OverviewResponse,
  PipelineResponse,
  RegionsResponse,
  ScoreResponse,
  SegmentsResponse,
  UserInfo,
} from '@/types/api'

let accessToken: string | null = null
let refreshPromise: Promise<boolean> | null = null

export function setAccessToken(token: string | null) {
  accessToken = token
}

export function getAccessToken() {
  return accessToken
}

async function tryRefresh(): Promise<boolean> {
  if (!refreshPromise) {
    refreshPromise = (async () => {
      const res = await fetch('/api/auth/refresh', {
        method: 'POST',
        credentials: 'include',
        headers: { 'X-Refresh-Request': '1' },
      })
      if (!res.ok) {
        accessToken = null
        return false
      }
      const body = (await res.json()) as LoginResponse
      accessToken = body.accessToken
      return true
    })().finally(() => {
      refreshPromise = null
    })
  }
  return refreshPromise
}

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

async function request<T>(path: string, init: RequestInit = {}, retry = true): Promise<T> {
  const headers = new Headers(init.headers)
  if (!headers.has('Content-Type') && init.body) {
    headers.set('Content-Type', 'application/json')
  }
  if (accessToken) {
    headers.set('Authorization', `Bearer ${accessToken}`)
  }

  const res = await fetch(path, {
    ...init,
    headers,
    credentials: 'include',
  })

  if (res.status === 401 && retry && !path.includes('/api/auth/login')) {
    const refreshed = await tryRefresh()
    if (refreshed) {
      return request<T>(path, init, false)
    }
  }

  if (!res.ok) {
    let message = `Request failed (${res.status})`
    try {
      const problem = await res.json()
      message = problem.title || problem.detail || message
    } catch {
      /* ignore */
    }
    throw new ApiError(res.status, message)
  }

  if (res.status === 204) {
    return undefined as T
  }

  const contentType = res.headers.get('content-type') || ''
  if (contentType.includes('application/json')) {
    return (await res.json()) as T
  }
  return (await res.blob()) as T
}

export const api = {
  login(username: string, password: string) {
    return request<LoginResponse>('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    })
  },
  logout() {
    return request<{ message: string }>('/api/auth/logout', { method: 'POST' })
  },
  me() {
    return request<UserInfo>('/api/auth/me')
  },
  overview() {
    return request<OverviewResponse>('/api/dashboard/overview')
  },
  segments() {
    return request<SegmentsResponse>('/api/dashboard/segments')
  },
  score(applicant: ApplicantPayload) {
    return request<ScoreResponse>('/api/scoring/score', {
      method: 'POST',
      body: JSON.stringify(applicant),
    })
  },
  application(id: string) {
    return request<ApplicationResponse>(`/api/applications/${encodeURIComponent(id)}`)
  },
  async rocCurveUrl(): Promise<string> {
    const blob = await request<Blob>('/api/dashboard/figures/roc-curve')
    return URL.createObjectURL(blob)
  },
  async shapSummaryUrl(): Promise<string> {
    const blob = await request<Blob>('/api/dashboard/figures/shap-summary')
    return URL.createObjectURL(blob)
  },
  regions() {
    return request<RegionsResponse>('/api/insights/regions')
  },
  pipeline() {
    return request<PipelineResponse>('/api/insights/pipeline')
  },
  macroStress(body: MacroStressRequest) {
    return request<MacroStressResponse>('/api/insights/macro-stress', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },
}
