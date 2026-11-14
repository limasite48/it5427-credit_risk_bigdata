import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { describe, expect, it, vi } from 'vitest'
import { UnderwritingPage } from './UnderwritingPage'

vi.mock('@/lib/api', () => ({
  api: {
    score: vi.fn(async () => ({
      applicationId: 888002,
      defaultProbability: 0.03,
      riskTier: 'LOW',
      action: 'Auto-Approve',
      tierDescription: 'Low Risk',
      topRiskDrivers: [
        {
          feature: 'EXT_SOURCE_2',
          featureValue: 0.72,
          shapValue: -0.2,
          effect: 'Lowers Risk',
          raisesRisk: false,
        },
      ],
    })),
    application: vi.fn(),
  },
  ApiError: class ApiError extends Error {
    status: number
    constructor(status: number, message: string) {
      super(message)
      this.status = status
    }
  },
}))

function renderPage() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <UnderwritingPage />
    </QueryClientProvider>,
  )
}

describe('UnderwritingPage', () => {
  it('loads full applicant preset into the form', async () => {
    const user = userEvent.setup()
    renderPage()
    await user.click(screen.getByRole('button', { name: /load full applicant/i }))
    expect(screen.getByLabelText(/^Income$/i)).toHaveValue('300000')
    expect(screen.getByLabelText(/^Occupation$/i)).toHaveValue('Managers')
  })

  it('validates required fields before scoring', async () => {
    const user = userEvent.setup()
    renderPage()
    await user.click(screen.getByRole('button', { name: /assess applicant/i }))
    expect(await screen.findAllByText(/invalid|required|too small|expected|occupation/i)).not.toHaveLength(0)
  })

  it('shows scoring result after assessment', async () => {
    const user = userEvent.setup()
    renderPage()
    await user.click(screen.getByRole('button', { name: /load full applicant/i }))
    await user.click(screen.getByRole('button', { name: /assess applicant/i }))
    expect(await screen.findByText(/auto-approve/i)).toBeInTheDocument()
    expect(screen.getAllByText(/EXT_SOURCE_2/).length).toBeGreaterThan(0)
    expect(screen.getByText(/Lowers risk/i)).toBeInTheDocument()
  })
})
