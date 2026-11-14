import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Toaster } from 'sonner'
import { AuthProvider, useAuth } from '@/auth/AuthContext'
import { AppShell } from '@/components/AppShell'
import { LoginPage } from '@/pages/LoginPage'
import { MacroPage } from '@/pages/MacroPage'
import { OverviewPage } from '@/pages/OverviewPage'
import { PipelinePage } from '@/pages/PipelinePage'
import { SegmentsPage } from '@/pages/SegmentsPage'
import { UnderwritingPage } from '@/pages/UnderwritingPage'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
})

function Protected({ children }: { children: React.ReactNode }) {
  const { user, loading } = useAuth()
  if (loading) {
    return (
      <div className="flex min-h-screen items-center justify-center text-sm text-navy-muted">
        Checking session…
      </div>
    )
  }
  if (!user) {
    return <Navigate to="/login" replace />
  }
  return children
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route
              element={
                <Protected>
                  <AppShell />
                </Protected>
              }
            >
              <Route path="/" element={<OverviewPage />} />
              <Route path="/segments" element={<SegmentsPage />} />
              <Route path="/macro" element={<MacroPage />} />
              <Route path="/pipeline" element={<PipelinePage />} />
              <Route path="/underwriting" element={<UnderwritingPage />} />
            </Route>
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </BrowserRouter>
        <Toaster richColors position="top-right" />
      </AuthProvider>
    </QueryClientProvider>
  )
}
