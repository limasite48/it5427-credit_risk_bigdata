import { NavLink, Outlet, useLocation } from 'react-router-dom'
import { Database, Globe2, LayoutDashboard, LogOut, Scale, Users } from 'lucide-react'
import { useAuth } from '@/auth/AuthContext'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'

const nav = [
  { to: '/', label: 'Overview', icon: LayoutDashboard },
  { to: '/segments', label: 'Segment Risk', icon: Users },
  { to: '/macro', label: 'Macro & Regions', icon: Globe2 },
  { to: '/pipeline', label: 'Data Pipeline', icon: Database },
  { to: '/underwriting', label: 'Underwriting', icon: Scale },
]

export function AppShell() {
  const { user, logout } = useAuth()
  const location = useLocation()
  const current = nav.find((n) => n.to === location.pathname)?.label ?? 'Dashboard'

  return (
    <div className="flex min-h-screen bg-canvas text-navy">
      <aside className="hidden w-56 shrink-0 border-r border-border bg-white md:flex md:flex-col">
        <div className="border-b border-border px-4 py-4">
          <div className="text-xs font-semibold uppercase tracking-[0.14em] text-navy-muted">
            IT5427
          </div>
          <div className="mt-1 text-sm font-semibold">Credit Underwriting</div>
        </div>
        <nav className="flex flex-1 flex-col gap-1 p-3">
          {nav.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === '/'}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-2 rounded-md px-3 py-2 text-sm font-medium text-navy-muted hover:bg-canvas hover:text-navy',
                  isActive && 'bg-accent-soft text-accent',
                )
              }
            >
              <item.icon className="h-4 w-4" />
              {item.label}
            </NavLink>
          ))}
        </nav>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-14 items-center justify-between border-b border-border bg-white px-4">
          <div>
            <div className="text-xs text-navy-muted">Credit Risk Platform / {current}</div>
            <div className="text-sm font-semibold">{current}</div>
          </div>
          <div className="flex items-center gap-3">
            <div className="text-right">
              <div className="text-sm font-medium">{user?.username}</div>
              <div className="text-xs text-navy-muted">{user?.roles.join(', ')}</div>
            </div>
            <Button variant="secondary" size="sm" onClick={() => void logout()}>
              <LogOut className="h-3.5 w-3.5" />
              Logout
            </Button>
          </div>
        </header>

        <main className="flex-1 p-4 md:p-6">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
