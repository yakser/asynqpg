import { Routes, Route, Navigate } from "react-router"
import { AuthProvider, RequireAuth } from "./contexts/AuthContext"
import { Layout } from "./components/Layout"
import { LoginPage } from "./pages/LoginPage"
import { OverviewPage } from "./pages/OverviewPage"
import { TasksPage } from "./pages/TasksPage"
import { WorkersPage } from "./pages/WorkersPage"
import { MaintenancePage } from "./pages/MaintenancePage"
import { ProfilePage } from "./pages/ProfilePage"

export default function App() {
  return (
    <AuthProvider>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route
          element={
            <RequireAuth>
              <Layout />
            </RequireAuth>
          }
        >
          <Route path="/" element={<Navigate to="/overview" replace />} />
          <Route path="/dashboard" element={<Navigate to="/overview" replace />} />
          <Route path="/overview" element={<OverviewPage />} />
          <Route path="/tasks" element={<TasksPage />} />
          <Route path="/tasks/:id" element={<RedirectTaskDetail />} />
          <Route path="/workers" element={<WorkersPage />} />
          <Route path="/maintenance" element={<MaintenancePage />} />
          <Route path="/profile" element={<ProfilePage />} />
        </Route>
      </Routes>
    </AuthProvider>
  )
}

// Redirect old /tasks/:id deep links to /tasks?id=:id so the drawer opens.
function RedirectTaskDetail() {
  const id = window.location.pathname.split("/").pop()
  return <Navigate to={`/tasks?id=${id ?? ""}`} replace />
}
