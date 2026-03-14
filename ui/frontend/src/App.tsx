import { Routes, Route, Navigate } from "react-router"
import { AuthProvider, RequireAuth } from "./contexts/AuthContext"
import { Layout } from "./components/Layout"
import { LoginPage } from "./pages/LoginPage"
import { DashboardPage } from "./pages/DashboardPage"
import { TasksPage } from "./pages/TasksPage"
import { TaskDetailPage } from "./pages/TaskDetailPage"
import { ProfilePage } from "./pages/ProfilePage"
import { SettingsPage } from "./pages/SettingsPage"

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
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<DashboardPage />} />
          <Route path="/tasks" element={<TasksPage />} />
          <Route path="/tasks/:id" element={<TaskDetailPage />} />
          <Route path="/profile" element={<ProfilePage />} />
          <Route path="/settings" element={<SettingsPage />} />
        </Route>
      </Routes>
    </AuthProvider>
  )
}
