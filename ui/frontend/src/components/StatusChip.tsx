import type { TaskStatus } from "@/api/types"

interface StatusChipProps {
  status: TaskStatus
  label?: string
}

export function StatusChip({ status, label }: StatusChipProps) {
  return (
    <span className={"chip " + status}>
      <span className={"dot s-" + status} />
      {label ?? status}
    </span>
  )
}

export function StatusDot({ status }: { status: TaskStatus }) {
  return <span className={"dot s-" + status} />
}
